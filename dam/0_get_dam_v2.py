import ee
ee.Initialize()

# Configurações
asset_lulc = 'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1'
asset_tiles = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask'
asset_roi = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'
asset_output = 'projects/ee-mapbiomas-imazon/assets/degradation/dam-frequency-c2'
version = '4'

default_params = {
    'tresh_dam_min': -0.250,
    'tresh_dam_max': -0.095,
    'time_window': 3
}

list_params = [
    [2024, default_params]
]

months = ['01','02','03','04','05','06','07','08','09','10','11']

# Funções auxiliares
def get_collection(date_start, date_end, cloud_cover, roi):
    bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'pixel_qa', 'tir']
    
    l5 = (ee.ImageCollection('LANDSAT/LT05/C02/T1_L2')
          .filter(f'CLOUD_COVER <= {cloud_cover}')
          .filterBounds(roi)
          .filterDate(date_start, date_end)
          .map(lambda img: img.set('time', img.get('system:time_start'))))
    
    l5 = scale_factor_bands(l5).select(
        ['SR_B1','SR_B2','SR_B3','SR_B4','SR_B5','SR_B7','QA_PIXEL','ST_B6'], 
        bands
    )
    
    l7 = (ee.ImageCollection('LANDSAT/LE07/C02/T1_L2')
          .filter(f'CLOUD_COVER <= {cloud_cover}')
          .filterBounds(roi)
          .filterDate(date_start, date_end)
          .map(lambda img: img.set('time', img.get('system:time_start'))))
    
    l7 = scale_factor_bands(l7).select(
        ['SR_B1','SR_B2','SR_B3','SR_B4','SR_B5','SR_B7','QA_PIXEL','ST_B6'], 
        bands
    )
    
    l8 = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
          .filter(f'CLOUD_COVER <= {cloud_cover}')
          .filterBounds(roi)
          .filterDate(date_start, date_end)
          .map(lambda img: img.set('time', img.get('system:time_start'))))
    
    l8 = scale_factor_bands(l8).select(
        ['SR_B2','SR_B3','SR_B4','SR_B5','SR_B6','SR_B7','QA_PIXEL','ST_B10'], 
        bands
    )
    
    l9 = (ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
          .filter(f'CLOUD_COVER <= {cloud_cover}')
          .filterBounds(roi)
          .filterDate(date_start, date_end)
          .map(lambda img: img.set('time', img.get('system:time_start'))))
    
    l9 = scale_factor_bands(l9).select(
        ['SR_B2','SR_B3','SR_B4','SR_B5','SR_B6','SR_B7','QA_PIXEL','ST_B10'], 
        bands
    )
    
    return l5.merge(l7).merge(l8).merge(l9)

def scale_factor_bands(collection):
    def apply_scaling(img):
        optical = img.select('SR_B.').multiply(0.0000275).add(-0.2)
        thermal = img.select('ST_B.*').multiply(0.00341802).add(149.0)
        return img.addBands(optical, None, True)\
                  .addBands(thermal, None, True)\
                  .selfMask()\
                  .copyProperties(img)\
                  .set('system:time_start', img.get('system:time_start'))\
                  .set('system:time_end', img.get('system:time_end'))
    return collection.map(apply_scaling)

def get_fractions(image):
    ENDMEMBERS = [
        [0.0119,0.0475,0.0169,0.625,0.2399,0.0675],
        [0.1514,0.1597,0.1421,0.3053,0.7707,0.1975],
        [0.1799,0.2479,0.3158,0.5437,0.7707,0.6646],
        [0.4031,0.8714,0.79,0.8989,0.7002,0.6607]
    ]
    
    fractions = image.select(['blue','green','red','nir','swir1','swir2'])\
                    .unmix(ENDMEMBERS)\
                    .max(0)\
                    .rename(['gv','npv','soil','cloud'])
    
    summed = fractions.expression('b("gv") + b("npv") + b("soil")')
    shade = summed.subtract(1.0).abs().rename('shade')
    return image.addBands(fractions).addBands(shade)

def get_ndfi(image):
    summed = image.expression('b("gv") + b("npv") + b("soil")')
    gvs = image.select('gv').divide(summed).rename('gvs')
    ndfi = ee.Image.cat(gvs, summed.subtract(gvs))\
                  .normalizedDifference()\
                  .rename('ndfi')\
                  .clamp(-1, 1)
    return image.addBands(gvs).addBands(ndfi)

def remove_cloud(image):
    qa = image.select('pixel_qa')
    mask = qa.bitwiseAnd(1 << 3).eq(0).And(qa.bitwiseAnd(1 << 4).eq(0))
    return image.updateMask(mask).copyProperties(image)

def remove_cloud_shadow(image):
    qa = image.select('qa_pixel')
    cloud_threshold = image.select('cloud').lt(0.025)
    
    cond = cloud_threshold
    kernel = ee.Kernel.euclidean(60, 'meters')
    proximity = cond.distance(kernel, False)
    
    cond = cond.where(proximity.gt(0), 0)
    image = image.updateMask(cond)
    
    proximity = image.select('ndfi').unmask(-1).eq(-1).distance(kernel, False)
    cond = cond.where(proximity.gt(0), 0)
    return image.updateMask(cond)


# Processamento principal
regions = ee.FeatureCollection(asset_roi).filter(ee.Filter.eq('Bioma', 'Amazônia'))
tiles = ee.ImageCollection(asset_tiles).filterBounds(regions.geometry())
#tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()
#tiles_list = set(tiles_list)
tiles_list = [226068]

for params in list_params:
    year, param_dict = params[0], params[1]
    lulc = ee.Image(asset_lulc).select(f'classification_2023')
    
    start = f"{year}-01-01"
    end = f"{year}-12-30"

    for grid in tiles_list:
        
        tile_image = ee.Image(tiles.filter(f'tile == {grid}').first())
        roi = tile_image.geometry()
        center = roi.centroid()
        
        dict_params = params[1]
        
        collection_target = (get_collection(start, end, 100, center)
            .map(remove_cloud)
            .map(get_fractions)
            .map(get_ndfi)
            .select(['ndfi']))
        
        start_tm = f"{year - dict_params['time_window'] + 1}-01-01"
        end_tm = f"{year - 1}-01-01"

        print(start_tm, end_tm)

        # deviations = ee.List(months).map(lambda m: compute_deviations(m, collection_target, start_tm, end_tm, roi))
        deviations = []
        for m in months:
            month_int = ee.Number.parse(ee.String(m))

            collection_time_win = (get_collection(start_tm, end_tm, 100, roi)
                .map(remove_cloud)
                .filter(ee.Filter.calendarRange(month_int, month_int, 'month'))
                .map(get_fractions)
                .map(get_ndfi)
                .select(['ndfi']))
            
            median_monthly = collection_time_win.reduce(ee.Reducer.median()).rename('metric')

            collection_taget_monthly = collection_target.select(['ndfi']).filter(ee.Filter.calendarRange(month_int, month_int, 'month'))
            
            list_deviations = ee.Algorithms.If(
                collection_time_win.size().eq(0),
                ee.List([]),
                ee.Algorithms.If(
                    collection_taget_monthly.size().eq(0),
                    ee.List([]),
                    collection_taget_monthly.map(lambda img: img.subtract(median_monthly)
                        .mask(median_monthly.gt(0.80))
                        .mask(lulc.eq(3))
                        .rename('deviation')).toList(collection_taget_monthly.size())
                )
            )

            deviations.append(list_deviations)

        
        col_deviation = ee.ImageCollection(ee.List(deviations).flatten())
        
        col_dam = col_deviation.map(lambda image: image.expression('deviation >= min && deviation <= max', {
                'deviation': image.select('deviation'),
                'min': dict_params['tresh_dam_min'],
                'max': dict_params['tresh_dam_max']
            }
        ))
            
        sum_dam = col_dam.sum()
        
        valid_observations = col_deviation.map(lambda img: img.unmask(100).neq(100)).sum()
        col_dam_norm = sum_dam.divide(valid_observations).multiply(100).byte()
        
        name = f"DAM_{year}_{grid}_{version}"
        asset_id = f"{asset_output}/{name}"
        
        print(f'Exporting {name}')
        
        task = ee.batch.Export.image.toAsset(
            image=ee.Image(col_dam_norm).selfMask(),
            description=name,
            assetId=asset_id,
            pyramidingPolicy={'.default': 'mode'},
            region=roi,
            scale=30,
            maxPixels=1e13
        )
        
        task.start()