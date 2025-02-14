import ee
ee.Initialize()

# Configurações
asset_lulc = 'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1'
asset_tiles = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask'
asset_roi = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'
asset_output = 'projects/ee-mapbiomas-imazon/assets/degradation/dam-frequency-c2'
version = '1'

default_params = {
    'tresh_dam_min': -0.250,
    'tresh_dam_max': -0.095,
    'time_window': 2
}

list_params = [
    [2023, default_params]
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
    npv_soil = summed.subtract(gvs)
    ndfi = ee.Image.cat(gvs, npv_soil).normalizedDifference().rename('ndfi').clamp(-1, 1)
    return image.addBands(gvs).addBands(ndfi)

def remove_cloud(image):
    qa = image.select('pixel_qa')
    mask = qa.bitwiseAnd(1 << 3).eq(0).And(qa.bitwiseAnd(1 << 4).eq(0))
    return image.updateMask(mask).copyProperties(image)

# Processamento principal
regions = ee.FeatureCollection(asset_roi).filter(ee.Filter.eq('Bioma', 'Amazônia'))
tiles = ee.ImageCollection(asset_tiles).filterBounds(regions.geometry())
tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()

for params in list_params:
    year, param_dict = params[0], params[1]
    lulc = ee.Image(asset_lulc).select(f'classification_{year}')
    
    for grid in tiles_list:
        tile_img = ee.ImageCollection(asset_tiles).filter(ee.Filter.eq('tile', grid)).first()
        roi = tile_img.geometry()
        center = roi.centroid()
        
        # Processar coleção alvo
        target_coll = (get_collection(f'{year}-01-01', f'{year}-12-30', 100, center)
                      .map(remove_cloud)
                      .map(get_fractions)
                      .map(get_ndfi))
        
        # Processar janela temporal
        start_tm = f"{year - param_dict['time_window'] + 1}-01-01"
        end_tm = f"{year - 1}-01-01"
        
        # Coleção de referência
        time_coll = (get_collection(start_tm, end_tm, 100, roi)
                     .map(remove_cloud)
                     .map(get_fractions)
                     .map(get_ndfi))
        
        # Calcular mediana
        median_monthly = time_coll.select('ndfi').reduce(ee.Reducer.median()).rename('metric')
        
        # Calcular desvios
        collection_deviations = target_coll.map(lambda img: 
            img.select('ndfi')
            .subtract(median_monthly)
            .updateMask(median_monthly.gt(0.8))
            .updateMask(lulc.eq(3))
            .rename('deviation')
            .copyProperties(img))
        
        # Calcular danos
        col_dam = collection_deviations.map(lambda img: 
            img.expression('b("deviation") >= min && b("deviation") <= max', {
                'deviation': img.select('deviation'),
                'min': param_dict['tresh_dam_min'],
                'max': param_dict['tresh_dam_max']
            }))
        
        # Soma total de danos
        sum_dam = col_dam.sum()
        
        # Exportar
        export_name = f'DAM_{year}_{grid}_{version}'
        task = ee.batch.Export.image.toAsset(
            image=sum_dam,
            description=export_name,
            assetId=f'{asset_output}/{export_name}',
            region=roi,
            scale=30,
            maxPixels=1e13,
            pyramidingPolicy={'.default': 'mode'}
        )
        task.start()
        print(f'Exportando: {export_name}')