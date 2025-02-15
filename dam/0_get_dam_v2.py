import ee
ee.Initialize()

# Configurações
asset_lulc = 'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1'
asset_tiles = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask'
asset_roi = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'
asset_output = 'projects/ee-mapbiomas-imazon/assets/degradation/dam-frequency-c2'
version = '2'

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

def create_time_band(image):
    return image.addBands(image.metadata('system:time_start').divide(1e18))

def calculate_deviations(collection_time_win, collection_target, month_int, lulc):
    # Calcula a mediana mensal da coleção no tempo de referência
    median_monthly = collection_time_win.select('ndfi').reduce(ee.Reducer.median()).rename('metric')
    
    # Filtra a coleção alvo pelo mês específico
    collection_monthly = collection_target.select('ndfi').filter(ee.Filter.calendarRange(month_int, month_int, 'month'))
    
    # Função para calcular a diferença entre a imagem e a mediana mensal
    def compute_deviation(img):
        deviation = img.subtract(median_monthly) \
            .updateMask(median_monthly.gt(0.80)) \
            .updateMask(lulc.eq(3)) \
            .rename('deviation')
        return deviation.copyProperties(img)
    
    # Aplica a função a cada imagem da coleção
    collection_deviations = collection_monthly.map(compute_deviation)
    
    return collection_deviations.toList(collection_deviations.size())

# Processamento principal
regions = ee.FeatureCollection(asset_roi).filter(ee.Filter.eq('Bioma', 'Amazônia'))
tiles = ee.ImageCollection(asset_tiles).filterBounds(regions.geometry())
tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()
tiles_list = set(tiles_list)
#tiles_list = [226068]

for params in list_params:
    year, param_dict = params[0], params[1]
    lulc = ee.Image(asset_lulc).select(f'classification_2023')
    
    for grid in tiles_list:
        tile_img = ee.Image(tiles.filter(ee.Filter.eq('tile', grid)).first())
        roi = tile_img.geometry()
        center = roi.centroid()
        
        # Processar coleção alvo
        target_coll = (get_collection(f'{year}-01-01', f'{year}-12-30', 100, center)
                      .map(remove_cloud)
                      .map(get_fractions)
                      .map(get_ndfi)
                      .select(['ndfi']))
        
        # Janela temporal
        start_tm = f"{year - param_dict['time_window'] + 1}-01-01"
        end_tm = f"{year - 1}-12-31"
        
        # Processar desvios mensais
        deviations = []
        for month in months:
            m = int(month)
            # Coleção de referência
            time_coll = (get_collection(start_tm, end_tm, 100, roi)
                         .map(remove_cloud)
                         .filter(ee.Filter.calendarRange(m, m, 'month'))
                         .map(get_fractions)
                         .map(get_ndfi)
                         .select(['ndfi']))
            
            # Lógica condicional
            condition = ee.Algorithms.If(
                time_coll.size().eq(0),
                ee.List([]),
                ee.Algorithms.If(
                    target_coll.filter(ee.Filter.calendarRange(m, m, 'month')).size().eq(0),
                    ee.List([]),
                    calculate_deviations(time_coll, target_coll, m, lulc)
                )
            )
            deviations.append(condition)
        
        # Consolidar resultados
        col_deviation = ee.ImageCollection(ee.List(deviations).flatten())
        
        # Calcular danos
        col_dam = col_deviation.map(lambda img: 
            img.expression('b("deviation") >= min && b("deviation") <= max', {
                'deviation': img.select('deviation'),
                'min': param_dict['tresh_dam_min'],
                'max': param_dict['tresh_dam_max']
            }).rename('dam'))
        
        # Estatísticas finais
        sum_dam = col_dam.sum()
        valid_obs = col_deviation.map(lambda img: img.unmask(100).neq(100)).sum()
        final_result = sum_dam.divide(valid_obs).multiply(100).byte().rename('dam_freq')
        
        # Exportar
        export_name = f'DAM_{year}_{grid}_{version}'
        task = ee.batch.Export.image.toAsset(
            image=final_result,
            description=export_name,
            assetId=f'{asset_output}/{export_name}',
            region=roi,
            scale=30,
            maxPixels=1e13,
            pyramidingPolicy={'.default': 'mode'}
        )
        task.start()
        print(f'Exportando: {export_name}')