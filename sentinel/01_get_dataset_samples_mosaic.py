
import sys, os

sys.path.append(os.path.abspath('.'))


import time
from retry import retry
import concurrent.futures
import datetime
import threading

from utils.helpers import *
from pprint import pprint

#service_account = 'sad-deep-learning-274812@appspot.gserviceaccount.com'
#credentials = ee.ServiceAccountCredentials(service_account, 'config/account-sad-deep-learning.json')

#ee.Initialize(credentials)

PROJECT = 'ee-mapbiomas-imazon'
#PROJECT = 'mapbiomas'

ee.Authenticate()
ee.Initialize(project=PROJECT)


'''

    Config Session

'''

PATH_DIR = '/home/jailson/Imazon/projects/mapbiomas/mapping_legal_amazon/sentinel'

ASSET_ROI = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'

ASSET_TILES =  'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3'

ASSET_MOSAICS =  'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3'

ASSET_LULC = 'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1'

ASSET_SAMPLES = 'projects/mapbiomas-workspace/VALIDACAO/mapbiomas_85k_col4_points_w_edge_and_edited_v1'

ASSET_STABLE = 'projects/ee-cgi-imazon/assets/mapbiomas/lulc_sentinel/stable_2018_2023'

SENTINEL_NEW_NAMES = [
    'blue',
    'green',
    'red',
    'red_edge_1',
    'nir',
    'swir1',
    'swir2',
    'pixel_qa'
]

ASSET_IMAGES = {
    's2':{
        'idCollection': '',
        'bandNames': ['B2', 'B3', 'B4', 'B5', 'B8', 'B11', 'B12', 'QA60'],
        'newBandNames': SENTINEL_NEW_NAMES,
    }
}




YEARS = [
    # 1985
    # 1985, 1986, 1987
    # 1988, 1989, 1990, 1991, 
    # 1992, 1993, 1994, 1995, 1996,
    # 1997, 1998, 1999, 
    # 2000, 2001, 2002,
    # 2003, 2004, 
    # 2005, 2006, 2007, 2008,
    # 2009, 2010, 
    # 2011, 
    # 2012, 
    # 2013, 
    # 2014,
    # 2015, 
    # 2016, 
    # 2017, 
    # 2018, 
    # 2019, 
    # 2020,
    # 2021, 
    2022, 
    # 2023
]




INPUT_FEATURES = [
    # 'gv', 
    # 'npv', 
    # 'soil', 
    # 'cloud',
    # 'gvs',
    # 'ndfi', 
    # 'csfi',
    "blue_median",
    "blue_median_wet",
    "blue_median_dry",
    "blue_stdDev",
    "green_median",
    "green_median_dry",
    "green_median_wet",
    "green_median_texture",
    "green_min",
    "green_stdDev",
    "red_median",
    "red_median_dry",
    "red_min",
    "red_median_wet",
    "red_stdDev",
    "nir_median",
    "nir_median_dry",
    "nir_median_wet",
    "nir_stdDev",
    "red_edge_1_median",
    "red_edge_1_median_dry",
    "red_edge_1_median_wet",
    "red_edge_1_stdDev",
    "red_edge_2_median",
    "red_edge_2_median_dry",
    "red_edge_2_median_wet",
    "red_edge_2_stdDev",
    "red_edge_3_median",
    "red_edge_3_median_dry",
    "red_edge_3_median_wet",
    "red_edge_3_stdDev",
    "red_edge_4_median",
    "red_edge_4_median_dry",
    "red_edge_4_median_wet",
    "red_edge_4_stdDev",
    "swir1_median",
    "swir1_median_dry",
    "swir1_median_wet",
    "swir1_stdDev",
    "swir2_median",
    "swir2_median_wet",
    "swir2_median_dry",
    "swir2_stdDev"
]

SEGMENT_BANDS = [
    "blue_median",
    "green_median",
    "red_median",
    "nir_median",
    "swir1_median",
    "swir2_median"
]

SAMPLE_REPLACE_VAL = {
    'label':{
        3:3,
        6:3,
        5:3,

        4:4,
        11:11,
        12:12,
        19:18,
        39:18,
        20:18,
        40:18,
        62:18,
        41:18,
        
        36: 3,

        46: 18, # coffe
        47: 18, # citrus
        35: 18, # palm oil
        48: 18, # other perennial crops

        9:3,

        30:25,
        23:25,
        22:25,
        29:25,
        24:25,


        15: 15,
        33: 33

    }
}


'''

    Harmonize classes from dataset samples

'''

HARMONIZATION_CLASSES_SAMPLES = {
    "AFLORAMENTO ROCHOSO": 12,
    "APICUM": 12,
    "AQUICULTURA": 33,
    "CAMPO ALAGADO E ÁREA PANTANOSA": 11,
    "CANA": 18,
    "FLORESTA INUNDÁVEL": 3,
    "FLORESTA PLANTADA": 3,
    "FORMAÇÃO CAMPESTRE": 12,
    "FORMAÇÃO FLORESTAL": 3,
    "FORMAÇÃO SAVÂNICA": 4,
    "INFRAESTRUTURA URBANA": 25,
    "LAVOURA PERENE": 18,
    "LAVOURA TEMPORÁRIA": 18,
    "MANGUE": 3,
    "MINERAÇÃO": 25,
    "NÃO OBSERVADO": 0,
    "OUTRA FORMAÇÃO NÃO FLORESTAL": 12,
    "OUTRA ÁREA NÃO VEGETADA": 25,
    "PASTAGEM": 15,
    "PRAIA E DUNA": 25,
    "RESTINGA HERBÁCEA": 12,
    "RIO, LAGO E OCEANO": 33,
    "VEGETAÇÃO URBANA": 3
}


EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=40)
MAX_REQUESTS_PER_SECOND = 100



'''

    Input Data

'''

roi_default = ee.FeatureCollection(ASSET_ROI).filter('Bioma == "Amazônia"')

tiles = ee.ImageCollection(ASSET_TILES)\
    .filter(f'year == 2023')\
    .filter('biome == "AMAZONIA"')
    #.filterBounds(roi_default.geometry())

stable = ee.Image(ASSET_STABLE)

tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['grid_name']).get('list').getInfo()

samples = ee.FeatureCollection(ASSET_SAMPLES)\
    .filter('AMOSTRAS == "Treinamento"')

print('samples ' + str(samples.size().getInfo()))



'''
    
    Function to Export

'''

# @retry()
def get_dataset(tile_id: str):

    # check if file already exists
    if os.path.isfile(f'{PATH_DIR}/data/{str(year)}/{tile_id}.geojson'):
        print(1)
        return None, None

    
    tile_image = ee.Image(tiles.filter(f'grid_name == "{tile_id}"').first())

    roi = tile_image.geometry()

    samples_harmonized_tile = samples_harmonized.filterBounds(roi)



    try:
        image = ee.Image(mosaic.filter(f'grid_name == "{tile_id}"').first())
        # image = ee.Image(image.divide(10000)).copyProperties(image)

        stable_grid = ee.Image(stable.clip(roi)).rename('label')

        # stable samples
        samples_stable = stable_grid.stratifiedSample(
            region=roi,
            scale=10,
            numPoints=10,  # Define o fator de amostragem
            dropNulls=True,
            geometries=True
        )

        samples_seed = samples_stable.merge(samples_harmonized_tile)


        # get segments
        segments = get_segments(ee.Image(image).select(SEGMENT_BANDS))

        segments = ee.Image(segments).reproject('EPSG:4326', None, 50)

        similar_mask = ee.Image(get_similar_mask(segments, samples_seed, 'label').selfMask())
        

        # redução de componentes conectados com percentil 5 e 95
        percentil = segments.addBands(stable_grid, ['label']).reduceConnectedComponents(
            ee.Reducer.percentile([5, 95]), 'segments'
        )

        # redução de componentes conectados com o modo
        validated = segments.addBands(stable_grid, ['label']).reduceConnectedComponents(
            ee.Reducer.mode(), 'segments'
        )


        # multiplica onde os percentis 5 e 95 são iguais a 1
        validated = validated.multiply(
            percentil.select(0).eq(percentil.select(1)).eq(1)
        )

        # similar_mask_validated = similar_mask.mask(similar_mask.eq(validated)).rename('label')
        similar_mask_validated = similar_mask.updateMask(similar_mask.eq(validated)).rename('label')


        # image = get_fractions_mosaic(image=image)
        # image = get_ndfi(image=image)
        # image = get_csfi(image=image)

        # select features
        image = ee.Image(image).select(INPUT_FEATURES)

        
    


        # get features from origin samples
        samples_image = image.sampleRegions(
            collection = samples_harmonized_tile, 
            scale = 10, 
            geometries = True
        )

        samples_segments = image.addBands(similar_mask_validated.selfMask()).sample(
            region=roi,
            scale=30,
            factor=0.005,  # Define o fator de amostragem
            dropNulls=True,
            geometries=True
        )

        # set properties
        # samples_image = samples_image.map(lambda feat: feat.copyProperties(image))
        samples_image = samples_image.map(lambda feat: feat.set('year', year)).filter(ee.Filter.notNull(['.geo']))
        samples_image = samples_image.map(lambda feat: feat.set('grid_name', tile_id))


        # samples_segments = samples_segments.map(lambda feat: ee.Feature(feat).copyProperties(image))
        samples_segments = samples_segments.map(lambda feat: feat.set('year', year)).filter(ee.Filter.notNull(['.geo']))#.filter(ee.Filter.neq('label', 0))
        samples_segments = samples_segments.map(lambda feat: feat.set('grid_name', tile_id))

        if samples_image.size().getInfo() > 0:
            samples_final = samples_image.merge(samples_segments)
        else:
            samples_final = samples_image

        samples_image_seg_gdf = ee.data.computeFeatures({
            'expression': samples_final,
            'fileFormat': 'GEOPANDAS_GEODATAFRAME'
        })

        return samples_image_seg_gdf, tile_id
    
    except Exception as e:
        print(e)
        print('exception')
        return None, None





'''
    
    Iteration

'''

for year in YEARS:

    # harmonization classes for dataset samples
    year_sample = 'CLASS_' + str(year) if year <= 2022 else 'CLASS_2022'
  
    samples_harmonized = samples.select(year_sample).remap(
        ee.Dictionary(HARMONIZATION_CLASSES_SAMPLES).keys(), 
        ee.Dictionary(HARMONIZATION_CLASSES_SAMPLES).values(), 
        year_sample
    ).select([year_sample], ['label'])

    mosaic = ee.ImageCollection(ASSET_MOSAICS)\
        .filter('biome == "AMAZONIA"')\
        .filter(f'year == {year}')
   
    # check if dir exists
    if not os.path.exists(f'{PATH_DIR}/data/{str(year)}'):
        os.makedirs(f'{PATH_DIR}/data/{str(year)}')

    

    
    for tile_id in tiles_list:


        samples_image_gdf = get_dataset(tile_id)


        if samples_image_gdf[0] is None: continue

        try:
            # export geodataframe
            samples_image_gdf[0].to_file(
                f'{PATH_DIR}/data/{str(year)}/{samples_image_gdf[1]}.geojson', driver='GeoJSON'
            ) 

        except Exception as e:
            print(e)
            continue
    





