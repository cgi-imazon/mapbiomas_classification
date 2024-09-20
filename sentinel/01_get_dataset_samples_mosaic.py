
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

ASSET_ROI = 'projects/imazon-simex/LULC/LEGAL_AMAZON/biomes_legal_amazon'

ASSET_TILES =  'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3'

ASSET_MOSAICS =  'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3'

# this must be your partition raw fc samples
ASSET_SAMPLES = 'projects/imazon-simex/LULC/COLLECTION9/SAMPLES/mapbiomas_85k_col3_points_w_edge_and_edited_v2_train_LA'



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
    # 2022, 
    2023
]




INPUT_FEATURES = [
    'gv', 
    'npv', 
    'soil', 
    'cloud',
    'gvs',
    'ndfi', 
    'csfi'
]



'''

    Harmonize classes from dataset samples

'''

HARMONIZATION_CLASSES_SAMPLES = {
    "AFLORAMENTO ROCHOSO": 25,
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

roi = ee.FeatureCollection(ASSET_ROI)

tiles = ee.ImageCollection(ASSET_TILES).filterBounds(roi.geometry())

tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['grid_name']).get('list').getInfo()

samples = ee.FeatureCollection(ASSET_SAMPLES).filterBounds(roi.geometry())

print('samples ' + str(samples.size().getInfo()))

'''
    
    Function to Export

'''



# @retry()
def get_dataset(tile_id: str):

    # check if file already exists
    if os.path.isfile(f'{PATH_DIR}/data/{str(year)}/{tile_id}.geojson'):
        print(1)
        return None

    

    tile_image = ee.Image(tiles.filter(f'grid_name == "{tile_id}"').first())

    roi = tile_image.geometry()

    center = roi.centroid()

    samples_harmonized_tile = samples_harmonized.filterBounds(roi)

    print(f'grid_name == "{tile_id}"')

    image = ee.Image(mosaic.filter(f'grid_name == "{tile_id}"').first())
    image = ee.Image(image.divide(10000)).copyProperties(image)
   

    image = get_fractions_mosaic(image=image)
    image = get_ndfi(image=image)
    image = get_csfi(image=image)

    # select features
    image = ee.Image(image).select(INPUT_FEATURES)
    


    # get features
    samples_image = image.sampleRegions(
        collection = samples_harmonized_tile, 
        scale = 30, 
        geometries = True
    )

    # set properties
    samples_image = samples_image.map(lambda feat: feat.copyProperties(image))
    samples_image = samples_image.map(lambda feat: feat.set('year', year)).filter(ee.Filter.notNull(['.geo']))
    samples_image = samples_image.map(lambda feat: feat.set('tile', tile_id))


    # convert to geodataframe
    samples_image_gdf = ee.data.computeFeatures({
        'expression': samples_image,
        'fileFormat': 'GEOPANDAS_GEODATAFRAME'
    })

    return samples_image_gdf, tile_id


def export_dataset(tiles_list: list, year:int):

    future_to_point = {EXECUTOR.submit(get_dataset, tile_id): tile_id for tile_id in tiles_list}

    for future in concurrent.futures.as_completed(future_to_point):
        point = future_to_point[future]

        samples_image_gdf = future.result()


        if samples_image_gdf is None: 
            print('error - gdf is none')
            continue

        
        try:
            # export geodataframe
            samples_image_gdf[0].to_file(
                f'{PATH_DIR}/data/{str(year)}/{samples_image_gdf[1]}.geojson', driver='GeoJSON'
            ) 

        except Exception as e:
            print(e)
            continue
    




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

        print(tile_id)

        samples_image_gdf = get_dataset(tile_id)

        try:
            # export geodataframe
            samples_image_gdf[0].to_file(
                f'{PATH_DIR}/data/{str(year)}/{samples_image_gdf[1]}.geojson', driver='GeoJSON'
            ) 

        except Exception as e:
            print(e)
            continue
    



    # export_dataset(tiles_list, year)


    












