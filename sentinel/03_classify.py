
import sys, os

sys.path.append(os.path.abspath('.'))


import time
import geopandas as gpd
import pandas as pd

from utils.helpers import *
from pprint import pprint
from glob import glob

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

ASSET_SAMPLES = '{}/data/area'.format(PATH_DIR)

ASSET_OUTPUT = ''



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


OUTPUT_VERSION = '1'




MODEL_PARAMS = {
    'numberOfTrees': 50,
    # 'variablesPerSplit': 4,
    # 'minLeafPopulation': 25
}

N_SAMPLES = 4000


SAMPLE_PARAMS = [
    {'label':  3, 'min_samples': N_SAMPLES * 0.20},
    {'label':  4, 'min_samples': N_SAMPLES * 0.20},
    {'label': 12, 'min_samples': N_SAMPLES * 0.10},
    {'label': 15, 'min_samples': N_SAMPLES * 0.20},
    {'label': 18, 'min_samples': N_SAMPLES * 0.10},
    {'label': 25, 'min_samples': N_SAMPLES * 0.15},
    {'label': 33, 'min_samples': N_SAMPLES * 0.15},
]

SAMPLE_REPLACE_VAL = {
    'classe':{
        6:3,
        5:3,

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
        24:25
    }
}



'''

    Input Data

'''

roi = ee.FeatureCollection(ASSET_ROI)

tiles = ee.ImageCollection(ASSET_TILES)\
    .filter('biome == "AMAZONIA"')\
    .filter(f'year == 2023')

tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['grid_name']).get('list').getInfo()

df_samples = pd.concat([pd.read_csv(x) for x in glob('{}/*'.format(ASSET_SAMPLES))])
df_samples = df_samples.replace(SAMPLE_REPLACE_VAL)
df_samples = df_samples.groupby(by=['classe','year','grid_name'])['area_ha'].sum().reset_index()


'''
    Functions
'''




def get_samples(tile_id, year):

    df_sp = pd.DataFrame([])

    df_proportion = df_samples.loc[
        (df_samples['grid_name'] == tile_id) &
        (df_samples['year'] == year)
    ]

    df_proportion['area_p'] = df_proportion['area_ha'] / df_proportion['area_ha'].sum()

    print(df_proportion)



    return tile_id



'''
    
    Iteration

'''

for year in YEARS:

    mosaic = ee.ImageCollection(ASSET_MOSAICS)\
        .filter('biome == "AMAZONIA"')\
        .filter(f'year == {year}')
   
    if not os.path.exists(f'{PATH_DIR}/data/{str(year)}'): continue
    
    for tile_id in tiles_list:

        imagename = '{}_{}_{}'.format('AMAZONIA', str(year), OUTPUT_VERSION)
        
        assetId = '{}/{}'.format(ASSET_OUTPUT, imagename)

        try:
            assetInfo = ee.data.getAsset(assetId)
        except Exception as e:

            samples = get_samples(tile_id, year)
