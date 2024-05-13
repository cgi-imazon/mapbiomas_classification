
import sys, os

sys.path.append(os.path.abspath('.'))


import datetime
import pandas as pd
import geopandas as gpd
import geemap, random


from utils.helpers import *
from pprint import pprint
from glob import glob

#service_account = 'sad-deep-learning-274812@appspot.gserviceaccount.com'
#credentials = ee.ServiceAccountCredentials(service_account, 'config/account-sad-deep-learning.json')

#ee.Initialize(credentials)

PROJECT = 'sad-deep-learning-274812'

ee.Initialize(project=PROJECT)


'''

    Config Session

'''

PATH_DIR = '/home/jailson/Imazon/projects/mapbiomas/mapping_legal_amazon'

PATH_LOGFILE = f'{PATH_DIR}/data/log_dataset_integrated.csv'

ASSET_ROI = 'projects/imazon-simex/LULC/LEGAL_AMAZON/biomes_legal_amazon'

ASSET_TILES = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask'

ASSET_CLASSIFICATION = 'projects/imazon-simex/LULC/LEGAL_AMAZON/classification'

ASSET_OUTPUT = 'projects/imazon-simex/LULC/LEGAL_AMAZON/features-int'

OUTPUT_VERSION = '1'


YEARS = [
    # 1985
    # 1985, 1986, 1987
    # 1988, 1989, 1990, 1991, 
    # 1992, 1993, 1994, 1995, 1996,
    # 1997, 1998, 1999, 
    # 2000, 2001, 2002,
    # 2003, 2004, 
    # 2005, 2006, 2007, 2008,
    # 2009, 2010, 2011, 2012, 2013, 2014,
    # 2015, 2016, 2017, 2018, 2019, 2020,
    # 2021, 2022, 
    2023
]



MODEL_PARAMS = {
    'numberOfTrees': 50,
    # 'variablesPerSplit': 4,
    # 'minLeafPopulation': 25
}

N_SAMPLES = 1000


SAMPLE_PARAMS = pd.DataFrame([
    #{'label':  3, 'min_samples': N_SAMPLES * 0.20},
    {'label':  4, 'min_samples': N_SAMPLES * 0.20},
    {'label': 12, 'min_samples': N_SAMPLES * 0.10},
    {'label': 15, 'min_samples': N_SAMPLES * 0.20},
    {'label': 18, 'min_samples': N_SAMPLES * 0.10},
    {'label': 25, 'min_samples': N_SAMPLES * 0.15},
    {'label': 33, 'min_samples': N_SAMPLES * 0.15},
])

SAMPLE_REPLACE_VAL = {
    'label':{
        11: 12
    }
}

'''

    Input Data

'''

roi = ee.FeatureCollection(ASSET_ROI)

tiles = ee.ImageCollection(ASSET_TILES).filterBounds(roi.geometry())

tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()

'''
    
    Function to Export

'''



'''
    
    Iteration

'''

for year in YEARS:

    for tile in tiles_list[:1]:

        tile_image = ee.Image(tiles.filter(f'tile == {tile}').first())

        roi = tile_image.geometry()

        center = roi.centroid()

        classification_year = ee.ImageCollection(ASSET_CLASSIFICATION)\
            .filter(f'version == "1" and year == {year}')
    


    
        # get metrics

        #
        n_observations_year = classification_year\
            .map(lambda image: image.gt(0).unmask(0))\
            .reduce(ee.Reducer.sum())\
            .rename('observations_year')

        #
        transitions_year = classification_year\
            .reduce(ee.Reducer.countRuns())\
            .divide(n_observations_year)\
            .rename('transitions_year')

        #
        distinct_year = classification_year\
            .reduce(ee.Reducer.countDistinctNonNull())\
            .rename('distinct_year')
        
        # mode
        mode_year = classification_year\
            .reduce(ee.Reducer.mode())\
            .rename('mode')

        #
        # occurrence in the year
        #
        forest_year = classification_year\
            .map(lambda image: image.eq(3))\
            .reduce(ee.Reducer.sum())\
            .divide(n_observations_year)\
            .rename('occurrence_forest_year')

        savanna_year = classification_year\
            .map(lambda image: image.eq(4))\
            .reduce(ee.Reducer.sum())\
            .divide(n_observations_year)\
            .rename('occurrence_savanna_year')

        grassland_year = classification_year\
            .map(lambda image: image.eq(12))\
            .reduce(ee.Reducer.sum())\
            .divide(n_observations_year)\
            .rename('occurrence_grassland_year')

        pasture_year = classification_year\
            .map(lambda image: image.eq(15))\
            .reduce(ee.Reducer.sum())\
            .divide(n_observations_year)\
            .rename('occurrence_pasture_year')

        agriculture_year = classification_year\
            .map(lambda image: image.eq(18))\
            .reduce(ee.Reducer.sum())\
            .divide(n_observations_year)\
            .rename('occurrence_agriculture_year')

        water_year = classification_year\
            .map(lambda image: image.eq(33))\
            .reduce(ee.Reducer.sum())\
            .divide(n_observations_year)\
            .rename('occurrence_water_year')


        # image feature space
        image = mode_year\
            .addBands(transitions_year)\
            .addBands(distinct_year)\
            .addBands(n_observations_year)\
            .addBands(forest_year)\
            .addBands(savanna_year)\
            .addBands(grassland_year)\
            .addBands(pasture_year)\
            .addBands(agriculture_year)\
            .addBands(water_year)
        

        name = '{}-{}-{}'.format(int(tile), year, OUTPUT_VERSION)


        region = roi.getInfo()['coordinates']

        print(f'exporting features: {name}')

        task = ee.batch.Export.image.toAsset(
            image=image,
            description=name,
            assetId='{}/{}'.format(ASSET_OUTPUT, name),
            pyramidingPolicy={".default": "mode"},
            region=region,
            scale=30,
            maxPixels=1e+13
        )

        task.start()






