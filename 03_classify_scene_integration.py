
import sys, os

sys.path.append(os.path.abspath('.'))


import datetime
import pandas as pd
import geopandas as gpd
from retry import retry
import concurrent.futures
import geemap
import random

from utils.helpers import *
from pprint import pprint
from glob import glob


#PROJECT = 'sad-deep-learning-274812'
PROJECT = 'mapbiomas'

ee.Initialize(project=PROJECT)




'''

    Config Session

'''

PATH_DIR = '/home/jailson/Imazon/projects/mapbiomas/mapping_legal_amazon'

ASSET_ROI = 'projects/imazon-simex/LULC/LEGAL_AMAZON/biomes_legal_amazon'
# ASSET_ROI = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'

ASSET_TILES = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask'

# PATH_SAMPLES = 'mapbiomas_classification\\data\\2024'
PATH_SAMPLES = f'{PATH_DIR}/data'

PATH_AREAS = f'{PATH_DIR}/data/area/areas_la_1985_2022.csv'

ASSET_CLASSIFICATION = 'projects/ee-cgi-imazon/assets/mapbiomas/lulc_landsat/classification'

ASSET_OUTPUT = 'projects/ee-cgi-imazon/assets/mapbiomas/lulc_landsat/integrated'

ASSETS_CLS_VERSIONS = {
    'classification_p1': {
        'id': 'projects/imazon-simex/LULC/classification',
        'version': '2'
    },
    'classification_p2': {
        'id': 'projects/imazon-simex/LULC/TEST/classification',
        'version': ''
    },
    'classification_p3': {
        'id': 'projects/imazon-simex/LULC/TEST/classification-2',
        'version': '2'
    },
    'classification_p4': {
        'id': 'projects/imazon-simex/LULC/COLLECTION7/classification',
        'version': '4'
    },
    'classification_p5': {
        'id': 'projects/imazon-simex/LULC/COLLECTION6/classification_review',
        'version': '1'        
    },
    'classification_p6': {
        'id': 'projects/imazon-simex/LULC/COLLECTION7/classification_review',
        'version': ''
    },
    'classification_p7': {
        'id': 'projects/imazon-simex/LULC/COLLECTION9/classification',
        'version': ''
    },
    'classification_amz_legal_p1':{
        'id': 'projects/imazon-simex/LULC/LEGAL_AMAZON/classification',
        'version': ''
    },
    'classification_amz_legal_p2':{
        'id': 'projects/ee-cgi-imazon/assets/mapbiomas/lulc_landsat/classification',
        'version': ''
    },
    'classification_amz_legal_p3':{
        'id': 'projects/ee-mapbiomas-imazon/assets/mapbiomas/lulc_landsat/classification',
        'version': ''
    }
}

'''
    version 1: fetures from all sensors 
'''
OUTPUT_VERSION = '2'


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
    # 2021, 
    2022, 
    # 2023
    # 2024
]

# EXECUTOR = concurrent.futures.ThreadPoolExecutor(max_workers=1)

SAMPLE_REPLACE_VAL = {
    'label':{
        3:3,
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
        24:25,


        15: 15,
        33: 33,

        4:4,
        12:12

    }
}

'''

'''

FEATURE_SPACE = [
    'mode',
    #'mode_secondary',

    'transitions_total',
    'transitions_year',
    
    'distinct_total',
    'distinct_year',

    'occurrence_agriculture_total',
    'occurrence_agriculture_year',
    
    'occurrence_forest_total',
    'occurrence_forest_year',
    
    'occurrence_grassland_total',
    'occurrence_grassland_year',
    
    'occurrence_pasture_total',
    'occurrence_pasture_year',
    
    'occurrence_savanna_total',
    'occurrence_savanna_year',
    
    'occurrence_water_total',
    'occurrence_water_year'
]

MODEL_PARAMS = {
    'numberOfTrees': 50,
    # 'variablesPerSplit': 4,
    # 'minLeafPopulation': 25
}

N_SAMPLES = 3000


SAMPLE_PARAMS = pd.DataFrame([
    {'label':  3, 'min_samples': N_SAMPLES * 0.20},
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

biome = ee.FeatureCollection(ASSET_ROI).filter('Bioma == "Amazônia"')

tiles = ee.ImageCollection(ASSET_TILES).filterBounds(biome.geometry()).sort('tile', False)

tiles_l = tiles.reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()

tiles_list_loaded = ee.ImageCollection(ASSET_OUTPUT)\
    .filter('version == "2"')\
    .reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()

tiles_list_loaded = [int(x) for x in tiles_list_loaded]

tiles_list = set([x for x in tiles_l if x not in tiles_list_loaded])

df_areas = pd.read_csv(PATH_AREAS).replace(SAMPLE_REPLACE_VAL)\
    .groupby(by=['tile','year','label'])['area'].sum().reset_index()


'''
    
    Function to Export

'''
def setName(image):
    return image.set('name', ee.String(image.get('system:index')).slice(0, 20))

def get_classification(geometry):
    collection1 = ee.ImageCollection(ASSETS_CLS_VERSIONS['classification_p1']['id'])\
        .filter(ee.Filter.eq("version", ASSETS_CLS_VERSIONS['classification_p1']['version']))\
        .filter(ee.Filter.bounds(geometry))\
        .map(setName)

    collection2 = ee.ImageCollection(ASSETS_CLS_VERSIONS['classification_p2']['id'])\
        .filter(ee.Filter.bounds(geometry))\
        .map(setName)

    collection3 = ee.ImageCollection(ASSETS_CLS_VERSIONS['classification_p3']['id'])\
        .filter(ee.Filter.eq("version", ASSETS_CLS_VERSIONS['classification_p3']['version']))\
        .filter(ee.Filter.bounds(geometry))\
        .map(setName)

    collection4 = ee.ImageCollection(ASSETS_CLS_VERSIONS['classification_p4']['id'])\
        .filter(ee.Filter.eq("version", ASSETS_CLS_VERSIONS['classification_p4']['version']))\
        .filter(ee.Filter.bounds(geometry))\
        .map(setName)

    collection5 = ee.ImageCollection(ASSETS_CLS_VERSIONS['classification_p5']['id'])\
        .filter(ee.Filter.eq("version", ASSETS_CLS_VERSIONS['classification_p5']['version']))\
        .filter(ee.Filter.bounds(geometry))\
        .map(setName)

    collection6 = ee.ImageCollection(ASSETS_CLS_VERSIONS['classification_p6']['id'])\
        .filter(ee.Filter.bounds(geometry))\
        .map(setName)

    collection7 = ee.ImageCollection(ASSETS_CLS_VERSIONS['classification_p7']['id'])\
        .filter(ee.Filter.bounds(geometry))\
        .map(setName)
    


    collection_amz_legal_p1 = ee.ImageCollection(ASSETS_CLS_VERSIONS['classification_amz_legal_p1']['id'])\
        .filter(ee.Filter.bounds(geometry))
        
    collection_amz_legal_p2 = ee.ImageCollection(ASSETS_CLS_VERSIONS['classification_amz_legal_p2']['id'])\
        .filter(ee.Filter.bounds(geometry))

    collection_amz_legal_p3 = ee.ImageCollection(ASSETS_CLS_VERSIONS['classification_amz_legal_p3']['id'])\
        .filter(ee.Filter.bounds(geometry))



    collection5 = collection5\
        .filter(ee.Filter.inList("name", collection6.aggregate_array('name')).Not())

    collectionFinal = collection6.merge(collection5)

    collection4 = collection4\
        .filter(ee.Filter.inList("name", collectionFinal.aggregate_array('name')).Not())

    ###
    collectionFinal = collectionFinal.merge(collection4)

    collection3 = collection3\
        .filter(ee.Filter.inList("name", collectionFinal.aggregate_array('name')).Not())

    collectionFinal = collectionFinal.merge(collection3)

    collection2 = collection2\
        .filter(ee.Filter.inList("name", collectionFinal.aggregate_array('name')).Not())

    collectionFinal = collectionFinal.merge(collection2)

    collection1 = collection1\
        .filter(ee.Filter.inList("name", collectionFinal.aggregate_array('name')).Not())

    collectionFinal = collectionFinal.merge(collection1)

    # data 2023
    collectionFinal = collectionFinal.merge(collection7)\
        .merge(collection_amz_legal_p1)\
        .merge(collection_amz_legal_p2)\
        .merge(collection_amz_legal_p3)

    # Remap classes
    collectionFinal = collectionFinal.map(
        lambda image: image.where(image.eq(19), 18)
        .where(image.eq(13), 12)
        .where(image.eq(25), 15)
        .selfMask()
    )

    return collectionFinal

def get_balanced_samples(balance: pd.DataFrame, samples: gpd.GeoDataFrame, list_samples_df):

    res = []

    # balance samples based on stratified area    
    df_areas['area_p'] = df_areas['area'] / df_areas.groupby('tile')['area'].transform('sum')
    df_areas['min_samples'] = df_areas['area_p'].mul(N_SAMPLES)

    
    # check min samples
    for id, row in balance.iterrows():

        label, min_samples_default = row['label'], row['min_samples']

        df_areas_year = df_areas.query(f'label == {label}')

        if df_areas_year.shape[0] > 0:
            min_samples_area = int(df_areas_year['min_samples'].values[0])
        else: 
            min_samples_area = 0
            

        # check samples available
        try:
            sp_available = df_samples_amazon.query(f'label == {label}').shape[0]
        except Exception as e:
            print('no samples available')
            sp_available = 0

        if sp_available > min_samples_area and sp_available > 0:
            samples_selected = df_samples_amazon.query(f'label == {label}').sample(n=min_samples_area)
        else:
            n_sp = int(min_samples_area - sp_available)
            samples_selected_plus = list_samples_df.query(f'label == {label}').sample(n=n_sp)
            samples_selected = samples_selected_plus
 
        try:
            res.append(samples_selected)
        except Exception as e:
            print(e)
            continue

    # add samples to rare classes
    min_samples_gras = list_samples_df.query('label == 12').sample(n=15)
    min_samples_agr = list_samples_df.query('label == 18').sample(n=15)
    min_samples_water = list_samples_df.query('label == 33').sample(n=15)
    min_samles_savana = list_samples_df.query('label == 4').sample(n=15)
    min_samles_savana = list_samples_df.query('label == 15').sample(n=15)
    min_samles_forest = list_samples_df.query('label == 3').sample(n=15)

    # 
    res.append(min_samples_gras)
    res.append(min_samples_agr)
    res.append(min_samples_water)
    res.append(min_samles_savana)
    res.append(min_samles_forest)#

    try:
        samples_classification = pd.concat(res)
    except Exception as e:
        return None

    return samples_classification

def get_features(tile, year):

    tile_image = ee.Image(tiles.filter(f'tile == {tile}').first())

    roi = tile_image.geometry()

    center = roi.centroid()

    #classification_year = ee.ImageCollection(ASSET_CLASSIFICATION)\
    #    .filter(f'version == "1" and year == {year}')\
    #    .select(0)\
    #    .filterBounds(center)

    classification_tile = get_classification(center).select(0)

    classification_year = classification_tile.filter(f'year == {year}')


    # get metrics

    #
    n_observations_total = classification_tile\
        .map(lambda image: image.gt(0).unmask(0))\
        .reduce(ee.Reducer.sum())\
        .rename('observations_total')
    
    n_observations_year = classification_year\
        .map(lambda image: image.gt(0).unmask(0))\
        .reduce(ee.Reducer.sum())\
        .rename('observations_year')



    #
    transitions_total = classification_tile\
        .reduce(ee.Reducer.countRuns())\
        .divide(n_observations_total)\
        .rename('transitions_total')
    
    transitions_year = classification_year\
        .reduce(ee.Reducer.countRuns())\
        .divide(n_observations_year)\
        .rename('transitions_year')



    #
    distinct_total = classification_tile\
        .reduce(ee.Reducer.countDistinctNonNull())\
        .rename('distinct_total')
    
    distinct_year = classification_year\
        .reduce(ee.Reducer.countDistinctNonNull())\
        .rename('distinct_year')
    

    
    # mode
    mode_year = classification_year\
        .reduce(ee.Reducer.mode())\
        .rename('mode')
    


    forest_total = classification_tile\
        .map(lambda image: image.eq(3))\
        .reduce(ee.Reducer.sum())\
        .divide(n_observations_total)\
        .rename('occurrence_forest_total')

    forest_year = classification_year\
        .map(lambda image: image.eq(3))\
        .reduce(ee.Reducer.sum())\
        .divide(n_observations_year)\
        .rename('occurrence_forest_year')



    # occurrence savanna 
    savanna_total = classification_tile\
        .map(lambda image: image.eq(4))\
        .reduce(ee.Reducer.sum())\
        .divide(n_observations_total)\
        .rename('occurrence_savanna_total')

    savanna_year = classification_year\
        .map(lambda image: image.eq(4))\
        .reduce(ee.Reducer.sum())\
        .divide(n_observations_year)\
        .rename('occurrence_savanna_year')
    


    # occurrence grass 
    grassland_total = classification_tile\
        .map(lambda image: image.eq(12))\
        .reduce(ee.Reducer.sum())\
        .divide(n_observations_total)\
        .rename('occurrence_grassland_total')
    
    grassland_year = classification_year\
        .map(lambda image: image.eq(12))\
        .reduce(ee.Reducer.sum())\
        .divide(n_observations_year)\
        .rename('occurrence_grassland_year')




    # occurrence pasture
    pasture_total = classification_tile\
        .map(lambda image: image.eq(15))\
        .reduce(ee.Reducer.sum())\
        .divide(n_observations_total)\
        .rename('occurrence_pasture_total')
    
    pasture_year = classification_year\
        .map(lambda image: image.eq(15))\
        .reduce(ee.Reducer.sum())\
        .divide(n_observations_year)\
        .rename('occurrence_pasture_year')




    # occurrence agriculture
    agriculture_total = classification_tile\
        .map(lambda image: image.eq(18))\
        .reduce(ee.Reducer.sum())\
        .divide(n_observations_total)\
        .rename('occurrence_agriculture_total')
    

    agriculture_year = classification_year\
        .map(lambda image: image.eq(18))\
        .reduce(ee.Reducer.sum())\
        .divide(n_observations_year)\
        .rename('occurrence_agriculture_year')
    


    # occurrence water year
    water_total = classification_tile\
        .map(lambda image: image.eq(33))\
        .reduce(ee.Reducer.sum())\
        .divide(n_observations_total)\
        .rename('occurrence_water_total')

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
        .addBands(water_year)\
        .addBands(transitions_total)\
        .addBands(distinct_total)\
        .addBands(n_observations_total)\
        .addBands(forest_total)\
        .addBands(savanna_total)\
        .addBands(grassland_total)\
        .addBands(pasture_total)\
        .addBands(agriculture_total)\
        .addBands(water_total)


    return image, roi

def get_sample_values(samples, tiles, year):

    sample_vals = ee.FeatureCollection([])

    for t in tiles:
        
        image, _ = get_features(str(t), year)

        sp = samples.filter(f'tile == {t}')
        
        sample_values = image.sampleRegions(
            collection = sp, 
            scale = 30
        )

        sample_vals = sample_vals.merge(sample_values)

    return sample_vals

#@retry()
def classify_data(tile, year):

    df_samples = gpd.GeoDataFrame()

    try:
        df_samples = gpd.read_file(f'{PATH_SAMPLES}/{year}/{tile}.geojson')
    except Exception as e:
        print(f'erro no tile {e}')
        print('searching random samples')
        pass

    samples_balanced = get_balanced_samples(
        balance=SAMPLE_PARAMS, 
        samples=df_samples, list_samples_df=df_samples_amazon)
    

    if samples_balanced is None: return None

    samples_balanced = samples_balanced[['label', 'geometry', 'tile']]

    tiles_of_samples = samples_balanced['tile'].values.tolist()
    tiles_of_samples = set(tiles_of_samples)



    # convert to ee features
    try:
        samples = geemap.geopandas_to_ee(samples_balanced)
    except Exception as e:
        print('error at getting samples')
        return None

    image, roi = get_features(tile, year)

    sp = get_sample_values(samples, tiles_of_samples, year)

    # classify
    # classifier_prob = ee.Classifier.smileRandomForest(**MODEL_PARAMS)\
    #     .setOutputMode('MULTIPROBABILITY')\
    #     .train(sp, 'label', FEATURE_SPACE)
    
    classifier = ee.Classifier.smileRandomForest(**MODEL_PARAMS)\
        .train(sp, 'label', FEATURE_SPACE)



    classification = ee.Image(image
        .classify(classifier)
        .rename(['classification'])
        .copyProperties(image)
        .copyProperties(image, ['system:footprint'])
        .copyProperties(image, ['system:time_start'])
    )

    #probabilities = ee.Image(image
    #    .classify(classifier_prob)
    #    .rename(['probability'])
    #    .copyProperties(image)
    #    .copyProperties(image, ['system:footprint'])
    #    .copyProperties(image, ['system:time_start'])
    #)
    
    #probabilities = probabilities\
    #    .toArray().arrayArgmax()\
    #    .arrayGet([0])


    #probabilities = ee.Image(probabilities).multiply(100).rename('probabilities')

    classification = classification.toByte()
    classification = classification.set('version', OUTPUT_VERSION)
    classification = classification.set('collection_id', 9.0)
    classification = classification.set('tile', str(tile))
    classification = classification.set('biome', 'AMAZONIA')
    classification = classification.set('territory', 'AMAZONIA')
    classification = classification.set('source', 'Imazon')
    classification = classification.set('year', year)


    name = '{}-{}-{}'.format(int(tile), year, OUTPUT_VERSION)
    assetId = '{}/{}'.format(ASSET_OUTPUT, name)

    region = roi.getInfo()['coordinates']


    print(f'exporting features: {name}')

    try:
        task = ee.batch.Export.image.toAsset(
            image=classification,#.addBands(probabilities),
            description=name,
            assetId=assetId,
            pyramidingPolicy={".default": "mode"},
            region=region,
            scale=30,
            maxPixels=1e+13
        )

        task.start()
    except Exception as e:
        print(f'erro exporting -  {e}')
        return None

    return f'success exporting tile {tile}'

'''
    
    Iteration

'''

for year in YEARS:
    
    file_samples = []
    for i in glob(f'{PATH_DIR}/data/{year}/*'):
        try: file_samples.append(gpd.read_file(i)) 
        except Exception as e: continue

    df_samples_amazon = pd.concat(file_samples)

    for tile in tiles_list:
        result = classify_data(tile, year)

        if result is None: 
            print('error - exporting ')
            continue



