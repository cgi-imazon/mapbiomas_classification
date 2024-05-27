
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

ASSET_DATASET_INT_C6 = 'projects/imazon-simex/LULC/SAMPLES/COLLECTION6/INTEGRATE'

ASSET_FEATURE_SPACE_C9 = 'projects/imazon-simex/LULC/COLLECTION9/feature-space'
ASSET_FEATURE_SPACE_C7 = 'projects/imazon-simex/LULC/COLLECTION7/feature-space'

PATH_AREAS = 'data/area/areas_amazon.csv'

INPUT_VERSION_DATASET_COL = '6'
INPUT_VERSION_DATASET = '2'
OUTPUT_VERSION = '1'

FEATURE_SPACE = [
    'mode',
    'mode_secondary',
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


N_SAMPLES = 5000


SAMPLE_PARAMS = pd.DataFrame([
    {'label':  3, 'min_samples': N_SAMPLES * 0.20},
    {'label':  4, 'min_samples': N_SAMPLES * 0.20},
    {'label': 12, 'min_samples': N_SAMPLES * 0.10},
    {'label': 15, 'min_samples': N_SAMPLES * 0.20},
    {'label': 18, 'min_samples': N_SAMPLES * 0.10},
    {'label': 25, 'min_samples': N_SAMPLES * 0.15},
    {'label': 33, 'min_samples': N_SAMPLES * 0.15},

])

REMAP_FROM = [12, 15, 18, 19, 24, 25, 29, 3, 33, 4, 6, 9]
REMAP_TO = [12, 15, 18, 18, 15, 15, 12, 3, 33, 4, 3, 3]

MODEL_PARAMS = {
    'numberOfTrees': 50,
    # 'variablesPerSplit': 4,
    # 'minLeafPopulation': 25
}


'''

    Functions

'''


def get_samples(tile, dataset_samples):

    tile = int(tile)

    tiles_list = tile + 1, tile + 2, tile + 1000, tile + 2000

    sp = dataset_samples.filter(ee.Filter.inList('tile', tiles_list))

    sp = sp.merge(dataset_samples)

    sp = sp.randomColumn('random', seed=1).sort('random', True)

    return sp


def get_balanced_samples(balance: pd.DataFrame, samples: ee.featurecollection.FeatureCollection, tile):

    year_ = 2022 if year == 2023 else 2022

    tile = int(tile)
    samples_balanced = ee.FeatureCollection([])

    # balance samples based on stratified area
    df_areas = pd.read_csv(PATH_AREAS).query(f'year == {year_} and tile == {tile}')
    df_areas['area_p'] = df_areas['area'] / df_areas.groupby('tile')['area'].transform('sum')
    df_areas['min_samples'] = df_areas['area_p'].mul(N_SAMPLES)

    
    
    # check min samples
    for id, row in balance.iterrows():

        label, min_samples = row['label'], row['min_samples']

        n_samples_fill = df_areas.loc[df_areas['class'] == int(label)].shape[0]

        if n_samples_fill < min_samples:
            samples_balanced = samples_balanced.merge(samples.filter(
                ee.Filter.eq('class', label)).limit(n_samples_fill))
        else:
            samples_balanced = samples_balanced.merge(samples.filter(
                ee.Filter.eq('class', label)).limit(min_samples))
            

    return samples_balanced

'''

    Input Data

'''

# fs = ee.ImageCollection(ASSET_FEATURE_SPACE_C7).filter('version == "4"')
fs = ee.ImageCollection(ASSET_FEATURE_SPACE_C9).filter('version == "5"')


YEARS = fs.reduceColumns(ee.Reducer.toList(), ['year']).get('list').getInfo()
YEARS = list(set(YEARS))

for year in YEARS:

    tiles = fs.filter(f'year == {year}').reduceColumns(ee.Reducer.toList(), ['tile'])\
        .get('list').getInfo()
    
    
    year = 2020 if year > 2020 else year

    # asset
    asset_dataset = '{}/samples-amazon-collection-{}-{}-{}'\
        .format(ASSET_DATASET_INT_C6, INPUT_VERSION_DATASET_COL, str(year), INPUT_VERSION_DATASET)

    # dataset 
    dataset_samples = ee.FeatureCollection(asset_dataset)

    # classes = dataset_samples.reduceColumns(ee.Reducer.frequencyHistogram(), ['class']).getInfo()

    for tile in tiles:

        samples = ee.FeatureCollection(get_samples(tile=tile, dataset_samples=dataset_samples))\
            .remap(REMAP_FROM, REMAP_TO, 'class')

        samples_balanced = get_balanced_samples(SAMPLE_PARAMS, samples, tile)

        classes = samples_balanced.reduceColumns(ee.Reducer.frequencyHistogram(), ['class']).get('histogram').getInfo()
        classes_str = [str(x) for x in list(dict(classes).keys())]
        classes_int = [int(x) for x in list(dict(classes).keys())]

        print(classes_str)

        exit()

        # classify
        classifier_prob = ee.Classifier.smileRandomForest(**MODEL_PARAMS)\
            .setOutputMode('MULTIPROBABILITY')\
            .train(samples_balanced, 'class', FEATURE_SPACE)
        
        classifier = ee.Classifier.smileRandomForest(**MODEL_PARAMS)\
            .train(samples_balanced, 'class', FEATURE_SPACE)


        # image feature space
        image = ee.Image('{}/{}-{}-{}'.format(ASSET_FEATURE_SPACE_C9, int(tile), year, INPUT_VERSION_DATASET))

        image = image.select(FEATURE_SPACE)




        classification = ee.Image(image
            .classify(classifier)
            .rename(['classification'])
            .copyProperties(image)
            .copyProperties(image, ['system:footprint'])
            .copyProperties(image, ['system:time_start'])
        )

        probabilities = ee.Image(image
            .classify(classifier_prob)
            .rename(['probability'])
            .copyProperties(image)
            .copyProperties(image, ['system:footprint'])
            .copyProperties(image, ['system:time_start'])
        )

        probabilities = probabilities\
            .arrayProject([0])\
            .arrayFlatten([classes_str])\
            .reduce(ee.Reducer.max())

        probabilities = ee.Image(probabilities).multiply(100).rename('probabilities')




        classification = classification.toByte()
        classification = classification.set('version', OUTPUT_VERSION)
        classification = classification.set('collection_id', 9.0)
        classification = classification.set('biome', 'AMAZONIA')
        classification = classification.set('territory', 'AMAZONIA')
        classification = classification.set('source', 'Imazon')
        classification = classification.set('year', year)

        probabilities = probabilities.toByte()
        probabilities = probabilities.set('version', OUTPUT_VERSION)
        probabilities = probabilities.set('collection_id', 9.0)
        probabilities = probabilities.set('biome', 'AMAZONIA')
        probabilities = probabilities.set('territory', 'AMAZONIA')
        probabilities = probabilities.set('source', 'Imazon')
        probabilities = probabilities.set('year', year)

