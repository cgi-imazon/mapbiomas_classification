
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

PATH_LOGFILE = f'{PATH_DIR}/data/log.csv'

ASSET_ROI = 'projects/imazon-simex/LULC/LEGAL_AMAZON/biomes_legal_amazon'

ASSET_TILES = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask'

ASSET_OUTPUT = 'projects/imazon-simex/LULC/LEGAL_AMAZON/classification'

OUTPUT_VERSION = '1'


LANDSAT_NEW_NAMES = [
    'blue',
    'green',
    'red',
    'nir',
    'swir1',
    'swir2',
    'pixel_qa',
    'tir'
]


ASSET_LANDSAT_IMAGES = {
    'l5c2' : {
        'idCollection': 'LANDSAT/LT05/C02/T1_L2',
        'bandNames': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'QA_PIXEL', 'ST_B6'],
        'newBandNames': LANDSAT_NEW_NAMES,
        'defaultVisParams': {}
    },
    'l7c2' : {
        'idCollection': 'LANDSAT/LE07/C02/T1_L2',
        'bandNames': ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'QA_PIXEL', 'ST_B6'],
        'newBandNames': LANDSAT_NEW_NAMES,
    },
    'l8c2' : {
        'idCollection': 'LANDSAT/LC08/C02/T1_L2',
        'bandNames': ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL', 'ST_B10'],
        'newBandNames': LANDSAT_NEW_NAMES,
    },
    'l9c2' : {
        'idCollection': 'LANDSAT/LC09/C02/T1_L2',
        'bandNames': ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL', 'ST_B10'],
        'newBandNames': LANDSAT_NEW_NAMES,
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
    # 2009, 2010, 2011, 2012, 2013, 2014,
    # 2015, 2016, 2017, 2018, 2019, 2020,
    # 2021, 2022, 
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
    description: if missing features, fill samples from the etire period
'''
def get_balanced_samples(balance: pd.DataFrame, samples: gpd.GeoDataFrame):

    # total dataset samples
    list_samples = list(glob(f'{PATH_DIR}/data/{year}/*/*'))

    # filter 20%
    list_samples = random.sample(list_samples, int(len(list_samples) * 0.5))
    list_samples_df = pd.concat([gpd.read_file(x) for x in list_samples])


    
    # check min samples
    for id, row in balance.iterrows():
        label, min_samples = row['label'], row['min_samples']


        # get count already filtered
        count_samples = samples.query(f'label == {label}').shape[0]
        count_missing_samples = min_samples - count_samples

        if count_missing_samples > 0:
            if label == 18:
                fill_samples_df = list_samples_df.query(f'label == {label}').sample(n=80)
                samples = pd.concat([samples, fill_samples_df])
            elif label == 33:
                fill_samples_df = list_samples_df.query(f'label == {label}').sample(n=50)
                samples = pd.concat([samples, fill_samples_df])
            elif label == 25:
                fill_samples_df = list_samples_df.query(f'label == {label}').sample(n=30)
                samples = pd.concat([samples, fill_samples_df])
            else:
                fill_samples_df = list_samples_df.query(f'label == {label}').sample(n= int(0.2 * min_samples))
                samples = pd.concat([samples, fill_samples_df])

    return samples


def get_samples(tile: int, date: str, sr='l8'):

    res = []
    

    tiles_list = tile + 1, tile + 2, tile + 1000, tile + 2000


    date_target = datetime.datetime.strptime(date, '%Y-%m-%d')
    date_t0 = date_target - datetime.timedelta(days=-45)
    date_t1 = date_target + datetime.timedelta(days=45)



    for t in tiles_list:
        
        list_samples = list(glob(f'{PATH_DIR}/data/{year}/{t}/*'))

        if len(list_samples) == 0: continue

        list_samples_df = pd.concat([gpd.read_file(x) for x in list_samples])

        list_samples_df = list_samples_df.query(
            f'DATE_ACQUIRED >= "{date_t0.strftime("%Y-%m-%d")}" or ' + 
            f'DATE_ACQUIRED <= "{date_t1.strftime("%Y-%m-%d")}"'
        )

        list_samples_df = list_samples_df.query(f'sensor == "{sr}"')

        res.append(list_samples_df)
    
    if len(res) == 0: return None

    df = pd.concat(res) 
    #df = df.sample(frac=0.8)

    return df


def save_log():
    pass

'''
    
    Iteration

'''

image_list_loaded = ee.ImageCollection(ASSET_OUTPUT)\
    .filter('version == "1"')\
    .reduceColumns(ee.Reducer.toList(), ['LANDSAT_SCENE_ID']).get('list').getInfo()

for year in YEARS:

    for tile in tiles_list[:1]:

        tile_image = ee.Image(tiles.filter(f'tile == {tile}').first())

        roi = tile_image.geometry()

        center = roi.centroid()


        # get landsat images by roi
        l5 = (
            ee.ImageCollection(ASSET_LANDSAT_IMAGES['l5c2']['idCollection'])
            .filterBounds(center)
            .filterDate(f'{str(year)}-01-01', f'{str(year)}-12-31')
            .map(lambda image: apply_scale_factors(image))
            .map(lambda image: image.set('sensor', 'l5'))
            .select(
                ASSET_LANDSAT_IMAGES['l5c2']['bandNames'], 
                ASSET_LANDSAT_IMAGES['l5c2']['newBandNames'])
            .map(lambda image: remove_cloud(image))

        )

        l7 = (
            ee.ImageCollection(ASSET_LANDSAT_IMAGES['l7c2']['idCollection'])
            .filterBounds(center)
            .filterDate(f'{str(year)}-01-01', f'{str(year)}-12-31')
            .map(lambda image: apply_scale_factors(image))
            .map(lambda image: image.set('sensor', 'l7'))
            .select(
                ASSET_LANDSAT_IMAGES['l7c2']['bandNames'], 
                ASSET_LANDSAT_IMAGES['l7c2']['newBandNames'])
            .map(lambda image: remove_cloud(image))

        )

        l8 = (
            ee.ImageCollection(ASSET_LANDSAT_IMAGES['l8c2']['idCollection'])
            .filterBounds(center)
            .filterDate(f'{str(year)}-01-01', f'{str(year)}-12-31')
            .map(lambda image: apply_scale_factors(image))
            .map(lambda image: image.set('sensor', 'l8'))
            .select(
                ASSET_LANDSAT_IMAGES['l8c2']['bandNames'], 
                ASSET_LANDSAT_IMAGES['l8c2']['newBandNames'])
            .map(lambda image: remove_cloud(image))

        )

        l9 = (
            ee.ImageCollection(ASSET_LANDSAT_IMAGES['l9c2']['idCollection'])
            .filterBounds(center)
            .filterDate(f'{str(year)}-01-01', f'{str(year)}-12-31')
            .map(lambda image: apply_scale_factors(image))
            .map(lambda image: image.set('sensor', 'l9'))
            .select(
                ASSET_LANDSAT_IMAGES['l9c2']['bandNames'], 
                ASSET_LANDSAT_IMAGES['l9c2']['newBandNames'])
            .map(lambda image: remove_cloud(image))

        )

        images = ee.ImageCollection(l5.merge(l7).merge(l8).merge(l9))

        image_list = images.reduceColumns(ee.Reducer.toList(), ['LANDSAT_SCENE_ID']).get('list').getInfo()

        image_list = list(set(image_list) - set(image_list_loaded))

        for img_id in image_list:

            log = open(PATH_LOGFILE, 'a+')

            imagename = '{}_{}_{}'.format(img_id, str(year),OUTPUT_VERSION)
            
            assetId = '{}/{}'.format(ASSET_OUTPUT, imagename)

            try:
                assetInfo = ee.data.getAsset(assetId)
            except Exception as e:
                
                date_target = f'{str(year)}-08-01'
                df_target_sample = False
                sensor = 'l8'


                if os.path.isfile(f'{PATH_DIR}/data/{str(year)}/{tile}/{img_id}.geojson'):
                    df_target_sample = gpd.read_file(f'{PATH_DIR}/data/{str(year)}/{tile}/{img_id}.geojson')
                    date_target = df_target_sample['DATE_ACQUIRED'].values[0]   
                    sensor = df_target_sample['sensor'].values[0]  

                
                samples_surrounded = get_samples(tile, date=date_target, sr=sensor)
                
                # if there is no samples, skip
                if df_target_sample is False and samples_surrounded is None: 
                    log.write(f'\n{year},{tile},{img_id},fail')
                    log.close()
                    continue


                if samples_surrounded is not None and df_target_sample is False:
                    df_samples_all = samples_surrounded
                elif samples_surrounded is None and df_target_sample is not False:
                    df_samples_all = df_target_sample
                else: 
                    df_samples_all = pd.concat([df_target_sample, samples_surrounded])


                # stratified sampling
                df_samples_all = get_balanced_samples(balance=SAMPLE_PARAMS, samples=df_samples_all)
                df_samples_all = df_samples_all[INPUT_FEATURES + ['label', 'geometry']]

                # convert to ee features
                samples = geemap.geopandas_to_ee(df_samples_all)
                samples = samples.map(lambda feat: feat.select(INPUT_FEATURES + ['label']))


                image = ee.Image(images.filter(f'LANDSAT_SCENE_ID == "{img_id}"').first())

                roi = image.geometry()
                
                image = get_fractions(image=image)
                image = get_ndfi(image=image)
                image = get_csfi(image=image)

                # select features
                image = ee.Image(image).select(INPUT_FEATURES)


                # get labels for this image
                labels_classified = df_samples_all['label'].drop_duplicates().values
                labels_classified = [str(x) for x in labels_classified]




                # classify
                classifier_prob = ee.Classifier.smileRandomForest(**MODEL_PARAMS)\
                    .setOutputMode('MULTIPROBABILITY')\
                    .train(samples, 'label', INPUT_FEATURES)
                
                classifier = ee.Classifier.smileRandomForest(**MODEL_PARAMS)\
                    .train(samples, 'label', INPUT_FEATURES)






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
                    .arrayFlatten([labels_classified])\
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

                region = roi.getInfo()['coordinates']

                print(f'exporting image: {imagename}')

                task = ee.batch.Export.image.toAsset(
                    image=classification.addBands(probabilities),
                    description=imagename,
                    assetId=assetId,
                    pyramidingPolicy={".default": "mode"},
                    region=region,
                    scale=30,
                    maxPixels=1e+13
                )

                task.start()




                log.write(f'\n{year},{tile},{img_id},success')
                log.close()




    








'''
var maxProb = image
  .arrayProject([0])
  .arrayFlatten([['a', 'b', 'c', 'd', 'e', 'f', 'g']])
  .reduce(ee.Reducer.max());


var classification = image
    .toArray().arrayArgmax()
    .arrayGet([0]);

Map.addLayer(classification.randomVisualizer())

Map.centerObject(image,12)

https://code.earthengine.google.com/030bcb76dfcc14dfeb8b3880c23cb8cd
'''


