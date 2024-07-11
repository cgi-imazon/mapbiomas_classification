
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

# PROJECT = 'sad-deep-learning-274812'
PROJECT = 'mapbiomas'

ee.Initialize(project=PROJECT)



'''

    Config Session

'''

PATH_DIR = '/home/jailson/Imazon/projects/mapbiomas/mapping_legal_amazon'

ASSET_ROI = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'

ASSET_TILES = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask'

# ASSET_FEATURES = 'projects/imazon-simex/LULC/fs-tiles'
ASSET_FEATURES = 'projects/imazon-simex/LULC/feature-space'

ASSET_OUTPUT = 'projects/imazon-simex/LULC/COLLECTION9/probability'

PATH_AREAS = 'data/area/areas_amazon.csv'

OUTPUT_VERSION = '1'
FS_VERSION = '1'

INPUT_FEATURES = [
    'mode_year',
    'transitions_year',
    'distinct_year',
    'observations_year',
    'occurrence_forest_year',
    'occurrence_savanna_year',
    'occurrence_grassland_year',
    'occurrence_pasture_year',
    'occurrence_agriculture_year',
    'occurrence_water_year',
    #'probability_min',
    #'probability_max',
    #'probability_median',
    #'probability_std_dev'
]


N_SAMPLES = 3000


SAMPLE_PARAMS = pd.DataFrame([
    {'label':  3, 'min_samples': N_SAMPLES * 0.20},
    {'label':  4, 'min_samples': N_SAMPLES * 0.05},
    {'label': 12, 'min_samples': N_SAMPLES * 0.05},
    {'label': 15, 'min_samples': N_SAMPLES * 0.20},
    {'label': 18, 'min_samples': N_SAMPLES * 0.10},
    {'label': 25, 'min_samples': N_SAMPLES * 0.02},
    {'label': 33, 'min_samples': N_SAMPLES * 0.20},

])

REMAP_FROM = [12, 15, 18, 19, 24, 25, 29, 3, 33, 4, 6, 9]
REMAP_TO = [12, 15, 18, 18, 15, 15, 12, 3, 33, 4, 3, 3]

MODEL_PARAMS = {
    'numberOfTrees': 50,
    # 'variablesPerSplit': 4,
    # 'minLeafPopulation': 25
}

SAMPLE_REPLACE_VAL = {
    'label':{
        11: 12
    }
}


'''

    Functions

'''



'''
    description: if missing features, fill samples from the etire period
'''

def get_samples(tile: int):

    res = []
    
    tiles_list = tile + 1, tile + 2, tile + 1000, tile + 2000, tile

    for t in tiles_list:
    
        list_samples = list(glob(f'{PATH_DIR}/data/{year}/*_integrated.geojson'))

        if len(list_samples) == 0: continue


        list_samples_df = pd.concat([gpd.read_file(x) for x in list_samples])

        res.append(list_samples_df)
    
    if len(res) == 0: 
        return None

    df = pd.concat(res) 
    #df = df.sample(frac=0.8)

    return df


'''
    description: if missing features, fill samples from the etire period
'''
def get_balanced_samples(balance: pd.DataFrame, samples: gpd.GeoDataFrame):

    # total dataset samples
    list_samples = list(glob(f'{PATH_DIR}/data/{year}/*_integrated.geojson'))

    # filter 20%
    list_samples = random.sample(list_samples, int(len(list_samples) * 0.5))
    list_samples_df = pd.concat([gpd.read_file(x) for x in list_samples])

    # balance samples based on stratified area
    df_areas = pd.read_csv(PATH_AREAS).query(f'year == {year} and tile == {tile}')
    df_areas['area_p'] = df_areas['area'] / df_areas.groupby('tile')['area'].transform('sum')
    df_areas['min_samples'] = df_areas['area_p'].mul(N_SAMPLES)

    samples = pd.concat([samples, list_samples_df])
    res_sp = []

    # check min samples
    for id, row in balance.iterrows():

        label, min_samples_default = row['label'], row['min_samples']

        min_samples_tile = df_areas.loc[df_areas['class'] == label].shape[0]

        n_samples_total = samples.query(f'label == {label}').shape[0]



        # has samples of class
        # samples gt min samples

        if n_samples_total == 0: continue 

        if (min_samples_tile < min_samples_default) and n_samples_total > min_samples_default:
            n_samples_fill = min_samples_default
        elif (min_samples_tile > min_samples_default) and min_samples_tile > min_samples_default:
            n_samples_fill = min_samples_tile
        else: continue


        if label == 33: 
            n_samples_fill = 50 if n_samples_fill > 50 else n_samples_fill
            samples_cls = samples.query(f'label == {label}').sample(n=int(n_samples_fill))
        else:
            samples_cls = samples.query(f'label == {label}').sample(n=int(n_samples_fill))

        
        
        res_sp.append(samples_cls)

    
    samples_final = pd.concat(res_sp)

    return samples_final

'''

    Input Data

'''

roi = ee.FeatureCollection(ASSET_ROI).filter('Bioma == "Amazônia"')

tiles = ee.ImageCollection(ASSET_TILES).filterBounds(roi.geometry())

tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()
tiles_list = list(set(tiles_list))

print(tiles_list[:1])

YEARS = [
    # 1985, 
    # 1986, 
    # 1987,
    # 1988, 
    # 1989, 
    # 1990, 1991, 
    # 1992, 
    # 1993, 1994, 
    # 1995, 
    # 1996,
    1997, 
    1998, 
    1999
]

for year in YEARS:

    tiles_loaded = ee.ImageCollection(ASSET_OUTPUT).filter(f'year == {str(year)}')\
        .reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()
    
    tiles_iter = list(set(tiles_list) - set(tiles_loaded))

    for tile in tiles_iter:

        #if tile in tiles_loaded: continue

        tile_image = ee.Image(tiles.filter(f'tile == {tile}').first())

        roi = tile_image.geometry()

        region = roi.getInfo()['coordinates']

        center = roi.centroid()

        name = '{}-{}-{}'.format(int(tile), year, OUTPUT_VERSION)

        assetId = '{}/{}'.format(ASSET_OUTPUT, name)
            
        try:
            assetInfo = ee.data.getAsset(assetId)
        except Exception as e: 

            # asset_feat_tile = '{}/{}-{}-1'.format(ASSET_FEATURES, str(tile), str(year))
            asset_feat_tile = '{}/feature-space-{}-9-1'.format(ASSET_FEATURES, str(year))

            print(asset_feat_tile)

            # get image
            image = ee.Image(asset_feat_tile).mask(tile_image)
            image = image.mask(image.select('observations_year').gt(0))

            # get samples
            samples_df = get_samples(tile=tile)


            # stratified sampling
            df_samples_all = get_balanced_samples(balance=SAMPLE_PARAMS, samples=samples_df)
            df_samples_all = df_samples_all[INPUT_FEATURES + ['label', 'geometry']]
            df_samples_all = df_samples_all.replace(SAMPLE_REPLACE_VAL)

            print(df_samples_all.shape)
      
            samples = geemap.geopandas_to_ee(df_samples_all)


            # get labels for this image
            labels_classified = df_samples_all['label'].drop_duplicates().values
            labels_classified = [str(x) for x in labels_classified]

            print(labels_classified)


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
            classification = classification.set('tile', str(tile))
            classification = classification.set('year', year)

            probabilities = probabilities.toByte()
            probabilities = probabilities.set('version', OUTPUT_VERSION)
            probabilities = probabilities.set('collection_id', 9.0)
            probabilities = probabilities.set('tile', str(tile))
            probabilities = probabilities.set('year', year)

            

            print(f'exporting image: {name}')

            task = ee.batch.Export.image.toAsset(
                image=classification.addBands(probabilities),
                description=name,
                assetId=assetId,
                pyramidingPolicy={".default": "mode"},
                region=region,
                scale=30,
                maxPixels=1e+13
            )

            task.start()

'''
Meu carro foi batido na traseira por outra pessoa. Ambos os carros tem seguro. Mas eu (dono do carro batido) 
não quero usar a franquia mínima do meu seguro, pois se eu bater no futuro, perderei esse direito. A dona do carro que 
bateu o meu, quer usar a franquia mínima para pagar o concerto. Isso é justo? Eu tenho direito de não usar o meu seguro e fazer ela pagar?

considere um cenário de embate jurídico, quem tem direito? 
'''