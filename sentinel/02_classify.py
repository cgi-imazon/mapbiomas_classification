
import sys, os

sys.path.append(os.path.abspath('.'))


import geemap
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

# https://code.earthengine.google.com/18407811802803ecb04f05a60c831d58
# https://code.earthengine.google.com/e00f2d90d9546e207cd137e6b9e00eb0


'''

    Config Session

'''

PATH_DIR = '/home/jailson/Imazon/projects/mapbiomas/mapping_legal_amazon/sentinel'

ASSET_ROI = 'projects/imazon-simex/LULC/LEGAL_AMAZON/biomes_legal_amazon'

ASSET_TILES =  'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3'

ASSET_MOSAICS =  'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3'

PATH_SAMPLES = '{}/data'.format(PATH_DIR)

PATH_REFERENCE_AREA = '{}/data/area/area_lulc_v2.csv'.format(PATH_DIR)

ASSET_OUTPUT = 'projects/ee-mapbiomas-imazon/assets/lulc/sentinel/classification'



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


OUTPUT_VERSION = '2'




MODEL_PARAMS = {
    'numberOfTrees': 50,
    # 'variablesPerSplit': 4,
    # 'minLeafPopulation': 25
}

N_SAMPLES = 3000


SAMPLE_PARAMS = {
    3: 500,
    4: 100,
    12: 120,
    15: 200,
    18: 200,
    25: 150,
    33: 100,
    11: 100,
}

SAMPLE_REPLACE_VAL = {
    'label':{
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

        31: 33,
        32: 25,
        21: 15

    }
}

coord = [
    -54.89831183203141,
    -2.4850787331932724
]

geo_roi = ee.Geometry.Point(coord)

'''

    Input Data

'''

roi = ee.FeatureCollection(ASSET_ROI)

tiles = ee.ImageCollection(ASSET_TILES)\
    .filter('biome == "AMAZONIA"')\
    .filter(f'year == 2023')

tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['grid_name']).get('list').getInfo()

df_reference_area = pd.read_csv(PATH_REFERENCE_AREA).replace({
    'classe': SAMPLE_REPLACE_VAL['label']
})

df_reference_area = df_reference_area.query('classe != 0')
df_reference_area = df_reference_area.groupby(by=['classe', 'grid_name', 'year'])['area_ha'].sum().reset_index()

print(df_reference_area.head())


'''
    Functions
'''




def get_samples(tile_id, year):

    # SC-22-X-C
    number_row = tile_id[3:5]

    df_sp = pd.DataFrame([])

    df_proportion = df_reference_area.loc[
        (df_reference_area['grid_name'] == tile_id) &
        (df_reference_area['year'] == year)
    ]

    df_proportion['area_p'] = df_proportion['area_ha'] / df_proportion['area_ha'].sum()

    print(df_proportion)

    
    for key, val in SAMPLE_PARAMS.items():
        
        n_samples_proportion = df_proportion.loc[df_proportion['classe'] == key, 'area_p'].values[0] * N_SAMPLES
        n_samples_min = val
        n_samples_exist = len(df_samples.loc[(df_samples['year'] == year) & (df_samples['label'] == key)])

        n_samples_final = 0

        if n_samples_proportion > n_samples_min:
            n_samples_final = n_samples_proportion
        elif n_samples_proportion < n_samples_min:
            n_samples_final = n_samples_min
        
        if n_samples_final < n_samples_exist: n_samples_exist
        if n_samples_final == 0: continue 

        df_samples_get = df_samples.sample(n=int(n_samples_final))

        df_sp = pd.concat([df_sp, df_samples_get])  


    return df_sp



'''
    
    Iteration

'''

for year in YEARS:

    df_samples = pd.concat([gpd.read_file(x) for x in glob('{}/{}/*'.format(PATH_SAMPLES, str(year)))])
    df_samples = df_samples.replace(SAMPLE_REPLACE_VAL)
    

    mosaic = ee.ImageCollection(ASSET_MOSAICS)\
        .filter('biome == "AMAZONIA"')\
        .filter(f'year == {year}')
   
    if not os.path.exists(f'{PATH_DIR}/data/{str(year)}'): continue
    
    for tile_id in tiles_list:

        imagename = '{}-{}-{}-{}'.format('AMAZONIA', tile_id, str(year), OUTPUT_VERSION)
        
        assetId = '{}/{}'.format(ASSET_OUTPUT, imagename)

        try:
            assetInfo = ee.data.getAsset(assetId)
        except Exception as e:

            samples = get_samples(tile_id, year)

            # filter features
            samples = samples[['label'] + INPUT_FEATURES + ['geometry']]


            samples_fc = geemap.geopandas_to_ee(samples)
            samples_fc = samples_fc.map(lambda feat: feat.select(INPUT_FEATURES + ['label']))


            # get image
            image = ee.Image(mosaic.filter(f'grid_name == "{tile_id}"').first())
            image = ee.Image(image.divide(10000)).copyProperties(image)
        
            image = get_fractions_mosaic(image=image)
            image = get_ndfi(image=image)
            image = get_csfi(image=image)

            # select features
            image = ee.Image(image).select(INPUT_FEATURES)



            # classify
            classifier_prob = ee.Classifier.smileRandomForest(**MODEL_PARAMS)\
                .setOutputMode('MULTIPROBABILITY')\
                .train(samples_fc, 'label', INPUT_FEATURES)
            
            classifier = ee.Classifier.smileRandomForest(**MODEL_PARAMS)\
                .train(samples_fc, 'label', INPUT_FEATURES)


            # get labels for this image
            labels_classified = samples['label'].drop_duplicates().values
            labels_classified = [str(x) for x in labels_classified]

            print(labels_classified)


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
            # classification = classification.set('tile', tile_id)
            classification = classification.set('year', year)

            probabilities = probabilities.toByte()
            probabilities = probabilities.set('version', OUTPUT_VERSION)
            probabilities = probabilities.set('collection_id', 9.0)
            probabilities = probabilities.set('biome', 'AMAZONIA')
            probabilities = probabilities.set('territory', 'AMAZONIA')
            probabilities = probabilities.set('source', 'Imazon')
            # probabilities = probabilities.set('tile',tile_id)
            probabilities = probabilities.set('year', year)

            region = image.geometry().getInfo()['coordinates']

            print(f'exporting image: {imagename}')

            task = ee.batch.Export.image.toAsset(
                image=classification.addBands(probabilities),
                description=imagename,
                assetId=assetId,
                pyramidingPolicy={".default": "mode"},
                region=region,
                scale=10,
                maxPixels=1e+13
            )

            task.start()