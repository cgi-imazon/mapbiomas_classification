
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

# https://code.earthengine.google.com/a639633f12cf995fb8f8f41afcdd9175


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



# SENTINEL_NEW_NAMES = [
#     'blue',
#     'green',
#     'red',
#     'red_edge_1',
#     'nir',
#     'swir1',
#     'swir2',
#     'pixel_qa'
# ]

# ASSET_IMAGES = {
#     's2':{
#         'idCollection': '',
#         'bandNames': ['B2', 'B3', 'B4', 'B5', 'B8', 'B11', 'B12', 'QA60'],
#         'newBandNames': SENTINEL_NEW_NAMES,
#     }
# }



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


OUTPUT_VERSION = '8'




MODEL_PARAMS = {
    'numberOfTrees': 50,
    #'variablesPerSplit': 4,
    #'minLeafPopulation': 20
}

N_SAMPLES = 4000


SAMPLE_PARAMS = {
    3: 20,
    4: 250,
    12: 300,
    15: 300,
    18: 300,
    25: 200,
    33: 300,
    11: 200,
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


'''

    Input Data

'''

roi = ee.FeatureCollection(ASSET_ROI)

# tiles = ee.ImageCollection(ASSET_TILES)\
#     .filter('biome == "AMAZONIA"')\
#     .filter(f'year == 2023')

# tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['grid_name']).get('list').getInfo()

tiles_list = [
  "SA-21-Z-B",
  "SA-22-X-D",
  "SA-23-Z-C",
  "SB-21-Y-D",
  "SB-22-Y-D",
  "SC-21-X-A",
  "SC-21-X-B",
  "SC-21-X-D",
  "SC-22-V-B",
  "SD-21-Y-C"
]




df_reference_area = pd.read_csv(PATH_REFERENCE_AREA).replace({
    'classe': SAMPLE_REPLACE_VAL['label']
})

df_reference_area = df_reference_area.query('classe != 0')
df_reference_area = df_reference_area.groupby(by=['classe', 'grid_name', 'year'])['area_ha'].sum().reset_index()



'''
    Functions
'''




def get_samples(tile_id, year):

    df_sp = pd.DataFrame([])

    # SC-22-X-C
    number_row = tile_id[3:5]

    # get samples around the target tile
    tile_list = [
        tile_id,
        # 'SC-{}-X-C'.format(str(int(number_row) + 1)),
        # 'SC-{}-X-C'.format(str(int(number_row) - 1)),
    ]

    df_proportion = df_reference_area.loc[
        (df_reference_area['grid_name'] == tile_id) &
        (df_reference_area['year'] == year)
    ]

    df_proportion['area_p'] = df_proportion['area_ha'] / df_proportion['area_ha'].sum()

    for index, row in df_proportion.iterrows():
        n_samples_final = 0
        n_samples_min = SAMPLE_PARAMS[row['classe']]

        n_samples_proportion = int(row['area_p'] * N_SAMPLES)

        # check the number of samples that exists in target area
        n_samples_exist = len(df_samples.loc[
            (df_samples['year'] == year) & 
            (df_samples['label'] == row['classe']) #&
            #(df_samples['grid_name'].isin(tile_list))
        ])

        n_samples_final = n_samples_proportion if n_samples_proportion > n_samples_min else n_samples_min

        if n_samples_final == 0: continue 

        n_samples_missing = int(n_samples_proportion - n_samples_final)
        n_samples_missing = n_samples_exist if n_samples_missing > n_samples_exist else n_samples_missing

        print(row['classe'], n_samples_final, n_samples_exist, n_samples_missing)

        df_samples_get = df_samples.loc[
            (df_samples['year'] == year) & 
            (df_samples['label'] == row['classe'])# &
            #(df_samples['grid_name'].isin(tile_list))
        ].sample(n=int(n_samples_final))

        if n_samples_missing > 0:
            df_sp_missing = df_samples.loc[
                (df_samples['year'] == year) & 
                (df_samples['label'] == row['classe'])
            ].sample(n=int(n_samples_missing))

            df_sp = pd.concat([df_sp, df_samples_get, df_sp_missing])  
        else:
            df_sp = pd.concat([df_sp, df_samples_get])  
    
#     for key, n_samples_min in SAMPLE_PARAMS.items():
# 
#         n_samples_final = 0
#         
#         # get samples based on area proportion
#         n_samples_proportion = df_proportion.loc[df_proportion['classe'] == key, 'area_p'].mul(N_SAMPLES).values
#         n_samples_proportion = 0 if len(n_samples_proportion) == 0 else n_samples_proportion[0]
#         
#         # check the number of samples that exists in target area
#         n_samples_exist = len(df_samples.loc[
#             (df_samples['year'] == year) & 
#             (df_samples['label'] == key) #&
#             #(df_samples['grid_name'].isin(tile_list))
#         ])
#         
#         # check total samples for all tiles
#         n_samples_exist_all = len(df_samples.loc[
#             (df_samples['year'] == year) & 
#             (df_samples['label'] == key)
#         ])
# 
#         n_samples_final = n_samples_proportion if n_samples_proportion > n_samples_min else n_samples_min
# 
#         if n_samples_final == 0: continue 
# 
#         n_samples_final = n_samples_exist if n_samples_final > n_samples_exist else n_samples_final
# 
#         n_samples_missing = int(n_samples_proportion - n_samples_final)
#         n_samples_missing = n_samples_exist_all if n_samples_missing > n_samples_exist_all else n_samples_missing
# 
#         print(key, n_samples_final, n_samples_exist, n_samples_missing)
# 
#         df_samples_get = df_samples.loc[
#             (df_samples['year'] == year) & 
#             (df_samples['label'] == key)# &
#             #(df_samples['grid_name'].isin(tile_list))
#         ].sample(n=int(n_samples_final))
# 
#         if n_samples_missing > 0:
#             df_sp_missing = df_samples.loc[
#                 (df_samples['year'] == year) & 
#                 (df_samples['label'] == key)
#             ].sample(n=int(n_samples_missing))
# 
#             df_sp = pd.concat([df_sp, df_samples_get, df_sp_missing])  
#         else:
#             df_sp = pd.concat([df_sp, df_samples_get])  


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
            # image = ee.Image(image.divide(10000)).copyProperties(image)
        
            # image = get_fractions_mosaic(image=image)
            # image = get_ndfi(image=image)
            # image = get_csfi(image=image)

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

# gsutil cp 03_multiclass/model/* gs://imazon/mapbiomas/lulc/reference_map/model/
