
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

# https://code.earthengine.google.com/9e17ef57c5683c8c8f527d286d31715f



'''

    Config Session

'''

PATH_DIR = '/home/jailson/Imazon/projects/mapbiomas/mapping_legal_amazon/sentinel'

ASSET_ROI = 'projects/imazon-simex/LULC/LEGAL_AMAZON/biomes_legal_amazon'

ASSET_TILES =  'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3'

ASSET_MOSAICS =  'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3'

PATH_SAMPLES = '{}/data'.format(PATH_DIR)

PATH_REFERENCE_AREA = '{}/data/area/area_lulc_s2.csv'.format(PATH_DIR)

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
    2021, 
    2022, 
    #2023
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

N_SAMPLES = 3000


SAMPLE_PARAMS = [
    {'label':  3, 'min_samples': N_SAMPLES * 0.60},
    {'label':  4, 'min_samples': N_SAMPLES * 0.05},
    {'label': 12, 'min_samples': N_SAMPLES * 0.10},
    {'label': 15, 'min_samples': N_SAMPLES * 0.14},
    {'label': 18, 'min_samples': N_SAMPLES * 0.10},
    {'label': 25, 'min_samples': N_SAMPLES * 0.10},
    {'label': 33, 'min_samples': N_SAMPLES * 0.10},
]

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

    }
}

coord = [
    [
      [
        -59.482657921454425,
        -13.429189007826484
      ],
      [
        -52.780997765204425,
        -13.429189007826484
      ],
      [
        -52.780997765204425,
        -9.359129410798744
      ],
      [
        -59.482657921454425,
        -9.359129410798744
      ],
      [
        -59.482657921454425,
        -13.429189007826484
      ]
    ]
]

geo_roi = ee.Geometry.Polygon(coord)

'''

    Input Data

'''

roi = ee.FeatureCollection(ASSET_ROI)

tiles = ee.ImageCollection(ASSET_TILES)\
    .filter('biome == "AMAZONIA"')\
    .filter(f'year == 2023')
    #.filterBounds(geo_roi)

tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['grid_name']).get('list').getInfo()

df_reference_area = pd.read_csv(PATH_REFERENCE_AREA).replace({
    'classe': SAMPLE_REPLACE_VAL['label']
})



'''
    Functions
'''




def get_samples(tile_id, year):

    # SC-22-X-C
    number_row = tile_id[3:5]

    tiles_id_arround = [
        tile_id, 
        tile_id.replace(number_row, str(int(number_row) + 1)),
        tile_id.replace(number_row, str(int(number_row) - 1)),
    ]

    df_sp = pd.DataFrame([])

    df_proportion = df_reference_area.loc[
        (df_reference_area['grid_name'] == tile_id) &
        (df_reference_area['year'] == year)
    ]

    df_proportion['area_p'] = df_proportion['area_ha'] / df_proportion['area_ha'].sum()

    
    for index, row in df_proportion.iterrows():

        n_samples_to_get = row['area_p'] * N_SAMPLES
        
        df_samples_year_cls = df_samples.loc[
            (df_samples['label'] == row['classe']) &
            (df_samples['year'] == year) &
            (df_samples['grid_name'].isin(tiles_id_arround))
        ]

        # check existent samples
        exist_samples = len(df_samples_year_cls)

        if n_samples_to_get > exist_samples:
            n_samples_final = exist_samples
        else:
            n_samples_final = n_samples_to_get

        df_samples_sampled = df_samples_year_cls.sample(n=int(n_samples_final), random_state=42)

        print(row['classe'], int(n_samples_final))

        df_sp = pd.concat([df_sp, df_samples_sampled])  

    
    
    '''
    for item in SAMPLE_PARAMS:
        min_samples = int(item['min_samples'])
        label = item['label']

        df_proportion_classe = df_proportion.loc[df_proportion['classe'] == label]
        no_classe = len(df_proportion_classe) == 0

        total_samples = 0 if no_classe else int(df_proportion_classe['area_p'].values[0] * N_SAMPLES)

        # check if there is enought samples to use, if no, use min samples
        n_samples_to_get = total_samples

        df_samples_year_cls = df_samples.loc[
            (df_samples['label'] == label) &
            (df_samples['year'] == year)
        ]

        # check existent samples
        exist_samples = len(df_samples_year_cls)

        if n_samples_to_get > exist_samples:
            n_samples_final = 15 if exist_samples > 15 else exist_samples
        else:
            n_samples_final = n_samples_to_get

        print(label,n_samples_final)

        if n_samples_final == 0: continue

        df_samples_sampled = df_samples_year_cls.sample(n=n_samples_final, random_state=42)

        df_sp = pd.concat([df_sp, df_samples_sampled])  
    '''
    

    df_sp = pd.concat([df_sp, df_samples.query('label == 33').sample(n=5)]) 
    df_sp = pd.concat([df_sp, df_samples.query('label == 25').sample(n=5)]) 
    df_sp = pd.concat([df_sp, df_samples.query('label == 18').sample(n=8)]) 
    df_sp = pd.concat([df_sp, df_samples.query('label == 12').sample(n=15)]) 
    df_sp = pd.concat([df_sp, df_samples.query('label == 4').sample(n=8)]) 
    df_sp = pd.concat([df_sp, df_samples.query('label == 11').sample(n=8)]) 


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