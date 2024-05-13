import ee, os
import time
from retry import retry
import concurrent.futures
import datetime
import threading

from utils.helpers import *
from pprint import pprint

#service_account = 'sad-deep-learning-274812@appspot.gserviceaccount.com'
#credentials = ee.ServiceAccountCredentials(service_account, os.path.abspath('.') + '/config/account-sad-deep-learning.json')


PROJECT = 'sad-deep-learning-274812'

ee.Initialize(project=PROJECT)

# ee.Initialize(credentials)


'''

    Config Session

'''

PATH_DIR = '/home/jailson/Imazon/projects/mapbiomas/mapping_legal_amazon'

ASSET_ROI = 'projects/imazon-simex/LULC/LEGAL_AMAZON/biomes_legal_amazon'

ASSET_TILES = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask'

# this must be your partition raw fc samples
ASSET_SAMPLES = 'projects/mapbiomas-workspace/VALIDACAO/mapbiomas_85k_col3_points_w_edge_and_edited_v2'

ASSET_REFERENCE = 'projects/mapbiomas-workspace/public/collection8/mapbiomas_collection80_integration_v1'

ASSET_OUTPUT = ''

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

SEGMENT_BANDS = [
    "blue",
    "green",
    "red",
    "nir",
    "swir1",
    "swir2"
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

tiles_list = tiles.reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()

samples = ee.FeatureCollection(ASSET_SAMPLES).filterBounds(roi.geometry())

print('samples ' + str(samples.size().getInfo()))

'''
    
    Function to Export

'''


def get_segments(image, size=16):

    seeds = ee.Algorithms.Image.Segmentation.seedGrid(
        size=size,
        gridType='square'
    )

    snic = ee.Algorithms.Image.Segmentation.SNIC(
        image=image,
        size=size,
        compactness=1,
        connectivity=8,
        neighborhoodSize=2*size,
        seeds=seeds
    )

    snic = ee.Image(snic)

    return snic.select(['clusters'], ['segments'])


def get_similar_mask(segments, samples):

    samples_segments = segments.sampleRegions(
        collection=samples,
        properties=['label']
    )

    list_label_seg = samples_segments.reduceColumns(ee.Reducer.toList().repeat(2),['label', 'segments']).get('list')

    segments_values = ee.List(list_label_seg)

    similiar_mask = segments.remap(segments_values.get(1), segments_values.get(0), 0)

    return similiar_mask.rename(['label'])


@retry()
def get_dataset(image_id: str):

    # check if file already exists
    if os.path.isfile(f'{PATH_DIR}/data/{str(year)}/{tile}/{image_id}.geojson'):
        return None

    image = ee.Image(images.filter(f'LANDSAT_SCENE_ID == "{image_id}"').first())

    geometry = image.geometry()
    
    image = get_fractions(image=image)
    image = get_ndfi(image=image)
    image = get_csfi(image=image)

    # select features
    image = ee.Image(image).select(INPUT_FEATURES + [
        "blue",
        "green",
        "red",
        "nir",
        "swir1",
        "swir2"
    ])




    '''
        Data Augmentation
    '''
    segments = get_segments(image.select(SEGMENT_BANDS), 16)

    similar_mask = get_similar_mask(segments, samples_harmonized_tile)



    percentil = segments.addBands(reference_map, [band_reference])\
        .reduceConnectedComponents(ee.Reducer.percentile([5, 95]), 'segments')

    validated = segments.addBands(reference_map, [band_reference])\
        .reduceConnectedComponents(ee.Reducer.mode(), 'segments')
    

    validated = validated.multiply(
        percentil.select(0).eq(percentil.select(1))
        .eq(1)
    )

    similar_mask_validated = similar_mask.mask(similar_mask.eq(validated)).selfMask().rename('label')


    samples_augmented = image.addBands(similar_mask_validated).sample(
        region=geometry,
        scale=30,
        factor=0.05,
        #numPixels=5,
        dropNulls=True,
        geometries=True,
        tileScale=12
    )

    samples_augmented = samples_augmented.map(lambda feat: feat.set('year', year))
    samples_augmented = samples_augmented.map(lambda feat: feat.set('tile', tile))

    samples_augmented = samples_augmented.filter(ee.Filter.notNull(['.geo']))




    # set properties
    # samples_image = samples_image.map(lambda feat: feat.copyProperties(image))
    # samples_image = samples_image.map(lambda feat: feat.set('year', year)).filter(ee.Filter.notNull(['.geo']))



    # convert to geodataframe
    # samples_image_gdf = ee.data.computeFeatures({
    #     'expression': samples_augmented,
    #     'fileFormat': 'GEOPANDAS_GEODATAFRAME'
    # })

    description = f'sp_{image_id}'

    task = ee.batch.Export.table.toAsset(
        collection=samples_augmented,
        description=description,
        assetId=ASSET_OUTPUT,
    )

    print('Exporting {}...'.format(description))

    task.start()

    return None, image_id


def export_dataset(image_ids: list, year:int, tile:str):

    future_to_point = {EXECUTOR.submit(get_dataset, image_id): image_id for image_id in image_ids}

    for future in concurrent.futures.as_completed(future_to_point):
        point = future_to_point[future]

        samples_image_gdf = future.result()

        if samples_image_gdf is None: 
            print('error - gdf is none')
            continue
        
        try:
            # export geodataframe
            samples_image_gdf[0].to_file(
                f'{PATH_DIR}/data/{str(year)}/{tile}/{samples_image_gdf[1]}_da.geojson', driver='GeoJSON'
            ) 

        except Exception as e:
            print(e)
            continue
    

            
            

    



'''
    
    Iteration

'''

start_time = datetime.datetime.now()

for year in YEARS:

    # harmonization classes for dataset samples
    year_sample = 'CLASS_' + str(year) if year <= 2022 else 'CLASS_2022'

    band_reference = f'classification_{str(year)}' if year <= 2022 else f'classification_{str(year - 1)}'
  
    samples_harmonized = samples.select(year_sample).remap(
        ee.Dictionary(HARMONIZATION_CLASSES_SAMPLES).keys(), 
        ee.Dictionary(HARMONIZATION_CLASSES_SAMPLES).values(), 
        year_sample
    ).select([year_sample], ['label'])


    reference_map = ee.Image(ASSET_REFERENCE).select(band_reference)


    for tile in tiles_list:

        # check if dir exists
        if not os.path.exists(f'{PATH_DIR}/data/{str(year)}/{tile}'):
            os.makedirs(f'{PATH_DIR}/data/{str(year)}/{tile}')


        tile_image = ee.Image(tiles.filter(f'tile == {tile}').first())

        roi = tile_image.geometry()

        center = roi.centroid()

        samples_harmonized_tile = samples_harmonized.filterBounds(roi)



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


        print(f'n images {len(image_list)}')


        export_dataset(image_list, year, tile)















