import ee

PROJECT = 'ee-mapbiomas-imazon'

ee.Initialize(project=PROJECT)


ASSET_SCENES = 'projects/mapbiomas-workspace/AUXILIAR/cenas-landsat'
ASSET_OUTPUT = 'projects/ee-mapbiomas-imazon/assets/degradation/dam_freq'
ASSET_BIOMES = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'
ASSET_MAPBIOMAS = 'projects/mapbiomas-workspace/public/collection8/mapbiomas_collection80_integration_v1'

VERSION = '2'

'''
    Config Params
'''

use_int_value = False
use_spatial_filter = False


spatial_filter_params = {
    'class_target': 1,
    'px_connected': 100,
    'area_minima': 0.1
}

default_detection_params = {
    'mask_tresh': 150, # 150
    'tresh_dam_min': 10,
    'tresh_dam_max': 50,
    'cloud_tresh': 2, # treshould to mask clouds. It is very sensitive to results,
    'time_window': 2
}

custom_detection_params = {
    'mask_tresh': 120, # 150
    'tresh_dam_min': -0.250,
    'tresh_dam_max': -0.095,
    'cloud_tresh': 2, # treshould to mask clouds. It is very sensitive to results,
    'time_window': 3
}

cloud_cover = 100



all_params = [
    [2023, custom_detection_params]
    #[2022, custom_detection_params],
    #[2021, custom_detection_params],
    #[2020, custom_detection_params],
    #[2019, custom_detection_params],
    #[2018, custom_detection_params],
    #[2017, custom_detection_params],
    #[2016, custom_detection_params],
    #[2015, custom_detection_params],
    #[2014, custom_detection_params],
    #[2013, custom_detection_params],
    
    #[2012, default_detection_params],
    #[2011, default_detection_params],
    #[2010, default_detection_params],
    #[2009, default_detection_params],
    #[2008, default_detection_params],
    #[2007, default_detection_params],
    #[2006, default_detection_params],
    #[2005, default_detection_params],
    #[2004, default_detection_params],
    #[2003, default_detection_params],
    #[2002, default_detection_params],
    #[2001, default_detection_params],
    #[2000, default_detection_params],
    #[1999, default_detection_params],
    #[1998, default_detection_params],
    #[1997, default_detection_params],
    #[1996, default_detection_params],
    #[1995, default_detection_params],
    #[1994, default_detection_params],
    #[1993, default_detection_params],
    #[1992, default_detection_params],
    #[1991, default_detection_params],
    #[1990, default_detection_params],
    #[1989, default_detection_params],
    #[1988, default_detection_params],
    #[1987, default_detection_params],
]


'''
    Functions
'''

def get_fractions(image):
    
    ENDMEMBERS = [
        [0.0119,0.0475,0.0169,0.625,0.2399,0.0675], # gv
        [0.1514,0.1597,0.1421,0.3053,0.7707,0.1975], # npv
        [0.1799,0.2479,0.3158,0.5437,0.7707,0.6646], # soil
        [0.4031,0.8714,0.79,0.8989,0.7002,0.6607] # cloud
    ]

    if use_int_value:
        ENDMEMBERS = [
            [119.0, 475.0, 169.0, 6250.0, 2399.0, 675.0],  # gv
            [1514.0, 1597.0, 1421.0, 3053.0, 7707.0, 1975.0], # npv
            [1799.0, 2479.0, 3158.0, 5437.0, 7707.0, 6646.0],  # soil
            [4031.0, 8714.0, 7900.0, 8989.0, 7002.0, 6607.0]  # cloud
        ]


    outBandNames = ['gv', 'npv', 'soil', 'cloud']
    
    fractions = ee.Image(image)\
        .select(['blue', 'green', 'red', 'nir', 'swir1', 'swir2'])\
        .unmix(ENDMEMBERS)

    if use_int_value:
        fractions = fractions\
            .max(0)\
            .multiply(100)\
            .byte()
    else:
        fractions = fractions\
            .max(0)
            #.multiply(100)\
            #.byte()

    fractions = fractions.rename(outBandNames)
    
    summed = fractions.expression('b("gv") + b("npv") + b("soil")')
    
    if use_int_value:
        shade = summed.subtract(100).abs().byte().rename("shade")
    else:
        shade = summed.subtract(1).abs().rename("shade")


    fractions = fractions.addBands(shade)
    
    return image.addBands(fractions)


def get_ndfi(image):

    summed = image.expression('b("gv") + b("npv") + b("soil")')

    if use_int_value:
        gvs = image.select("gv").divide(summed).multiply(100).rename("gvs")
    else:
        gvs = image.select("gv").divide(summed).rename("gvs")

    npvSoil = image.expression('b("npv") + b("soil")')

    ndfi = ee.Image.cat(gvs, npvSoil) \
        .normalizedDifference() \
        .rename('ndfi')

    if use_int_value:
        # rescale NDFI from 0 to 200 \
        ndfi = ndfi.expression('byte(b("ndfi") * 100 + 100)')

    image = image.addBands(gvs)
    image = image.addBands(ndfi)

    return ee.Image(image)


def scale_factor_bands(is_int, collection):
    def scale_image(image):
        optical_bands = image.select('SR_B.') \
            .multiply(0.0000275) \
            .add(-0.2) \
            .multiply(10000)

        thermal_bands = image.select('ST_B.*') \
            .multiply(0.00341802) \
            .add(149.0) \
            .multiply(10)

        image = image.addBands(optical_bands, None, True)
        image = image.addBands(thermal_bands, None, True)

        return image

    if is_int:
        collection = collection.map(scale_image)
    else:
        collection = collection.map(lambda image: image.multiply(0.0000275).add(-0.2).selfMask().copyProperties(image))

    return collection

def get_collection(date_start, date_end, cloud_cover, roi, cloud_thresh):
    collection = None
    l5 = None
    l7 = None
    l8 = None
    l9 = None
    bands = [
        'blue',
        'green',
        'red',
        'nir',
        'swir1',
        'swir2',
        'pixel_qa',
        'tir'
    ]

    l5 = ee.ImageCollection('LANDSAT/LT05/C02/T1_L2') \
        .filter(ee.Filter.lte('CLOUD_COVER', cloud_cover)) \
        .filterBounds(roi) \
        .filterDate(date_start, date_end)

    l5 = scale_factor_bands(use_int_value, l5) \
        .select(
            ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'QA_PIXEL', 'ST_B6'],
            bands
        ) \
        .map(lambda image: image.set('time', image.get('system:time_start')))

    l7 = ee.ImageCollection('LANDSAT/LE07/C02/T1_L2') \
        .filter(ee.Filter.lte('CLOUD_COVER', cloud_cover)) \
        .filterBounds(roi) \
        .filterDate(date_start, date_end)

    l7 = scale_factor_bands(use_int_value, l7) \
        .select(
            ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'QA_PIXEL', 'ST_B6'],
            bands
        ) \
        .map(lambda image: image.set('time', image.get('system:time_start')))

    l8 = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2') \
        .filter(ee.Filter.lte('CLOUD_COVER', cloud_cover)) \
        .filterBounds(roi) \
        .filterDate(date_start, date_end)

    l8 = scale_factor_bands(use_int_value, l8) \
        .select(
            ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL', 'ST_B10'],
            bands
        ) \
        .map(lambda image: image.set('time', image.get('system:time_start')))

    l9 = ee.ImageCollection('LANDSAT/LC09/C02/T1_L2') \
        .filter(ee.Filter.lte('CLOUD_COVER', cloud_cover)) \
        .filterBounds(roi) \
        .filterDate(date_start, date_end)

    l9 = scale_factor_bands(use_int_value, l9) \
        .select(
            ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL', 'ST_B10'],
            bands
        ) \
        .map(lambda image: image.set('time', image.get('system:time_start')))

    collection = l5.merge(l7).merge(l8).merge(l9)

    if use_int_value:
        collection = collection \
            .map(get_fractions) \
            .map(get_ndfi) \
            .map(lambda image: image.set('cloudTresh', cloud_thresh)) \
            .map(remove_cloud_shadow)
    else:
        collection = collection \
            .map(get_fractions) \
            .map(get_ndfi) \
            .map(lambda image: image.set('cloudTresh', cloud_thresh)) \
            .map(remove_cloud_shadow)

    return collection

def remove_cloud_shadow(image):
    qa = image.select('qa_pixel')

    if use_int_value:
        cloud_threshold = image.select('cloud').lt(ee.Number(image.get('cloudTresh')))
    else:
        cloud_threshold = image.select('cloud').lt(0.025)

    cond = cloud_threshold
    kernel = ee.Kernel.euclidean(60, 'meters')
    proximity = cond.distance(kernel, False)

    cond = cond.where(proximity.gt(0), 0)
    image = image.updateMask(cond)

    kernel = ee.Kernel.euclidean(60, 'meters')
    proximity = image.select('ndfi').unmask(-1).eq(-1).distance(kernel, False)

    cond = cond.where(proximity.gt(0), 0)
    image = image.updateMask(cond)

    return image

def get_dam(image):
    image = ee.Image(image)

    min_thresh = ee.Number(image.get('min'))
    max_thresh = ee.Number(image.get('max'))

    difference = image.select([1]).subtract(image.select([0]).unmask(image.select([1])))

    degradation = difference.gte(min_thresh).And(difference.lte(max_thresh))

    time_start = image.get('time')

    return degradation.set('system:time_start', time_start).rename('dam')

def dam_classify(collection, reducer, band_name, min_value, max_value, collection_target):
    collection = collection.select(band_name)
    collection_target = collection_target.select(band_name)

    reduced = ee.Image(collection.reduce(reducer)).rename('reduced')

    collection_target = collection_target.map(lambda image: image.addBands(reduced).set('min', min_value).set('max', max_value))
    collection_target = collection_target.map(get_dam)

    return collection_target

def spatial_filter(image, params):
    px_ruido = image.eq(params['class_target']) \
        .selfMask() \
        .connectedPixelCount(params['px_connected'] + 1, True)

    # px_ruido = px_ruido.mask(px_ruido.gte(params['px_connected']))

    px_ruido = px_ruido.multiply(ee.Image.pixelArea()).divide(10000)

    px_ruido = px_ruido.mask(px_ruido.gte(params['area_minima']))

    return image.where(px_ruido, 0).byte()


'''
    Input Data
'''


biome = ee.FeatureCollection(ASSET_BIOMES)\
    .filter('Bioma == "Amazônia"')
    

scenes = ee.FeatureCollection(ASSET_SCENES)\
    .filterBounds(biome.geometry())


scenes_centroid = scenes.map(lambda scene: scene.centroid() )

scenes_list = scenes.reduceColumns(ee.Reducer.toList(), ['SPRNOME']).get('list')\
    .getInfo()




'''
    Iterate DAM
'''

for i in all_params:

    y = i[0]

    # detection period
    dt_start = str(y) + '-01-01'
    dt_end = str(y) + '-12-30'

    # temporal window
    dt_start_window = str(y - i[1]['time_window'])  + '-01-01'
    dt_end_window = str(y - 1) + '-12-30'

    print(dt_start, dt_end, dt_start_window, dt_end_window)

    lulc = ee.Image(ASSET_MAPBIOMAS).select('classification_' + str(y))

    for scene in scenes_list:

        centroid = scenes_centroid.filter(ee.Filter.eq('SPRNOME', scene)).geometry()
        roi = scenes.filter(ee.Filter.eq('SPRNOME', scene)).geometry()

        collection = get_collection(
            dt_start_window, 
            dt_end_window, 
            cloud_cover, 
            centroid,
            i[1]['cloud_tresh'] 
        )

        ndfi_collection = collection.select('ndfi')

        ndfi_min = ndfi_collection.reduce(ee.Reducer.min())
        ndfi_mean = ndfi_collection.reduce(ee.Reducer.mean())

        if use_int_value:
            ndfi_collection = ndfi_collection.map(lambda image: image.mask(ndfi_min.gt(i[1]['mask_tresh'])).selfMask())
        else:
            ndfi_collection = ndfi_collection.map(lambda image: image.mask(ndfi_mean.gt(i[1]['mask_tresh'])).selfMask())
        

        collection_target = get_collection(
            dt_start, 
            dt_end, 
            cloud_cover, 
            centroid,
            i[1]['cloud_tresh'] 
        )

        ndfi_collection_target = collection_target.select('ndfi')

        dam_images = dam_classify(
            ndfi_collection,
            ee.Reducer.median(),
            'ndfi',
            i[1]['tresh_dam_min'],
            i[1]['tresh_dam_max'],
            ndfi_collection_target
        )

        degradation_frequency = ee.Image(dam_images.reduce(ee.Reducer.sum()))\
            .rename('degradation_frequency')
        
        degradation_frequency = degradation_frequency#.mask(lulc.eq(3)).selfMask()

        if use_spatial_filter:
            degradation_frequency = ee.Image(spatial_filter(degradation_frequency, spatial_filter_params)).selfMask()
            degradation_frequency = ee.Image(degradation_frequency).reproject(crs='epsg:4326', scale=30)
        


        degradation_frequency = degradation_frequency.updateMask(degradation_frequency) \
            .set('year', i[0]) \
            .set('biome', 'AMAZONIA') \
            .set('version', VERSION) \
            .set('tile', scene)


        # geometry_roi = ee.Image(collection.first()).geometry()

        outputName = scene.replace('/', '_') + '-' + str(i[0]) + '-' + VERSION

        print(f'exporting {outputName}')

        task = ee.batch.Export.image.toAsset(
            image=degradation_frequency,
            description=outputName,
            assetId=ASSET_OUTPUT + '/' + outputName,
            pyramidingPolicy={'.default': 'mode'},
            region=roi,
            scale=30,
            maxPixels=1e13
        )

        task.start()