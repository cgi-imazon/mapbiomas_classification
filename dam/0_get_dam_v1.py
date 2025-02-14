import ee

# Initialize the Earth Engine API
ee.Initialize()

# Configurações
assetLulc = 'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1'
assetTiles = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask'
assetRoi = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'
assetOutput = 'projects/ee-mapbiomas-imazon/assets/degradation/dam-frequency-c2'
version = '1'

defaultParams = {
    # 'mask_tresh': 120, # 150
    'tresh_dam_min': -0.250,
    'tresh_dam_max': -0.095,
    # 'cloud_tresh': 2, # threshold to mask clouds. It is very sensitive to results,
    'time_window': 3
}

listParams = [
    [2024, defaultParams],
    # [2023, defaultParams],
    # [2021, defaultParams],
    # [2020, defaultParams],
    # [2019, defaultParams],
    # [2018, defaultParams],
    # [2017, defaultParams],
    # [2016, defaultParams],
    # [2015, defaultParams],
    # [2014, defaultParams],
    # [2013, defaultParams],
    # [2012, defaultParams],
    # [2011, defaultParams],
    # [2010, defaultParams],
    # [2009, defaultParams],
    # [2008, defaultParams],
    # [2007, defaultParams],
    # [2006, defaultParams],
    # [2005, defaultParams],
    # [2004, defaultParams],
    # [2003, defaultParams],
    # [2002, defaultParams],
    # [2001, defaultParams],
    # [2000, defaultParams],
    # [1999, defaultParams],
    # [1998, defaultParams],
    # [1997, defaultParams],
    # [1996, defaultParams],
    # [1995, defaultParams],
    # [1994, defaultParams],
    # [1993, defaultParams],
    # [1992, defaultParams],
    # [1991, defaultParams],
    # [1990, defaultParams],
    # [1989, defaultParams],
    # [1988, defaultParams],
    # [1987, defaultParams],
]

months = ['01','02','03','04','05', '06', '07','08', '09','10', '11']

# Função para obter a coleção
def getCollection(dateStart, dateEnd, cloudCover, roi):
    bands = ['blue', 'green', 'red', 'nir', 'swir1', 'swir2', 'pixel_qa', 'tir']
    
    l5 = (ee.ImageCollection('LANDSAT/LT05/C02/T1_L2')
          .filter(ee.Filter.lte('CLOUD_COVER', cloudCover))
          .filterBounds(roi)
          .filterDate(dateStart, dateEnd)
          .map(lambda img: img.set('time', img.get('system:time_start'))))
    l5 = scaleFactorBands(l5).select(['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'QA_PIXEL', 'ST_B6'], bands)
    
    l7 = (ee.ImageCollection('LANDSAT/LE07/C02/T1_L2')
          .filter(ee.Filter.lte('CLOUD_COVER', cloudCover))
          .filterBounds(roi)
          .filterDate(dateStart, dateEnd)
          .map(lambda img: img.set('time', img.get('system:time_start'))))
    l7 = scaleFactorBands(l7).select(['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'QA_PIXEL', 'ST_B6'], bands)
    
    l8 = (ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
          .filter(ee.Filter.lte('CLOUD_COVER', cloudCover))
          .filterBounds(roi)
          .filterDate(dateStart, dateEnd)
          .map(lambda img: img.set('time', img.get('system:time_start'))))
    l8 = scaleFactorBands(l8).select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL', 'ST_B10'], bands)
    
    l9 = (ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
          .filter(ee.Filter.lte('CLOUD_COVER', cloudCover))
          .filterBounds(roi)
          .filterDate(dateStart, dateEnd)
          .map(lambda img: img.set('time', img.get('system:time_start'))))
    l9 = scaleFactorBands(l9).select(['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL', 'ST_B10'], bands)
    
    collection = l5.merge(l7).merge(l8).merge(l9)
    return collection

# Função para aplicar o fator de escala
def scaleFactorBands(collection):
    def scale(image):
        tmStart = image.get('system:time_start')
        tmEnd = image.get('system:time_end')

        optical_bands = image.select('SR_B.*').multiply(0.0000275).add(-0.2)
        thermal_bands = image.select('ST_B.*').multiply(0.00341802).add(149.0)

        return image.addBands(optical_bands, None, True).addBands(thermal_bands, None, True).selfMask()\
                    .copyProperties(image)\
                    .set('system:time_start', tmStart)\
                    .set('system:time_end', tmEnd)
    
    return collection.map(scale)

# Função para obter as frações
def getFractions(image):
    ENDMEMBERS = [
        [0.0119, 0.0475, 0.0169, 0.625, 0.2399, 0.0675],  # GV
        [0.1514, 0.1597, 0.1421, 0.3053, 0.7707, 0.1975],  # NPV
        [0.1799, 0.2479, 0.3158, 0.5437, 0.7707, 0.6646],  # Soil
        [0.4031, 0.8714, 0.79, 0.8989, 0.7002, 0.6607]  # Cloud
    ]
    
    fractions = ee.Image(image).select(['blue', 'green', 'red', 'nir', 'swir1', 'swir2']).unmix(ENDMEMBERS).max(0)
    fractions = fractions.rename(['gv', 'npv', 'soil', 'cloud'])

    summed = fractions.expression('b("gv") + b("npv") + b("soil")')
    shade = summed.subtract(1.0).abs().rename("shade")

    fractions = fractions.addBands(shade)
    return image.addBands(fractions)

# Função para calcular o NDFI
def getNdfi(image):
    summed = image.expression('b("gv") + b("npv") + b("soil")')
    gvs = image.select("gv").divide(summed).rename("gvs")

    npvSoil = image.expression('b("npv") + b("soil")')
    ndfi = ee.Image.cat(gvs, npvSoil).normalizedDifference().rename('ndfi')

    image = image.addBands(gvs)
    image = image.addBands(ndfi.clamp(-1, 1))

    return ee.Image(image)

# Função para remover nuvens
def removeCloud(image):
    cloudShadowBitMask = 1 << 3
    cloudsBitMask = 1 << 4

    qa = image.select('pixel_qa')
    mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0).And(qa.bitwiseAnd(cloudsBitMask).eq(0))

    return image.updateMask(mask).copyProperties(image)

# Função para remover sombra de nuvens
def removeCloudShadow(image):
    qa = image.select('qa_pixel')
    cloudThreshold = image.select('cloud').lt(0.025)

    cond = cloudThreshold
    kernel = ee.Kernel.euclidean(60, 'meters')
    proximity = cond.distance(kernel, False)

    cond = cond.where(proximity.gt(0), 0)
    image = image.updateMask(cond)

    proximity = image.select('ndfi').unmask(-1).eq(-1).distance(kernel, False)
    cond = cond.where(proximity.gt(0), 0)
    image = image.updateMask(cond)

    return image

# Função para criar banda temporal
def createTimeBand(image):
    return image.addBands(image.metadata('system:time_start').divide(1e18))

# Carregar a coleção de regiões
regions = ee.FeatureCollection(assetRoi).filter(ee.Filter.eq('Bioma', 'Amazônia'))

# Carregar a coleção de tiles
tiles = ee.ImageCollection(assetTiles).filterBounds(regions.geometry())
# tilesList = tiles.reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()

tilesList = [226068]

# Loop pelos anos e parâmetros
for params in listParams:
    year = params[0]
    lulc = ee.Image(assetLulc).select('classification_2023')

    start = f"{year}-01-01"
    end = f"{year}-12-30"

    for grid in tilesList:
        tileImage = ee.Image(tiles.filter(ee.Filter.eq('tile', grid)).first())
        roi = tileImage.geometry()
        center = roi.centroid()

        dictParams = params[1]

        collectionTarget = getCollection(start, end, 100, center)\
            .map(removeCloud)\
            .map(getFractions)\
            .map(getNdfi)\
            .select(['ndfi'])

        startTm = f"{year - dictParams['time_window'] + 1}-01-01"
        endTm = f"{year - 1}-01-01"

        collectionTimeWin = getCollection(startTm, endTm, 100, roi)\
            .map(removeCloud)\
            .map(getFractions)\
            .map(getNdfi)

        medianMonthly = collectionTimeWin.select('ndfi').reduce(ee.Reducer.median()).rename('metric')

        collectionDeviations = collectionTarget.map(lambda img: img.subtract(medianMonthly)
                                                     .updateMask(medianMonthly.gt(0.80))
                                                     .updateMask(lulc.eq(3))
                                                     .rename('deviation'))

        colDam = collectionDeviations.map(lambda image: image.expression('deviation >= min && deviation <= max', {
            'deviation': image.select('deviation'),
            'min': dictParams['tresh_dam_min'],
            'max': dictParams['tresh_dam_max']
        }))

        sumDam = colDam.sum()

        name = f'DAM_{year}_{grid}_{version}'
        assetId = f'{assetOutput}/{name}'

        print(f'Exporting {name}')

        # Exportar a imagem para o Earth Engine
        task = ee.batch.Export.image.toAsset(
            image=sumDam,
            description=name,
            assetId=assetId,
            pyramidingPolicy={'.default': 'mode'},
            region=roi,
            scale=30,
            maxPixels=1e13
        )
        task.start()
