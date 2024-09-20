import ee


def get_fractions(image: ee.image.Image) -> ee.image.Image:

    # default endmembers
    ENDMEMBERS = [
        [0.0119,0.0475,0.0169,0.625,0.2399,0.0675], # GV
        [0.1514,0.1597,0.1421,0.3053,0.7707,0.1975], # NPV
        [0.1799,0.2479,0.3158,0.5437,0.7707,0.6646], # Soil
        [0.4031,0.8714,0.79,0.8989,0.7002,0.6607] #  Cloud
    ]

    outBandNames = ['gv', 'npv', 'soil', 'cloud']
    
    
    fractions = ee.Image(image).select(['blue', 'green', 'red', 'nir', 'swir1', 'swir2'])\
        .unmix(ENDMEMBERS)\
        .max(0)


    fractions = fractions.rename(outBandNames)

    summed = fractions.expression('b("gv") + b("npv") + b("soil")')

    shade = summed.subtract(1.0).abs().rename("shade")

    fractions = fractions.addBands(shade)

    return image.addBands(fractions)


def get_ndfi(image: ee.image.Image) -> ee.image.Image:
    """Calculate NDFI and add it to image fractions

    Parameters:
        image (ee.Image): Fractions image containing the bands:
        gv, npv, soil, cloud

    Returns:
        ee.Image: Fractions image with NDFI bands
    """
    summed = image.expression('b("gv") + b("npv") + b("soil")')

    gvs = image.select("gv").divide(summed).rename("gvs")

    npv_soil = image.expression('b("npv") + b("soil")')

    ndfi = ee.Image.cat(gvs, npv_soil)\
            .normalizedDifference()\
            .rename('ndfi')
    
    image = image.addBands(gvs)
    image = image.addBands(ndfi)

    return ee.Image(image)


def get_csfi(image: ee.image.Image) -> ee.image.Image:
    """Calculate CSFI and add it to image fractions

    Parameters:
        image (ee.Image): Fractions image containing the bands:
        gv, npv, soil, cloud

    Returns:
        ee.Image: Fractions image with csfi bands
    """

    csfi = image.expression(
        "(float(b('gv') - b('shade'))/(b('gv') + b('shade')))")

    csfi = csfi.multiply(100).add(100).byte().rename(['csfi'])

    image = image.addBands(csfi)

    return ee.Image(image)


def apply_scale_factors(image: ee.image.Image) -> ee.image.Image:
    optical_bands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
    thermal_bands = image.select('ST_B.*').multiply(0.00341802).add(149.0)

    return image.addBands(optical_bands, None, True).addBands(thermal_bands, None, True)


def remove_cloud(image: ee.image.Image) -> ee.image.Image:
    # Bits 3 and 5 are cloud shadow and cloud, respectively.
    cloudShadowBitMask = 1 << 3
    cloudsBitMask = 1 << 4

    #Get the pixel QA band.
    qa = image.select('pixel_qa')

    # Both flags should be set to zero, indicating clear conditions.
    mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0).And(qa.bitwiseAnd(cloudsBitMask).eq(0))


    return image.updateMask(mask).copyProperties(image)


def remove_cloud_s2(collection: ee.imagecollection.ImageCollection) -> ee.imagecollection.ImageCollection:

    CLEAR_THRESHOLD = 0.60

    cloud_prob = ee.ImageCollection('GOOGLE/CLOUD_SCORE_PLUS/V1/S2_HARMONIZED')    

    colFreeCloud = collection.linkCollection(cloud_prob, ['cs'])\
        .map(lambda image: 
            image.updateMask(image.select('cs').gte(CLEAR_THRESHOLD))
                    .copyProperties(image)
                    .copyProperties(image, ['system:footprint'])
                    .copyProperties(image, ['system:time_start'])
        )
    
    return colFreeCloud