var palettes = require('users/gena/packages:palettes');

// config


//var assetLulc = 'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1';
var assetLulc = 'projects/mapbiomas-raisg/public/collection3/mapbiomas_raisg_panamazonia_collection3_integration_v2'


var assetTiles = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask';

// var assetRoi = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019';
var assetRoi = 'projects/mapbiomas-raisg/DATOS_AUXILIARES/VECTORES/biomas-paises'

var assetOutput = 'projects/ee-mapbiomas-imazon/assets/degradation/dam-frequency-c2'

var version = '1'







var defaultParams = {
    'tresh_dam_min': -0.250,
    'tresh_dam_max': -0.095,
    'tresh_df_min': -0.250,
    'time_window': 3
}

var listParams = [
    [2024, defaultParams],
    [2023, defaultParams],
    [2021, defaultParams],
    [2020, defaultParams],
    [2019, defaultParams],
    // [2018, defaultParams],
    // [2017, defaultParams],
    // [2016, defaultParams],
    // [2015, defaultParams],
    // [2014, defaultParams],
    // [2013, defaultParams],
    // [2012, defaultParams],
    // [2011, defaultParams],
    // [2010, defaultParams],
    // [2009, defaultParams],
    // [2008, defaultParams],
    // [2007, defaultParams],
    // [2006, defaultParams],
    // [2005, defaultParams],
    // [2004, defaultParams],
    // [2003, defaultParams],
    // [2002, defaultParams],
    // [2001, defaultParams],
    // [2000, defaultParams],
    // [1999, defaultParams],
    // [1998, defaultParams],
    // [1997, defaultParams],
    // [1996, defaultParams],
    // [1995, defaultParams],
    // [1994, defaultParams],
    // [1993, defaultParams],
    // [1992, defaultParams],
    // [1991, defaultParams],
    // [1990, defaultParams],
    // [1989, defaultParams],
    // [1988, defaultParams],
    // [1987, defaultParams],
]







var regions = ee.FeatureCollection(assetRoi)
    .filter('name == "Amazonía"')
    
var roi = geometry; 


//var tiles = ee.ImageCollection(assetTiles).filterBounds(regions.geometry());

// var tilesList = tiles.reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()





// get collection
/*
function getCollection(dateStart, dateEnd, cloudCover, roi) {
  
    var collection = null;
    
    var l5 = null;
    var l7 = null;
    var l8 = null;
    var l9 = null;
    
    var bands = [
      'blue',
      'green',
      'red',
      'nir',
      'swir1',
      'swir2',
      'pixel_qa',
      'tir'
    ]
    

    l5 = ee.ImageCollection('LANDSAT/LT05/C02/T1_L2')
            .filter('CLOUD_COVER <= ' + cloudCover)
            .filterBounds(roi)
            .filterDate(dateStart, dateEnd)
            .map(function(img){return img.set('time', img.get('system:time_start'))});
            
    
            
    l5 = scaleFactorBands(l5) 
            .select(
              ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'QA_PIXEL', 'ST_B6'], 
              bands
            )
        
    

    
    l7 = ee.ImageCollection('LANDSAT/LE07/C02/T1_L2')
            .filter('CLOUD_COVER <= ' + cloudCover)
            .filterBounds(roi)
            .filterDate(dateStart, dateEnd)
            .map(function(img){return img.set('time', img.get('system:time_start'))});
            
    l7 = scaleFactorBands(l7)  
            .select(
              ['SR_B1', 'SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B7', 'QA_PIXEL', 'ST_B6'], 
              bands
            )

    
    
    
    
    
    l8 = ee.ImageCollection('LANDSAT/LC08/C02/T1_L2')
            .filter('CLOUD_COVER <= ' + cloudCover)
            .filterBounds(roi)
            .filterDate(dateStart, dateEnd)
            .map(function(img){return img.set('time', img.get('system:time_start'))});
    
    l8 = scaleFactorBands(l8)
            .select(
              ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL', 'ST_B10'], 
              bands
            )

    
    
    
    
            
    l9 = ee.ImageCollection('LANDSAT/LC09/C02/T1_L2')
            .filter('CLOUD_COVER <= ' + cloudCover)
            .filterBounds(roi)
            .filterDate(dateStart, dateEnd)
            .map(function(img){return img.set('time', img.get('system:time_start'))});
            
    l9 = scaleFactorBands(l9)
            .select(
              ['SR_B2', 'SR_B3', 'SR_B4', 'SR_B5', 'SR_B6', 'SR_B7', 'QA_PIXEL', 'ST_B10'], 
              bands
            )
            
    
    collection = l5.merge(l7).merge(l8).merge(l9);
    
    
    
    
    
    return collection;

}
*/

function getCollection(dateStart, dateEnd, cloudCover, roi){
    var col = ee.ImageCollection('LANDSAT/COMPOSITES/C02/T1_L2_32DAY')
        .filterDate(dateStart, dateEnd)
        .filterBounds(roi)
        .map(function(img){return img.clip(geometry)})
    
    return col
}


function scaleFactorBands(collection) {

    collection = collection.map(function(image) { 
        var tmStart = image.get('system:time_start');
        var tmEnd = image.get('system:time_end');
        
        
        var optical_bands = image.select('SR_B.').multiply(0.0000275).add(-0.2)
        var thermal_bands = image.select('ST_B.*').multiply(0.00341802).add(149.0)
    
        return image.addBands(optical_bands, null, true).addBands(thermal_bands, null, true).selfMask()
            .copyProperties(image)
            .set('system:time_start', tmStart)
            .set('system:time_end', tmEnd);
    })

    return collection
}


function getFractions(image) {
      // default endmembers
      var ENDMEMBERS = [
          [0.0119,0.0475,0.0169,0.625,0.2399,0.0675], // GV
          [0.1514,0.1597,0.1421,0.3053,0.7707,0.1975], // NPV
          [0.1799,0.2479,0.3158,0.5437,0.7707,0.6646], // Soil
          [0.4031,0.8714,0.79,0.8989,0.7002,0.6607] // Cloud
      ]

      var outBandNames = ['gv', 'npv', 'soil', 'cloud']
      
      var fractions = ee.Image(image)
          .select(['blue', 'green', 'red', 'nir', 'swir1', 'swir2'])
          .unmix(ENDMEMBERS) 
          .max(0);
          //.multiply(100) 
          //.byte() ;
      
      fractions = fractions.rename(outBandNames);
      
      var summed = fractions.expression('b("gv") + b("npv") + b("soil")');
      
      var shade = summed 
          .subtract(1.0) 
          .abs() 
          //.byte() 
          .rename("shade");

      fractions = fractions.addBands(shade);
      
      return image.addBands(fractions);
}


function getNdfi(image){
      var summed = image.expression('b("gv") + b("npv") + b("soil")')
  
      var gvs = image.select("gv").divide(summed).rename("gvs");
  
      var npvSoil = image.expression('b("npv") + b("soil")');
  
      var ndfi = ee.Image.cat(gvs, npvSoil) 
          .normalizedDifference() 
          .rename('ndfi');
  
      // rescale NDFI from 0 to 200 \
      //ndfi = ndfi.expression('byte(b("ndfi") * 100 + 100)');
  
      image = image.addBands(gvs);
      image = image.addBands(ndfi.clamp(-1, 1));
  
      return ee.Image(image);
}


function removeCloud(image) {

    var cloudShadowBitMask = 1 << 3;
    var cloudsBitMask = 1 << 4;

    // Get the pixel QA band.
    var qa = image.select('pixel_qa');

    var mask = qa.bitwiseAnd(cloudShadowBitMask).eq(0)
                .and(qa.bitwiseAnd(cloudsBitMask).eq(0));

    return image.updateMask(mask).copyProperties(image);
}


function createTimeBand(image) {
  return image.addBands(image.metadata('system:time_start').divide(1e18));
}







listParams.forEach(function(params){
      
    var year = params[0];
    var dictParams = params[1];
    
    var band = 'classification_' + (year - 1).toString()
    
    if (year >= 2020) { band = 'classification_2019' }
    
    var lulc = ee.Image(assetLulc).select(band)




    var start = String(year) + '-01-01';
    var end = String(year) + '-12-30';
    
      var startTm = String((year - (dictParams['time_window']) + 1))  + '-01-01';
    var endTm = String(year - 1)  + '-01-01';
    
    
    
    
    var collectionTarget = getCollection(start, end, 100, roi)
        //.map(removeCloud)
        .map(getFractions)
        .map(getNdfi)
        .select(['ndfi']);
        
    var collectionTimeWin = getCollection(startTm, endTm, 100, roi)
        //.map(removeCloud)
        .map(getFractions)
        .map(getNdfi)
        .select(['ndfi']);
        
        
        
        
    
    var medianMonthly = collectionTimeWin.reduce(ee.Reducer.median()).rename('metric');

    var collectionDeviations = collectionTarget.map(function(img) {
        var deviation = img.subtract(medianMonthly)
            .updateMask(medianMonthly.gt(0.80))
            .updateMask(lulc.eq(3))
            .rename('deviation');
        return deviation.copyProperties(img);
    });
    
    
    
    
    var colDam = collectionDeviations.map(function(image){
      return image.expression('deviation >= min && deviation <= max', {
        'deviation': image.select('deviation'),
        'min': dictParams['tresh_dam_min'],
        'max': dictParams['tresh_dam_max']
      }).copyProperties(image)
    });

    var colDamDf = collectionDeviations.map(function(image){
      return image.expression('deviation <= min', {
        'deviation': image.select('deviation'),
        'min': dictParams['tresh_df_min']
      }).copyProperties(image);
    });
    
    
    var sumDam = colDam.sum().rename('freq_dam').selfMask().byte();
    var sumDamDf = colDamDf.sum().rename('freq_dam_df').selfMask().byte();
    
    var imageExport = sumDam.addBands(sumDamDf);

    imageExport = imageExport
        .set('year', year)
        .set('version', version)
    
    
    
    
    
    
    Map.addLayer(sumDam.updateMask(sumDam.gt(1)), {
     // min:6, max:75,
      min:1, max:10,
      palette:palettes.cmocean.Thermal[7]
    }, 'freq dam');
    

    Map.addLayer(sumDamDf.updateMask(sumDamDf.gt(1)), {
      min:1, max:10,
      palette:palettes.cmocean.Thermal[7]
    }, 'freq dam df');        
    
    // export session
    var name =  'DAM_' + year.toString() + '_' + version
    var assetId = assetOutput + '/' + name;
    
    print('exporting ', name)
    
    Export.image.toAsset({
      //image: colDamNorm, 
      image: imageExport, 
      description: name, 
      assetId:assetId, 
      pyramidingPolicy: {'.default': 'mode'}, 
      region: roi, 
      scale: 30, 
      maxPixels:1e13
    });
    
    
    
});


