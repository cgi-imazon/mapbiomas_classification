var palettes = require('users/gena/packages:palettes');

// config


var assetLulc = 'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1';

var assetTiles = 'projects/mapbiomas-workspace/AUXILIAR/landsat-mask';

var assetRoi = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019';

var assetOutput = 'projects/ee-mapbiomas-imazon/assets/degradation/dam-frequency-c2'

var version = '1'



var defaultParams = {
    //'mask_tresh': 120, // 150
    'tresh_dam_min': -0.250,
    'tresh_dam_max': -0.095,
    //'cloud_tresh': 2, // treshould to mask clouds. It is very sensitive to results,
    'time_window': 2
}

var listParams = [
    //[2024, defaultParams],
    [2023, defaultParams],
    // [2021, defaultParams],
    // [2020, defaultParams],
    // [2019, defaultParams],
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

var months = ['01','02','03','04','05', '06', '07','08', '09','10', '11']


// get collection
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


function removeCloudShadow(image) {
    var qa = image.select('qa_pixel');

  
    var cloudThreshold = image.select('cloud').lt(0.025);

    var cond = cloudThreshold;
    var kernel = ee.Kernel.euclidean(60, 'meters');
    var proximity = cond.distance(kernel, false);

    cond = cond.where(proximity.gt(0), 0);
    image = image.updateMask(cond);

    proximity = image.select('ndfi').unmask(-1).eq(-1).distance(kernel, false);

    cond = cond.where(proximity.gt(0), 0);
    image = image.updateMask(cond);

    return image;
}



function createTimeBand(image) {
  return image.addBands(image.metadata('system:time_start').divide(1e18));
}







var regions = ee.FeatureCollection(assetRoi)
    .filter('Bioma == "Amazônia"')
    
var tiles = ee.ImageCollection(assetTiles).filterBounds(regions.geometry());

var tilesList = tiles.reduceColumns(ee.Reducer.toList(), ['tile']).get('list').getInfo()




listParams.forEach(function(params){
      
    var year = params[0];
    
    var lulc = ee.Image(assetLulc).select('classification_' + year.toString())

    var start = String(year) + '-01-01';
    var end = String(year) + '-12-30';
    
    
    tilesList.forEach(function(grid){
      
        var tileImage = ee.Image(tiles.filter('tile == ' + grid).first())

        var roi = tileImage.geometry()

        var center = roi.centroid()
        
        var dictParams = params[1];
        
    
        var collectionTarget = getCollection(start, end, 100, center)
            .map(removeCloud)
            .map(getFractions)
            .map(getNdfi)
     
            
        var startTm = String((year - (dictParams['time_window']) + 1))  + '-01-01';
        var endTm = String(year - 1)  + '-01-01';
        
        
        
        
        
        var deviations = months.map(function(m) {
            var monthInt = parseInt(m);
        
            var collectionTimeWin = getCollection(startTm, endTm, 100, roi)
                .map(removeCloud)
                .filter(ee.Filter.calendarRange(monthInt, monthInt, 'month'))
                .map(getFractions)
                .map(getNdfi);
      
        
            // Se a coleção estiver vazia, retorna uma lista vazia e interrompe a execução
            return ee.Algorithms.If(
                collectionTimeWin.size().eq(0),
                ee.List([]),
                ee.Algorithms.If(
                    collectionTarget.select('ndfi').filter(ee.Filter.calendarRange(monthInt, monthInt, 'month')).size().eq(0),
                    ee.List([]),
                    (function() {
                        var medianMonthly = collectionTimeWin.select('ndfi').reduce(ee.Reducer.median()).rename('metric');
        
                        var collectionMonthly = collectionTarget.select('ndfi')
                            .filter(ee.Filter.calendarRange(monthInt, monthInt, 'month'));
        
        
                        var collectionDeviations = collectionMonthly.map(function(img) {
                            var deviation = img.subtract(medianMonthly)
                                .updateMask(medianMonthly.gt(0.80))
                                .updateMask(lulc.eq(3))
                                .rename('deviation');
                            return deviation.copyProperties(img);
                        });
        
                        return collectionDeviations.toList(collectionDeviations.size());
                    })()
                )
            );
        });
        
        
        var colDeviation = ee.ImageCollection(ee.List(deviations).flatten())//.filter(ee.Filter.gt('system:time_start', 1704067200000));
        
        
        
        var colDam = colDeviation.map(function(image){
          return image.expression('deviation >= min && deviation <= max', {
            'deviation': image.select('deviation'),
            'min': dictParams['tresh_dam_min'],
            'max': dictParams['tresh_dam_max']
          });
        });
        
        var sumDam = colDam.sum();
      
        //
        var validObservations = colDeviation.map(function(img) {
            return img.unmask(100).neq(100); // Conta somente valores válidos (não nulos)
        }).sum();
                
        var colDamNorm = sumDam.divide(validObservations).multiply(100).byte();        
        
        // export session
        var name =  'DAM_' + year.toString() + '_' +  grid.toString() + '_' + version
        var assetId = assetOutput + '/' + name;
        
        print('exporting ', name)
        
        Export.image.toAsset({
          image: colDamNorm, 
          //image: sumDam, 
          description: name, 
          assetId:assetId, 
          pyramidingPolicy: {'.default': 'mode'}, 
          region: roi, 
          scale: 30, 
          maxPixels:1e13
        })
    });  
});


