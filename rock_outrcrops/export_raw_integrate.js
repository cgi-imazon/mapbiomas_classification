var palettesMb = require('users/mapbiomas/modules:Palettes.js').get('classification8');
var palettes = require('users/gena/packages:palettes')  


// assets

var assetMosaics = 'projects/nexgenmap/MapBiomas2/LANDSAT/BRAZIL/mosaics-2';

var assetAmz = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019';

var assetLulc = 'projects/imazon-simex/LULC/COLLECTION9/integrated-transversal';



var assetOutput = 'projects/imazon-simex/LULC/COLLECTION9/classification-prob'

var assetProbs = 'projects/imazon-simex/LULC/COLLECTION9/probability';



// config

var featureSpace = [
  'ndfi_min',
  'ndfi_max',
  'ndfi_median_dry',
  'ndfi_median_wet',
  'ndfi_stdDev',
  'soil_min',
  'soil_max',
  'soil_median_dry',
  'soil_median_wet',
  'soil_stdDev',
  'gv_min',
  'gv_max',
  'gv_median_dry',
  'gv_median_wet',
  'gv_stdDev',
  'gvs_min',
  'gvs_max',
  'gvs_median_dry',
  'gvs_median_wet',
  'gvs_stdDev',
  'shade_min',
  'shade_max',
  'shade_median_dry',
  'shade_median_wet',
  'shade_stdDev',
  'slope',
  'npv_min',
  'npv_max',
  'npv_median_dry',
  'npv_median_wet',
  'npv_stdDev'
];



var rfParams = {
    'numberOfTrees': 50,
    // 'variablesPerSplit': 4,
    // 'minLeafPopulation': 25
}





var years = [
  // 2023, 
  // 2022,
  // 2021, 2020, 2019, 2018, 2017, 2016, 2015, 2014,
  // 2013, 2011, 2010, 2009, 2008, 2007, 2006, 2005,
  // 2004, 2003, 1999, 1998, 1997, 1996, 1995, 1994,
  // 1993, 1992, 1990, 1989, 1988, 1987, 1986, 
  1985
];








// input data

var amzBiome = ee.FeatureCollection(assetAmz)
    .filter(ee.Filter.eq('Bioma', 'Amazônia')); 



// historical serie classificated (probability)
var flooded = ee.ImageCollection("projects/mapbiomas-workspace/TRANSVERSAIS/AMAZONIA/WETLAND-V2/CLASSIFICATION");

// image with water flood maximum
var maxFlood = ee.Image("projects/mapbiomas-workspace/TRANSVERSAIS/AMAZONIA/WETLAND-V2/floodMAX");


// maximum pixel values
var maxProbsFlood = flooded.max();
    maxProbsFlood = maxProbsFlood.where(maxFlood.eq(0), 0)
    .multiply(100).selfMask();

var lulcMode = ee.ImageCollection(assetLulc)
    .filter('version == "22"')
    .mode()




// 

function fillGap(image, lulcInt) {
  
    var geomFix = ee.Image(1).clip(geometry)

    image = image.unmask(0);

    image = image.where(image.eq(0), lulcInt)
    image = image.where(geomFix.eq(1), lulcInt)

    return image.selfMask()

}







// iterate 


var vis = {
  palette:palettesMb,
  min:0,max:62
}

years.forEach(function(year){
  
    
    var lulcInt = ee.Image('projects/imazon-simex/LULC/COLLECTION9/integrated-transversal/AMAZONIA-' + String(year) + '-22')
  
  
    var clsRaw = ee.ImageCollection(assetProbs)
        .filter('year == ' + String(year))
        //.select('probabilities')
        .select('classification')
        .max()

    var lulc = ee.ImageCollection(assetLulc)
        .filter('year == ' + String(year) + 'and version == "22"')
        .mosaic()
        
    var ref = lulc.eq(29).clip(amzBiome)


    var image = clsRaw.where(lulc.eq(6), 6)
        image = image.where(lulc.eq(11), 11)
        image = image.where(lulc.eq(29), 29)
        image = image.updateMask(lulc.gt(0))

    image = fillGap(image, lulcInt)

    
    image =  image.set('biome', 'AMAZONIA')
            .set('collection_id', 9.0)
            .set('territory', 'BRAZIL')
            .set('source', 'IMAZON')
            .set('version', '1')
            .set('year', year)
            .set('description', 'mapa bruto sem filtros');
            
            
    Map.addLayer(image, vis)
              

    var desc = 'AMAZONIA' + '_' + year + '_1' 

    Export.image.toAsset({
      image: image,
      description: desc,
      assetId: assetOutput  + '/' + desc,
      pyramidingPolicy: {'.default': 'mode'},
      scale: 30,
      region: roi,
      maxPixels:1e13
    })
  


});
















/*
  
    var classified = currentImage.classify(classifier)
        classified = classified.byte();
        classified = classified
            .set('biome', 'AMAZONIA')
            .set('collection_id', 8.0)
            .set('territory', 'BRAZIL')
            .set('source', 'IMAZON')
            .set('version', 1)
            .set('year', year)
            .set('description', 'classificacao de áreas construidas.');
            
    

    
    Map.addLayer(classified.randomVisualizer(), {}, grid)
    
    var desc = 'impervious_grid_' + grid + '_' + year + '_1' 

    Export.image.toAsset({
      image: classified,
      description: desc,
      assetId: 'users/jailson/impervious/classification-c2/' + desc,
      pyramidingPolicy: {'.default': 'mode'},
      scale: 30,
      region: currentGrid.geometry(),
      maxPixels:1e13
    })
*/