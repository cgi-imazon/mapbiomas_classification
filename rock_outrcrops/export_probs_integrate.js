var palettesMb = require('users/mapbiomas/modules:Palettes.js').get('classification8');
var palettes = require('users/gena/packages:palettes')  


// assets

var assetMosaics = 'projects/nexgenmap/MapBiomas2/LANDSAT/BRAZIL/mosaics-2';

var assetAmz = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019';

var assetLulc = 'projects/imazon-simex/LULC/COLLECTION9/integrated-transversal';



var assetOutput = 'projects/imazon-simex/LULC/COLLECTION9/probability-transversal'

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
  // 1993, 1992, 1990, 1989, 1988, 1987, 1986
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










function fillGap(image) {
  
    var probsGeneral = ee.ImageCollection(assetProbs)
        .select('probabilities')
        .mean()
        
    image = image.unmask(0);

    image = image.where(image.eq(0), probsGeneral)

    return image.selfMask()

}





// iterate 

years.forEach(function(year){

  
    var probsGeneral = ee.ImageCollection(assetProbs)
        .filter('year == ' + String(year))
        .select('probabilities')
        .max()

    var lulc = ee.ImageCollection(assetLulc)
        .filter('year == ' + String(year) + 'and version == "22"')
        .mosaic()
        
    var ref = lulc.eq(29).clip(amzBiome)

    var targetArea = ref.distance(ee.Kernel.euclidean(10)).gte(0);
    
    
    
    var refTarget = ref.mask(targetArea).rename('label');
    
    var image = ee.ImageCollection(assetMosaics).filter('year == ' + String(year))
        .mosaic()
        .select(featureSpace)
        .clip(amzBiome).mask(targetArea);
        
      
        
        
    var fs = image.addBands(refTarget);
        
    

    
    // get samples
    var samples = fs.sample({region:geometry, scale:30, numPixels:5000, geometries:true});
    
    
        

            // classify
    var classifierProb = ee.Classifier.smileRandomForest(rfParams)
        .setOutputMode('MULTIPROBABILITY')
        .train(samples, 'label', featureSpace)


    var probabilitiesRock = ee.Image(fs
        .classify(classifierProb)
        .rename(['probability'])
        .copyProperties(image)
        .copyProperties(image, ['system:footprint'])
        .copyProperties(image, ['system:time_start'])
    )
    
    probabilitiesRock = probabilitiesRock
        .arrayProject([0])
        .arrayFlatten([['no_rock', 'rock']])
        .select('rock')
        .multiply(100)
        //.reduce(ee.Reducer.max())



    
    // integrate probs
    var probMap = probsGeneral.where(lulc.eq(29), probabilitiesRock)
        probMap = probMap.where(lulc.eq(6).or(lulc.eq(11)), maxProbsFlood)
        
        
        
    probMap = fillGap(probMap).updateMask(lulc.gt(0)).byte()
    
    var probabilities =  probMap.set('biome', 'AMAZONIA')
            .set('collection_id', 9.0)
            .set('territory', 'BRAZIL')
            .set('source', 'IMAZON')
            .set('version', 1)
            .set('year', year)
            .set('description', 'probabilidades integradas mapa imazon');
              
    Map.addLayer(probabilities)

    var desc = 'probability' + '_' + year + '_1' 

    Export.image.toAsset({
      image: probabilities,
      description: desc,
      assetId: assetOutput  + '/' + desc,
      pyramidingPolicy: {'.default': 'mode'},
      scale: 30,
      region: roi,
      maxPixels:1e13
    })
  


});










