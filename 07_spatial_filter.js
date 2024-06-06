


var palettesMb = require('users/mapbiomas/modules:Palettes.js').get('classification8');

var assetRoi = 'projects/imazon-simex/LULC/LEGAL_AMAZON/biomes_legal_amazon'
var asset = 'projects/imazon-simex/LULC/LEGAL_AMAZON/integrated'





var roi = ee.FeatureCollection(assetRoi).geometry()
var image = ee.ImageCollection(asset).select('classification').mosaic()
    .clip(roi);



function spatialFilterArea(image) {

  var objectId15 = image.eq(15).selfMask().connectedComponents({
    connectedness: ee.Kernel.plus(2),
    maxSize: 256
  });
  
  var objectId18 = image.eq(18).selfMask().connectedComponents({
    connectedness: ee.Kernel.plus(2),
    maxSize: 256
  });
  
  
  
  
  var objectSize15 = objectId15.select('labels').connectedPixelCount({
    maxSize: 256, eightConnected: false
  });
  
  var objectSize18 = objectId18.select('labels').connectedPixelCount({
    maxSize: 256, eightConnected: false
  });
  
  
  
  
  var pixelArea = ee.Image.pixelArea().divide(10000);
  
  
  
  var objectArea15 = objectSize15.multiply(pixelArea);
  var objectArea18 = objectSize18.multiply(pixelArea);
  
  
  objectArea15 = objectArea15.lte(6).unmask(0);
  objectArea18 = objectArea18.lte(6).unmask(0);
  
  
  
  
  var kernel = ee.Kernel.plus({radius: 1});
  
  
  
  
  var buffer15 = image.mask(objectArea15.eq(0))
               .focalMax({kernel: kernel, iterations: 1})
               .reproject({scale:30, crs:'epsg:4326'})
               .mask(objectArea15.eq(1));
      
  var buffer18 = image.mask(objectArea18.eq(0))
               .focalMax({kernel: kernel, iterations: 1})
               .reproject({scale:30, crs:'epsg:4326'})
               .mask(objectArea15.eq(1));

  objectId15 = objectId15.mask(objectArea15.eq(1)).addBands(buffer15, null, true);
  
  
  
  
  // replace noisy by mode of border
  var replacement = objectId15.reduceConnectedComponents({
    reducer:ee.Reducer.mode(), labelBand:'labels', maxSize:256
  });
  
  
  
  
  return [objectArea15, replacement]
  

}

function majorityFilter(image) {
  
  var kernel = ee.Kernel.manhattan(1);
  
  image = image.reduceNeighborhood({
    reducer: ee.Reducer.mode(), 
    kernel: kernel
  });
  
  return image.reproject('epsg:4326', null, 30)
  
}


var imageFiltered = majorityFilter(image)

var around = spatialFilterArea(imageFiltered);

var visMb = {
  palette:palettesMb,
  min:0,max:62
}



Map.addLayer(image, visMb, 'c9', true);

Map.addLayer(imageFiltered, visMb, 'c9 filtered', false);


Map.addLayer(around[0].selfMask(), {min:0, max:1, palette:['black','white']}, 'target')
Map.addLayer(around[1], visMb, 'around')

