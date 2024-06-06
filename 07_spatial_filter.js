


var palettesMb = require('users/mapbiomas/modules:Palettes.js').get('classification8');

var assetRoi = 'projects/imazon-simex/LULC/LEGAL_AMAZON/biomes_legal_amazon'
var asset = 'projects/imazon-simex/LULC/LEGAL_AMAZON/integrated'





var roi = ee.FeatureCollection(assetRoi).geometry()
var image = ee.ImageCollection(asset).select('classification').mosaic()
    .clip(roi);



function spatialFilterArea(image, classe) {
  
  var pixelArea = ee.Image.pixelArea().divide(10000);

  var objectId = image.eq(classe).selfMask().connectedComponents({
    connectedness: ee.Kernel.plus(2),
    maxSize: 256
  });
  
  
  var objectSize = objectId.select('labels').connectedPixelCount({
    maxSize: 256, eightConnected: false
  });
  
  
  var objectArea = objectSize.multiply(pixelArea);
      objectArea = objectArea.lte(3).unmask(0);

  
  var kernel = ee.Kernel.plus({radius: 1});
  
  var buffer = image.mask(objectArea.eq(0))
               .focalMax({kernel: kernel, iterations: 1})
               .reproject({scale:30, crs:'epsg:4326'})
               .mask(objectArea.eq(1));
      

  objectId = objectId.mask(objectArea.eq(1)).addBands(buffer, null, true);

  
  
  // replace noisy by mode of border
  var replacement = objectId.reduceConnectedComponents({
    reducer:ee.Reducer.mode(), labelBand:'labels', maxSize:256
  });
  
  var imageFiltered = image.where(objectArea.eq(1), replacement);

  return imageFiltered
  

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
    imageFiltered = spatialFilterArea(imageFiltered, 15);
    imageFiltered = spatialFilterArea(imageFiltered, 18);

var visMb = {
  palette:palettesMb,
  min:0,max:62
}



Map.addLayer(image, visMb, 'c9', true);

Map.addLayer(imageFiltered, visMb, 'c9 filtered', false);


//Map.addLayer(around[0].selfMask(), {min:0, max:1, palette:['black','white']}, 'target')
Map.addLayer(imageFiltered, visMb, 'filtered')

