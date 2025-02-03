

var assetBiomesF = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019-raster';



var years = [
  2016, 
  2017, 
  2018, 
  2019, 
  2020, 
  2021, 
  2022, 
  2023  
]

var yearsAgree = [
  [2017, 2018, 11]
]


var amazonia = ee.Image(assetBiomesF).eq(1);

var mapbiomas_palette = require('users/mapbiomas/modules:Palettes.js').get('classification9')

var sentinelCollection =  ee.ImageCollection('projects/mapbiomas-workspace/COLECAO9-S2/integracao')
    .filter(ee.Filter.eq('version', '0-4'))
    .max().updateMask(amazonia);
    



var agreement = yearsAgree.map(function(pairYears){
  
  var from = sentinelCollection.select('classification_' + pairYears[0].toString()); 
  var to = sentinelCollection.select('classification_' + pairYears[1].toString());
  
  
  var replacedBy = to.updateMask(from.subtract(to).neq(0));
  var fromWet = replacedBy.updateMask(from.eq(11));
  
  
  
  
  var pxArea = ee.Image.pixelArea().divide(10000);
  
  var reducer = ee.Reducer.sum().group(1, 'label');
  
  var dataArea = pxArea.addBands(fromWet).reduceRegion({
    reducer: reducer,
    geometry: geometry,
    scale: 10,
    maxPixels: 1e12
  });
  
  var listArea = ee.List(dataArea.get('groups'));
  
  listArea = listArea.map(function(obj) {
    obj = ee.Dictionary(obj);
    
    return ee.Feature(null)
      .set('label', obj.get('label'))
      .set('area', obj.get('sum'))
      .set('year', 2017)
  });
  
  var area = ee.FeatureCollection(listArea)
  
  Export.table.toDrive({
    collection: area, description: 'areas_agree_wet', fileFormat: 'CSV'})
  
  
  Map.addLayer(replacedBy, {palette: mapbiomas_palette, min:0, max: 69}, 'replaced by')
  Map.addLayer(replacedBy.updateMask(from.eq(pairYears[2])), {palette: mapbiomas_palette, min:0, max: 69}, 'it was 11')
});













