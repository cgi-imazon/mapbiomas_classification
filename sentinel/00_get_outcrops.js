

var ASSET_ROI = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019'

var ASSET_MOSAICS =  'projects/mapbiomas-mosaics/assets/SENTINEL/BRAZIL/mosaics-3'

var ASSET_LANDSAT_LULC = 'projects/mapbiomas-public/assets/brazil/lulc/collection9/mapbiomas_collection90_integration_v1'

var ASSET_MASK = 'projects/ee-cgi-imazon/assets/mapbiomas/lulc_sentinel/roi_outcrop'



var mask = ee.Image(ASSET_MASK)

var roi = ee.FeatureCollection(ASSET_ROI).filter('Bioma == "Amazônia"')

var lulcLandsat = ee.Image(ASSET_LANDSAT_LULC).select('classification_2023').clip(roi.geometry());

var outcropTemplate = lulcLandsat.eq(29).updateMask(mask.eq(1))

var mosaic = ee.ImageCollection(ASSET_MOSAICS)
    .filter('biome == "AMAZONIA" and year == 2023').mosaic().updateMask(outcropTemplate.gte(0));






Map.addLayer(mosaic, {
  bands:['red_median', 'green_median', 'blue_median'],
  min:0,max:2050
});

Map.addLayer(mask, {min:0 , max:1})

Map.addLayer(outcropTemplate, {min:0 , max:1})


/*

Export.image.toAsset({image: regionToClassify, 
  description: 'roi_outcrop', 
  assetId:'projects/ee-cgi-imazon/assets/mapbiomas/lulc_sentinel/roi_outcrop',
  pyramidingPolicy: {
    '.default':'mode'
  }, region:geometry, scale:30, maxPixels:1e13
})
*/