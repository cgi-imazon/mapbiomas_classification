// author: Jailson S.
// this script shows the DAM product



var palettes = require('users/gena/packages:palettes');
var palettesMb = require('users/mapbiomas/modules:Palettes.js');
var mapp = require('users/joaovsiqueira1/packages:Mapp.js');




/**
 * 
 * assets 
 * 
*/


var assetBiomesF = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019-raster';
var assetDam = 'projects/imazon-simex/DEGRADATION/dam-frequency-c2';
var assetDamClassified = 'projects/imazon-simex/DEGRADATION/dam-frequency-c2-cls';
var assetMapbiomas = 'projects/mapbiomas-workspace/public/collection8/mapbiomas_collection80_integration_v1'

/**
 * 
 * config variables 
 * 
*/

var years = [
  //1985,1986,
  1987,1988,1989,1990,
  1991,1992,1993,1994,1995,1996,1997,
  1998,1999,2000,2001,2002,2003,2004,
  2005,2006,2007,2008,2009,2010,2011,
  2012,
  2013,2014,2015,2016,2017,2018,2019, 
  2020,2021,2022
];


var params = {
  'px_connected': 100,
  'min_pix':50
}

/**
 * 
 * input data 
 * 
*/

var amazonia = ee.Image(assetBiomesF).eq(1);

// general dam
var damCollection = ee.ImageCollection(assetDam)
    .filter('tile == ""')

// dam classified in fire and logging
var damCollectionCls = ee.ImageCollection(assetDamClassified)


var vis = {
    'min': 0,
    'max': 12, 
    'palette': ['red', 'brown', 'black'],
    'format': 'png'
};

var visMb = {
    'min': 0,
    'max': 62, 
    'palette': palettesMb.get('classification8') ,
    'format': 'png'
};

years.forEach(function(year) {
  
    
    var dam = damCollection.filter('year == ' + String(year));
        dam = ee.Image(dam.first()).mask(amazonia).selfMask();
        
        
    var damCls = damCollectionCls.filter('year == ' + String(year));
        damCls = ee.Image(damCls.first()).mask(amazonia).selfMask();
        
    var lulc = ee.Image(assetMapbiomas).updateMask(amazonia).select('classification_' + String(year));
        
        
    Map.addLayer(lulc, visMb, 'mapbiomas c8 - ' + String(year), false);
    
    Map.addLayer(dam, vis, 'dam general - ' + String(year), false);

    Map.addLayer(damCls, {
      'min': 0,
      'max': 12, 
      'palette': ['red', 'brown', 'black'],
      'bands': ['dam_logging_' + String(year)],
      'format': 'png'
    }, 'dam logging - ' + String(year), false);

    Map.addLayer(damCls, {
      'min': 0,
      'max': 12, 
      'palette': ['red', 'brown', 'black'],
      'bands': ['dam_fires_' + String(year)],
      'format': 'png'
    }, 'dam fire - ' + String(year), false);
  
  
});