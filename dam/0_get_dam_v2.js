

/**
 * 
 * assets
 * 
 */


var assetTilesLandsat = 'projects/mapbiomas-workspace/AUXILIAR/cenas-landsat';

var assetBiomes = 'projects/mapbiomas-workspace/AUXILIAR/biomas-2019';

var assetMapbiomas = '';

var assetOutput = '';

var outputVersion = '';


/**
 * 
 * config
 * 
 */


var customParams = {
    'mask_tresh': 0.7, // 150
    'tresh_dam_min': -0.250,
    'tresh_dam_max': -0.095,
    'cloud_tresh': 0.02, // treshould to mask clouds. It is very sensitive to results,
    'time_window': 3
}



var cloudCover = 100;


var params = [
    [2024, customParams]
]


/**
 * 
 * functions
 * 
*/


function getNdfi() {}

function getFractions() {}

function scaleFactor() {}

function removeCloud() {}

function getDam() {}

function damClassify() {}


/**
 * 
 * inputs
 * 
*/


