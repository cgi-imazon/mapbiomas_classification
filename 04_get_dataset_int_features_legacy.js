//
/**
  * Asset 1 - 1985 a 2019
  * projects/imazon-simex/LULC/TEST/classification
  * Descrição - Coleção geral dos dados de classificação usados a partir da coleção 4
    var version = {
        "1": 79410, // sem segmentos, sem savana e campo (c4)
        "2": 76733, // com segmentos, sem savana e campo (c5 e c6)
        "3": 3266,  // teste
        "4": 202,   // teste
        "5": 103,   // teste
        "6": 14,    // teste
        "7": 18,    // teste
        "8": 19,    // teste
        "9": 16885, // ecotono teste 1
        "10": 10,   // teste
        "11": 10,   // teste
        "12": 245   // teste
    }
 */
var asset1 = 'projects/imazon-simex/LULC/classification';
var collection1 = ee.ImageCollection(asset1)
    .filter(ee.Filter.eq("version", "2"));

/**
 * Asset 2 - 1985 a 2020
 * projects/imazon-simex/LULC/TEST/classification
 * Descrição - Coleção contendo dados completos para o ano de 2020 e
 * as cenas selecionadas para a classificação de campo e savana
    var version = {
        "1": 43366,   // com segmentos, com savana e campo (c6)
        "null": 3161  // checar esses dados (provavél bug no versionamento)
    }
 * 
*/
var asset2 = 'projects/imazon-simex/LULC/TEST/classification';
var collection2 = ee.ImageCollection(asset2);

/**
 * Asset 3 - 1985 a 2020
 * projects/imazon-simex/LULC/TEST/classification-2
 * Descrição - Dados revisados para a coleção 6
    var version = {
        "2": 3352  // com segmentos, com savana e campo (c6 revisão)
    }
 * 
 */
//
var asset3 = 'projects/imazon-simex/LULC/TEST/classification-2';
var collection3 = ee.ImageCollection(asset3)
    .filter(ee.Filter.eq("version", "2"));

/**
 * Asset 4 - 2021 
 * projects/imazon-simex/LULC/COLLECTION7/classification
 * Descrição - Coleção geral dos dados de classificação usados para a coleção 7
    var version = {
        "1": 23,    // teste
        "2": 23,    // teste
        "3": 23,    // teste
        "4": 3472   // com segmentos, com savana e campo, com filtro de segmento (c7)
    }
 */
var asset4 = 'projects/imazon-simex/LULC/COLLECTION7/classification';
var collection4 = ee.ImageCollection(asset4)
    .filter(ee.Filter.eq("version", "4"));

/**
 * Asset 5 - 1985 a 2021
 * projects/imazon-simex/LULC/COLLECTION6/classification_review
 * Descrição - Dados revisados para a coleção 7
    var version = {
        "1": 5248 // com segmentos, com savana e campo (c7 revisado)
    }
 */

var asset5 = 'projects/imazon-simex/LULC/COLLECTION6/classification_review';
var collection5 = ee.ImageCollection(asset5)
    .filter(ee.Filter.eq("version", "1"));

/**
 * Coleção final
 */
var setName = function (image) {
    return image.set('name', ee.String(image.get('system:index')).slice(0, 20));
};

// Dados 1985-2021 revisados para a coleção 7
collection5 = collection5.map(setName);

print("Imagens REVISADAS para a C7:", collection5.aggregate_array('name'));

// Dados 2021 pré-revisão da coleção 7
// Remove as imagens com o mesmo nome dos dados revisados
collection4 = collection4.map(setName)
    .filter(ee.Filter.inList("name", collection5.aggregate_array('name')).not());

//print("Imagens NÃO REVISADAS para a C7:", collection4.aggregate_array('name'));

// Atualiza a colecao com os dados revisados
var collectionFinal = collection5.merge(collection4);

//print("Imagens NOVAS para a C7:", collectionFinal.aggregate_array('name'));

//
collection3 = collection3.map(setName)
    .filter(ee.Filter.inList("name", collectionFinal.aggregate_array('name')).not());

// Atualiza a colecao com os dados revisados
collectionFinal = collectionFinal.merge(collection3);
//print("Imagens NOVAS para a C7 + REVISADAS C6:", collectionFinal.aggregate_array('name'));

//
collection2 = collection2.map(setName)
    .filter(ee.Filter.inList("name", collectionFinal.aggregate_array('name')).not());

// Atualiza a colecao com os dados revisados
collectionFinal = collectionFinal.merge(collection2);
//print("Imagens NOVAS para a C7 + REVISADAS C6 + 2020 C6, CAMPO/SAVANA:", collectionFinal.aggregate_array('name'));
//
collection1 = collection1.map(setName)
    .filter(ee.Filter.inList("name", collectionFinal.aggregate_array('name')).not());

// Atualiza a colecao com os dados revisados
collectionFinal = collectionFinal.merge(collection1);
//print("Imagens NOVAS para a C7 + REVISADAS C6 + 2020 C6, CAMPO/SAVANA + C5/C6 SEM CAMPO/SAVANA:", collectionFinal.aggregate_array('name'));

// Remap classes
collectionFinal = collectionFinal.map(
    function (image) {
        return image
            .where(image.eq(19), 18)
            .where(image.eq(13), 12);
    }
)
//
var palettes = require('users/mapbiomas/modules:Palettes.js');
var paletteMapBiomas = palettes.get('classification6');

var visMapBiomas = {
    min: 0,
    max: 49,
    palette: paletteMapBiomas,
    format: 'png'
};

var years = [
    1985, 1986, 1987, 1988,
    1989, 1990, 1991, 1992,
    1993, 1994, 1995, 1996,
    1997, 1998, 1999, 2000,
    2001, 2002, 2003, 2004,
    2005, 2006, 2007, 2008,
    2009, 2010, 2011, 2012,
    2013, 2014, 2015, 2016,
    2017, 2018, 2019, 2020,
    2021
];

var outputVersion = '1';
var assetOutput = 'projects/imazon-simex/LULC/feature-space'

years.forEach(
    function (year) {
        var dateStart = year.toString() + '-01-01';
        var dateEnd = year.toString() + '-12-31';

        var classificationYear = collectionFinal.filter(ee.Filter.date(dateStart, dateEnd))
            .map(function(image) { return image.unmask(0); });
        
        
        // Number of observations in the year
        var nObservationsYear = classificationYear
            .map(function(image) { return image.gt(0).unmask(0); })
            .reduce(ee.Reducer.sum())
            .rename('observations_year');
        
        // Transitions in the year
        var transitionsYear = classificationYear
            .reduce(ee.Reducer.countRuns())
            .divide(nObservationsYear)
            .rename('transitions_year');
        
        // Distinct observations in the year
        var distinctYear = classificationYear
            .reduce(ee.Reducer.countDistinctNonNull())
            .rename('distinct_year');
        
        // Mode of the year
        var modeYear = classificationYear
            .reduce(ee.Reducer.mode())
            .rename('mode_year');
        
        // Occurrence of forest in the year
        var forestYear = classificationYear
            .map(function(image) { return image.eq(3); })
            .reduce(ee.Reducer.sum())
            .divide(nObservationsYear)
            .rename('occurrence_forest_year');
        
        // Occurrence of savanna in the year
        var savannaYear = classificationYear
            .map(function(image) { return image.eq(4); })
            .reduce(ee.Reducer.sum())
            .divide(nObservationsYear)
            .rename('occurrence_savanna_year');
        
        // Occurrence of grassland in the year
        var grasslandYear = classificationYear
            .map(function(image) { return image.eq(12); })
            .reduce(ee.Reducer.sum())
            .divide(nObservationsYear)
            .rename('occurrence_grassland_year');
        
        // Occurrence of pasture in the year
        var pastureYear = classificationYear
            .map(function(image) { return image.eq(15); })
            .reduce(ee.Reducer.sum())
            .divide(nObservationsYear)
            .rename('occurrence_pasture_year');
        
        // Occurrence of agriculture in the year
        var agricultureYear = classificationYear
            .map(function(image) { return image.eq(18); })
            .reduce(ee.Reducer.sum())
            .divide(nObservationsYear)
            .rename('occurrence_agriculture_year');
        
        // Occurrence of water in the year
        var waterYear = classificationYear
            .map(function(image) { return image.eq(33); })
            .reduce(ee.Reducer.sum())
            .divide(nObservationsYear)
            .rename('occurrence_water_year');
                
                
                
        // image feature space
        var image = modeYear
            .addBands(transitionsYear)
            .addBands(distinctYear)
            .addBands(nObservationsYear)
            .addBands(forestYear)
            .addBands(savannaYear)
            .addBands(grasslandYear)
            .addBands(pastureYear)
            .addBands(agricultureYear)
            .addBands(waterYear)
            
        
        image = image
            .set('year', year)
            .set('version', outputVersion)
            .set('collection_id', 9.0)
        

        var name = 'feature-space-' + String(year) + '-' + '9-' + outputVersion


        Export.image.toAsset({
          image: image, 
          description: name, 
          assetId: assetOutput + '/' + name, 
          pyramidingPolicy: {'.default':'mode'}, 
          region: geometry, 
          scale: 30, 
          maxPixels: 1e13
        });

        
        
        
        

        Map.addLayer(classificationYear.mode(), visMapBiomas, year.toString(), false);
    }
);



















