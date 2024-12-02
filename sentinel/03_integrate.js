var palettesMb = require('users/mapbiomas/modules:Palettes.js').get('classification8');



var assetOutcrop = 'projects/ee-cgi-imazon/assets/mapbiomas/lulc_sentinel/probabilities_rock_3';

var assetWetland = 'projects/mapbiomas-workspace/TRANSVERSAIS/AMAZONIA/WETLAND-V2/CLASSIFICATION-SENTINEL-V2';

var assetLulc = 'projects/nexgenmap/MapBiomas2/SENTINEL/COLLECTION2/AMAZONIA/classification';

var assetOutput = 'projects/ee-cgi-imazon/assets/mapbiomas/lulc_sentinel/integrated'

var version = '5'

var years = [
    2016,
    2017,
    2018,
    2019,
    2020,
    2021,
    2022,
    2023
];





var visMb = {
  palette:palettesMb,
  min:0,max:62
}

var wetlandMask = ee.Image(assetWetland);

var outcropMask = ee.Image(assetOutcrop);



years.forEach(function(year){


    var lulcBase = ee.ImageCollection(assetLulc)
        .filter(ee.Filter.eq('year', year))
        .filter(ee.Filter.eq('version', '1'))
        .mosaic();

    
    var lulcInt = lulcBase.where(
      lulcBase.eq(3).and(wetlandMask.eq(1)), 6
    )
    
    lulcInt = lulcInt.where(
      lulcBase.neq(6).and(outcropMask.eq(1)), 11
    );
    
    lulcInt = lulcInt.where(
      outcropMask.eq(1), 29
    );
    
    var name = 'AMAZONIA-' + year.toString() +  '-' + version

    var classification = lulcInt.byte()
        .set('biome', 'AMAZONIA')
        .set('collection_id', 9.0)
        .set('territory', 'BRAZIL')
        .set('source', 'IMAZON')
        .set('version', version)
        .set('year', year)
        .set('description', '');


    Export.image.toAsset({
        image: classification,
        description: name,
        assetId: assetOutput + '/' + name,
        pyramidingPolicy: {'.default': 'mode'},
        region: geometry,
        maxPixels:1e13
    })

    

});

    
//Map.addLayer(outcropMask.selfMask(), {min:0, max:1, palette:['orange']}, 'rock')
//Map.addLayer(wetlandMask, {min:0, max:1, palette:['blue']}, 'wetland')










