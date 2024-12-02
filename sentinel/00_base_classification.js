/**
 * 
 */
var asset = "projects/nexgenmap/MapBiomas2/SENTINEL/COLLECTION2/AMAZONIA/classification";

var Palettes = require('users/mapbiomas/modules:Palettes.js');
var palette = Palettes.get('classification9');

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

var collection = ee.ImageCollection(asset);

print(collection.size());

var maps = years.map(
    function (year) {
        var map = ui.Map({style:{border: '1px solid black'}});

        var vis = {
            'bands': 'classification',
            'min': 0,
            'max': 69,
            'palette': palette,
            'format': 'png',
        };
        
        var collectionYear = collection
          .filter(ee.Filter.eq('year', year))
          .filter(ee.Filter.eq('version', '1'));
          
        print(year, collectionYear.size())
        
        map.addLayer(collectionYear, vis, year.toString());

        map.setControlVisibility(false);

        map.add(ui.Label(year.toString(), { 'padding': '0px' }));

        map.setCenter(-54.9407, -8.6721, 9);

        return map;
    }
);

ui.root.widgets().reset([
    ui.Panel(
        maps,
        ui.Panel.Layout.flow('horizontal', true),
        {
            'stretch': 'vertical',
            // 'width': '100%'
        }
    )]);

ui.Map.Linker(maps);
