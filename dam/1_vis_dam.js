var palettes = require('users/gena/packages:palettes');
var palettesMb = require('users/mapbiomas/modules:Palettes.js');






// config
var assetDamMosaico = 'projects/ee-mapbiomas-imazon/assets/degradation/dam-frequency-c2';

var assetDamCenas = 'projects/sad-deep-learning-274812/assets/degradation/dam/dam-df-c2';

var years = [
    2024, 
    //2023,
    //2021, 
    //2020,
    //2019
]

// select all frequencies greater than below
var filterFrequency = 1;




// inputs
var damCenas = ee.ImageCollection(assetDamCenas);

var damMosaico = ee.ImageCollection(assetDamMosaico);



// visualize
years.forEach(
    function(year) {
      
        var imgDamCena = damCenas.filter('year == ' + year.toString());
        var imgDamMosa = damMosaico.filter('year == ' + year.toString());

        
        // select deforestation and distubance by type of DAM 
        var distDamCena = imgDamCena.select('freq_dam').min();
        
        var defDamCena = imgDamCena.select('freq_dam_df').min();
        
        var distDamMosa = imgDamMosa.select('freq_dam').min();
        
        var defDamMosa = imgDamMosa.select('freq_dam_df').min();
        
        
        
        
        
        // filter by frequency
        defDamCena = defDamCena.updateMask(defDamCena.gt(filterFrequency))
        
        distDamCena = distDamCena.updateMask(distDamCena.gt(filterFrequency))
        
        defDamMosa = defDamMosa.updateMask(defDamMosa.gt(filterFrequency))
        
        distDamMosa = distDamMosa.updateMask(distDamMosa.gt(filterFrequency))
        
        
    
    
    
        /**
         * 
         * display dam distubances (both with processed scenes and monthly mosaic)
         * 
         */
         
        Map.addLayer(distDamMosa, {
         // min:6, max:75,
          min:1, max:10,
          palette:['#53ffc9', '#da64ff', '#ae1e66']
        }, year.toString() + '-DAM MOSAICO DISTÚRBIO', false);
        
    
        Map.addLayer(distDamCena, {
          min:1, max:30,
          //palette:palettes.cmocean.Thermal[7]
          //palette:['#edff00', '#ff8a22', '#ff0000']
          palette: ['#53ffc9', '#da64ff', '#ae1e66']
        },  year.toString() + '-DAM CENA DISTÚRBIO', false);        
    
    
        /**
         * 
         * display dam deforestation (both with processed scenes and monthly mosaic)
         * 
         */
         
        Map.addLayer(defDamMosa, {
         // min:6, max:75,
          min:1, max:10,
          //palette:['#53ffc9', '#da64ff', '#ae1e66']
          palette:['#edff00', '#ff8a22', '#ff0000']
        }, year.toString() + '-DAM MOSAICO DEF', false);
        
    
        Map.addLayer(defDamCena, {
          min:1, max:30,
          //palette:palettes.cmocean.Thermal[7]
          palette:['#edff00', '#ff8a22', '#ff0000']
          //palette: ['#53ffc9', '#da64ff', '#ae1e66']
        },  year.toString() + '-DAM CENA DEF', false);   
         
    }
);












