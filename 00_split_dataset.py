
import sys, os

sys.path.append(os.path.abspath('.'))

import ee

from utils.helpers import *


#service_account = 'sad-deep-learning-274812@appspot.gserviceaccount.com'
#credentials = ee.ServiceAccountCredentials(service_account, 'mapping_legal_amazon/config/account-sad-deep-learning.json')

PROJECT = 'sad-deep-learning-274812'

ee.Initialize(project=PROJECT)


'''

    Config Session

'''

ASSET_ROI = 'projects/imazon-simex/LULC/LEGAL_AMAZON/biomes_legal_amazon'

# this must be your partition raw fc samples
ASSET_SAMPLES = 'projects/mapbiomas-workspace/VALIDACAO/mapbiomas_85k_col4_points_w_edge_and_edited_v1'
ASSET_OUTPUT = 'projects/imazon-simex/LULC/COLLECTION9/SAMPLES'


INPUT_FEATURES = [
    'gv', 
    'npv', 
    'soil', 
    'cloud',
    'gvs',
    'ndfi', 
    'csfi'
]


'''

    Input Data

'''

roi = ee.FeatureCollection(ASSET_ROI)

samples = ee.FeatureCollection(ASSET_SAMPLES)\
    .filter('AMOSTRAS == "Treinamento"')\
    .filterBounds(roi)


'''

    Partition

'''



samples_partition = samples.randomColumn()

samples_train = samples_partition.filter('random < 0.3')
samples_val = samples_partition.filter('random >= 0.3')



'''

    Export

'''    


name_train = 'mapbiomas_85k_col3_points_w_edge_and_edited_v2_train'
name_val = 'mapbiomas_85k_col3_points_w_edge_and_edited_v2_val'



task_train = ee.batch.Export.table.toAsset(
    collection=samples_train,
    description=name_train,
    assetId=ASSET_OUTPUT + '/' + name_train,
)

task_val = ee.batch.Export.table.toAsset(
    collection=samples_val,
    description=name_val,
    assetId=ASSET_OUTPUT + '/' + name_val,
)




task_train.start()
task_val.start()


    










