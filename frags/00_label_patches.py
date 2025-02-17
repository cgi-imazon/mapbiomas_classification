import numpy as np
import os
import rasterio

from osgeo import gdal
from skimage import measure

'''
    Config Session
'''

PATH_CLASSIFICATION = './data/forest_chunk.tif'
PATH_LABEL = './data/patch_label.tif'

'''
    Input Data
'''

# labeling and losing geospatial information:

classification = rasterio.open(PATH_CLASSIFICATION).read()

labeled_classfication = measure.label(classification, background=0,connectivity=2) # label Classification
labeled_classfication_transposed = np.transpose(labeled_classfication, (1,2,0))

dataset=gdal.Open(PATH_CLASSIFICATION)

projection = dataset.GetProjection()
geo_transform = dataset.GetGeoTransform()

drv = gdal.GetDriverByName("GTiff")
dst_ds = drv.Create(PATH_LABEL,
                    labeled_classfication.shape[1],
                    labeled_classfication.shape[0],
                                1,
                                gdal.GDT_CInt32,['COMPRESS=DEFLATE',
                                                   'BIGTIFF=YES',
                                                   'PREDICTOR=1',
                                                   'TILED=YES'])
dst_ds.SetProjection(projection)
dst_ds.SetGeoTransform(geo_transform)
dst_ds.GetRasterBand(1).WriteArray(labeled_classfication)