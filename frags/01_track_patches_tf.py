import os
import rasterio
import tensorflow as tf
import numpy as np

tfrecord_dir = "path/to/tfrecords"

# Função para carregar dados do TFRecord
def parse_tfrecord(example_proto):
    feature_description = {
        "image": tf.io.FixedLenFeature([], tf.string),
        "shape": tf.io.FixedLenFeature([2], tf.int64),
    }
    example = tf.io.parse_single_example(example_proto, feature_description)
    image = tf.io.decode_raw(example["image"], tf.uint8)
    shape = example["shape"]
    image = tf.reshape(image, shape)
    return tf.cast(image, tf.int32)

def load_tfrecord(file_path):
    dataset = tf.data.TFRecordDataset(file_path).map(parse_tfrecord)
    return np.array(list(dataset.as_numpy_iterator()))

# Identificar patches usando TensorFlow
def identify_patches(block):
    block_tensor = tf.convert_to_tensor(block, dtype=tf.int32)
    labeled_tensor = tf.image.connected_components(block_tensor)
    return labeled_tensor.numpy()

# Rastrear patches ao longo dos anos
def track_patches(base_raster, yearly_rasters):
    previous_raster = base_raster.copy()
    patch_map = {}
    processed_rasters = {}

    for year, raster in yearly_rasters.items():
        new_raster = np.zeros_like(previous_raster)
        labeled_block = identify_patches(raster)

        unique_patches = np.unique(labeled_block[labeled_block > 0])

        for patch in unique_patches:
            mask = labeled_block == patch
            old_patches = np.unique(previous_raster[mask])
            old_patches = old_patches[old_patches > 0]

            if len(old_patches) == 1:
                # Patch mantém o ID
                new_raster[mask] = old_patches[0]
            elif len(old_patches) > 1:
                # Junção de patches -> Novo ID
                new_id = np.max(new_raster) + 1
                new_raster[mask] = new_id
                patch_map[new_id] = {"parent": list(old_patches)}
            else:
                # Novo patch -> Novo ID
                new_id = np.max(new_raster) + 1
                new_raster[mask] = new_id
                patch_map[new_id] = {"parent": None}

        processed_rasters[year] = new_raster
        previous_raster = new_raster.copy()

    return processed_rasters, patch_map

# Carregar arquivos TFRecord
tfrecord_files = sorted([f for f in os.listdir(tfrecord_dir) if f.endswith(".tfrecord")])
rasters = {int(f.split("_")[-1].split(".")[0]): load_tfrecord(os.path.join(tfrecord_dir, f)) for f in tfrecord_files}

# Processar rastreamento
base_year = min(rasters.keys())
base_raster = rasters[base_year]
processed_rasters, patch_map = track_patches(base_raster, {y: r for y, r in rasters.items() if y > base_year})

# Salvar resultados em TFRecord
def write_tfrecord(output_path, image_array):
    shape = np.array(image_array.shape, dtype=np.int64)
    serialized_image = tf.io.encode_raw(tf.convert_to_tensor(image_array, dtype=tf.uint8))
    feature = {
        "image": tf.train.Feature(bytes_list=tf.train.BytesList(value=[serialized_image.numpy()])),
        "shape": tf.train.Feature(int64_list=tf.train.Int64List(value=shape))
    }
    example = tf.train.Example(features=tf.train.Features(feature=feature))
    
    with tf.io.TFRecordWriter(output_path) as writer:
        writer.write(example.SerializeToString())

for year, raster in processed_rasters.items():
    output_path = os.path.join(tfrecord_dir, f"processed_forest_patches_{year}.tfrecord")
    write_tfrecord(output_path, raster)
