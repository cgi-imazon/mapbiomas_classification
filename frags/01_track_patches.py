import os
import numpy as np
import rasterio
from rasterio.features import shapes
from scipy.ndimage import label


'''

    Config

'''

raster_dir = "path/to/rasters"


'''

    Functions

'''

def load_raster(file_path):
    with rasterio.open(file_path) as src:
        return src.read(1), src.profile


def identify_patches(binary_array):
    labeled_array, num_features = label(binary_array)
    return labeled_array, num_features


def track_patches(base_raster, yearly_rasters):
    parent_ids = np.unique(base_raster[base_raster > 0])
    patch_map = {pid: {'current_area': np.sum(base_raster == pid), 'childs': []} for pid in parent_ids}

    unique_id = max(parent_ids) + 1  # Próximo ID único para novos patches
    previous_raster = base_raster.copy()

    for year, raster in yearly_rasters.items():
        new_raster = np.zeros_like(raster)

        for parent_id in patch_map:
            mask = previous_raster == parent_id
            if np.any(mask):
                intersection = raster[mask]
                unique_values = np.unique(intersection[intersection > 0])

                if len(unique_values) == 1:
                    new_raster[mask] = parent_id  # Patch manteve-se inteiro
                elif len(unique_values) > 1:
                    for val in unique_values:
                        child_mask = (raster == val) & mask
                        if np.any(child_mask):
                            new_raster[child_mask] = unique_id
                            patch_map[parent_id]['childs'].append(unique_id)
                            unique_id += 1
                else:
                    patch_map[parent_id]['current_area'] = np.sum(mask)

        # Identificar patches novos
        orphan_patches, num_orphans = label((raster > 0) & (new_raster == 0))
        for orphan_id in range(1, num_orphans + 1):
            new_raster[orphan_patches == orphan_id] = unique_id
            unique_id += 1

        previous_raster = new_raster.copy()
        yearly_rasters[year] = new_raster  # Atualiza a entrada com os novos IDs

    return yearly_rasters, patch_map


'''

    Input
  
'''

raster_files = sorted([f for f in os.listdir(raster_dir) if f.endswith(".tif")])

rasters = {}
for file in raster_files:
    year = int(file.split("_")[-1].split(".")[0])  # Extrai o ano do nome do arquivo
    rasters[year], profile = load_raster(os.path.join(raster_dir, file))


'''

    Run

'''


base_year = min(rasters.keys())
processed_rasters, patch_map = track_patches(rasters[base_year], {y: r for y, r in rasters.items() if y > base_year})


for year, raster in processed_rasters.items():
    output_path = os.path.join(raster_dir, f"processed_forest_patches_{year}.tif")
    with rasterio.open(output_path, "w", **profile) as dst:
        dst.write(raster, 1)
