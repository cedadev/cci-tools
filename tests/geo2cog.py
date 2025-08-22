import rasterio
from rasterio.io import MemoryFile
from rio_cogeo.profiles import cog_profiles
from rio_cogeo.cogeo import cog_translate
import sys

def geo2cog(file: str, cachedir: str = None):

    with rasterio.Env(GDAL_DRIVER_NAME='SRTMHTG'):
        with rasterio.open(file) as src:
            arr = src.read()
            kwargs = src.meta

    kwargs['predictor'] = 2

    if cachedir is not None:
        newfile = f"{cachedir}/{file.split('/')[-1].replace('.tif','_COG.tif')}"
    else:
        newfile = file.replace('.tif','_COG.tif')

    with MemoryFile() as memfile:
        with memfile.open(**kwargs) as mem:
            mem.write(arr)
            dst_profile = cog_profiles.get('deflate')
            cog_translate(mem, newfile, dst_profile, use_cog_driver=True, in_memory=False)

if __name__ == '__main__':
    geo2cog(sys.argv[-1])