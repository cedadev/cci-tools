import rasterio
from rasterio.io import MemoryFile
from rio_cogeo.profiles import cog_profiles
from rio_cogeo.cogeo import cog_translate
import sys
import click
import os
import glob


@click.command()
@click.argument('file')
@click.argument('outdir')
def main(file: str, outdir: str):
    """
    Convert single file or directory of files
    """
    if os.path.isfile(file):
        geo2cog(file, outdir)
    
    for f in glob.glob(f'{file}/*'):
        geo2cog(f, outdir)

def geo2cog(file: str, outdir: str = None):

    with rasterio.Env(GDAL_DRIVER_NAME='SRTMHTG'):
        with rasterio.open(file) as src:
            arr = src.read()
            kwargs = src.meta

    kwargs['predictor'] = 2

    if outdir is not None:
        newfile = f"{outdir}/{file.split('/')[-1].replace('.tif','_COG.tif')}"
    else:
        newfile = file.replace('.tif','_COG.tif')

    print(file, newfile)

    with MemoryFile() as memfile:
        with memfile.open(**kwargs) as mem:
            mem.write(arr)
            dst_profile = cog_profiles.get('deflate')
            cog_translate(mem, newfile, dst_profile, use_cog_driver=True, in_memory=False)

if __name__ == '__main__':
    main()