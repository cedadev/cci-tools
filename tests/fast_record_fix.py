import glob
import json
records_dir = '/home/users/dwest77/cedadev/cci/cci-tools/records/esacci.BIOMASS.yr.L4.AGB.multi-sensor.multi-platform.MERGED.6-0.100m-fv5.01.openeo'

#new_collection = 'esacci.BIOMASS.yr.L4.AGB.multi-sensor.multi-platform.MERGED.6-0.100m-fv6.0.openeo'.lower()

for x, file in enumerate(glob.glob(f'{records_dir}/*.json')):
    print(x)

    with open(file) as f:
        refs = json.load(f)

    refs['id'] = refs['id'].replace('fv5.0.','fv5.01.')

    with open(file,'w') as f:
        f.write(json.dumps(refs))