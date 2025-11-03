
from cci_tools.core.utils import recursive_find, STAC_API

_, collection_summary = recursive_find(f'{STAC_API}/collections/cci',[], depth=1)

total_empty = 0
total_filled = 0
empties, filled=[],[]
for c in collection_summary:
    collection = c[0]
    items = c[1]
    if items == 0:
        total_empty += 1
        empties.append(collection)
    else:
        total_filled += 1
        filled.append(collection)

print('DRS-level collection summary')
print('Contains Items:',total_filled)
print('No items:',total_empty)

print('(NOTE: This ignores "-main" NonDRS collections which have all been accounted for.)')

with open('empty.txt','w') as f:
    f.write('\n'.join(empties))

with open('filled.txt','w') as f:
    f.write('\n'.join(filled))
