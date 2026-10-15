# Functions relating to uploading itemsets
from cci_tools.core.utils import STAC_API, client, auth
import json
import logging
from cci_tools.core.utils import logstream, set_verbose

logger = logging.getLogger(__name__)
logger.addHandler(logstream)
logger.propagate = False

STAC_API = ''

def open_itemset(itempath: str):
    """
    Open an itemset from the GWS archive
    """

    with open(itempath) as f:
        itemset = json.load(f)

    if isinstance(itemset,dict):
        itemset = [itemset]

    upload_itemset(itemset)

def upload_itemset(itemset: list):
    """
    Upload all items in an itemset to the STAC API 
    with the correct collection
    """

    for item in itemset:
        dataset_id = item['collection']
        item_id = item['id']

        if client.get(STAC_API + '/collections/' + dataset_id).status_code != 200:
            logger.error(f'Unable to post {item_id}: Corresponding collection {dataset_id} does not exist.')


        # Construct paths for STAC collection STAC item
        stac_collection = STAC_API + "/collections/" + dataset_id + "/items"
        stac_item = stac_collection + "/" + item_id
    
        # Post a new STAC record
        response = client.post(stac_collection, json=item, auth=auth)
    
        # If the STAC record already exists, just update it
        if response.status_code == 409:
            response = client.put(stac_item, json=item, auth=auth)