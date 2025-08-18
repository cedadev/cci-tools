import pystac_client

url = "https://api.stac.ceda.ac.uk/collections/cci_openeo_test/items/ndvi-example-stac-item1"

it = pystac_client.Item.open(url)
x = 1