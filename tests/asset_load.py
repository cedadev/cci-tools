from openeo.local import LocalConnection
lc = LocalConnection('./')
url = "https://api.stac.ceda.ac.uk/collections/cci_openeo_test/items/ndvi-example-stac-item1"
print(lc.load_stac(url=url, bands=['ndvi']).execute())