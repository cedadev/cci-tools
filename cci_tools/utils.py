__author__    = "Daniel Westwood"
__contact__   = "daniel.westwood@stfc.ac.uk"
__copyright__ = "Copyright 2025 United Kingdom Research and Innovation"

import httpx
import json
import click

dryrun = True

from httpx_auth import OAuth2ClientCredentials

def open_json(file):
    with open(file) as f:
        return json.load(f)

creds = open_json('AUTH_CREDENTIALS')

auth = OAuth2ClientCredentials(
    "https://accounts.ceda.ac.uk/realms/ceda/protocol/openid-connect/token",
    client_id=creds["id"],
    client_secret=creds["secret"]
)

STAC_API = 'https://api.stac.164.30.69.113.nip.io'

client = httpx.Client(
    verify=False,
    timeout=180,
)