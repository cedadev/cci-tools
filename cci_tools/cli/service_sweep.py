import requests
import json

from slack_sdk import WebClient

OTC_SERVICES = {
    'Opensearch (Live)':'https://archive.opensearch.ceda.ac.uk/',
    'Opensearch (Test)':'https://opensearch-test.ceda.ac.uk/',
    'Vocab Server':'https://vocab.ceda.ac.uk',
    'Data Bridge':'https://eo-data-bridge.ceda.ac.uk',
    'Postgres DB':'https://eo-data-bridge.ceda.ac.uk/dataset/',
    'STAC API':'https://api.stac.164.30.69.113.nip.io'
}


if __name__ == '__main__':

    slack_client = None
    with open('/home/users/dwest77/cedadev/cci/cci-tools/config/slack_cfg.json') as f:
        slack_cfg = json.load(f)

    if slack_cfg is not None:

        token = slack_cfg['token']
        channel = slack_cfg['channel']
        slack_client = WebClient(token=token)

    msg = []
    for service, url in OTC_SERVICES.items():
        r = requests.get(url).status_code
        sitrep = ':red_circle:'
        if str(r)[0] == '2':
            sitrep = ':large_green_circle:'
        print(f'{service} : {sitrep} ({r})')
        msg.append(sitrep)

    message = [
        'Service Status Report:',
        f'Services: {", ".join(list(OTC_SERVICES.keys()))}',
        ' '.join(msg)]

    if slack_client is not None:
        slack_client.chat_postMessage(
            channel=channel, 
            text='\n'.join(message),
            username=f'CCI Service Sweeper'
        )