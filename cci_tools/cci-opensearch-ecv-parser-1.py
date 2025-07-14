#!/usr/bin/env python3
import argparse
import requests
import os
import sys
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from requests.exceptions import ConnectionError
from requests.exceptions import RetryError
import re
import json as JSON
from xml.dom import minidom
from datetime import date
from datetime import datetime
from pathlib import Path
import xml.etree.ElementTree as ET
import shutil

ns = {'cci': 'http://a9.com/-/spec/opensearch/1.1/',
      'ceda': 'http://localhost/ceda/opensearch',
      'eo': 'http://a9.com/-/opensearch/extensions/eo/1.0/',
      'geo': 'http://a9.com/-/opensearch/extensions/geo/1.0/',
      'safe': 'http://www.esa.int/safe/sentinel/1.1',
      'time': 'http://a9.com/-/opensearch/extensions/time/1.0/',
      'param': 'http://a9.com/-/spec/opensearch/extensions/parameters/1.0/',
      'sru': 'http://a9.com/-/opensearch/extensions/sru/2.0/'
     }

retries = Retry(
    total=3,
    backoff_factor=4,
    status_forcelist=[500],
    allowed_methods=["GET"]
)

# https://opensearch-test.ceda.ac.uk
# https://archive.opensearch.ceda.ac.uk

#opensearch_url = 'https://archive-opensearch-main.rancher2.130.246.130.221.nip.io'#'https://opensearch-test.ceda.ac.uk'
opensearch_url = 'https://archive.opensearch.ceda.ac.uk'

maxRecordsRetrieved = 500
maximumRecords = 500

description_url = opensearch_url + '/opensearch/description.xml'

def _sizeof_fmt(num, suffix='B'):
    for unit in ['','K','M','G','T','P','E','Z']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Y', suffix)

def _fetch_url(_url, requestsSession=None):
    result = None
    response = None
    error = False
    if requestsSession is None:
        opensearch_adapter = HTTPAdapter(max_retries=retries)
        with requests.Session() as session:
            session.mount(opensearch_url, opensearch_adapter)
            session.mount(opensearch_url, opensearch_adapter)
            try:
                response = session.get(_url)#, verify=False)
            except ConnectionError as ce:
                print(ce)
                error = True
                pass
            except RetryError as re:
                print(re)
                error = True
                pass
    else:
        try:
            response = requestsSession.get(_url)#, verify=False)
        except ConnectionError as ce:
            print(ce)
            error = True
            pass
        except RetryError as re:
            print(re)
            error = True
            pass
        
    if response is not None:
        if response.status_code == 200:
            result = response.content
        else:
            print("Response status: " + str(response.status_code))
            result = None
            error = True
    else:
        print("Response to '" + str(_url)  + "' undefined")
        result = None
        error = True

    return (result, error)


def _feature_file_collections(feature_json):
    bytes = 0
    for (k, v) in enumerate(feature_json['features']):
        bytes = bytes + v['properties']['filesize']

    return bytes


def _parse_file_collections(feature, id):

    feature_template_url = opensearch_url + '/opensearch/request?parentIdentifier={{IDENTIFIER}}{{EXTRA_ID}}&httpAccept=application/geo%2Bjson&maximumRecords='
    total_bytes = 0

    for (index, aggregate) in enumerate(feature['aggregations']):
        extra_id = "" if len(aggregate['id']) == 0 else "&drsId=" + aggregate['id']
        url = feature_template_url.replace('{{IDENTIFIER}}', id).replace('{{EXTRA_ID}}', extra_id)

        num_features = 1
        feature_url = url + str(num_features)
        print("Feature: ", str(feature_url))
        total_features = 0
        aggregate_bytes = 0

        (feature_rsp, status_code) = _fetch_url(feature_url)

        if feature_rsp is not None:
            opensearch_adapter = HTTPAdapter(max_retries=retries)
            with requests.Session() as session:
                session.mount(opensearch_url, opensearch_adapter)
                session.mount(opensearch_url, opensearch_adapter)
                feature_json = JSON.loads(feature_rsp)
                features_processed = 0
                start_page = 1
                total_features = feature_json['totalResults']
                print("totalResults: ", str(total_features))
                num_features = total_features if total_features <= maximumRecords else maximumRecords
                if num_features > 0:
                    while features_processed < total_features and start_page <= 20:
                        feature_url = url + str(maximumRecords if total_features > maximumRecords else total_features) + '&startPage=' + str(start_page)
                        print("Feature URL: " + str(feature_url))
                        (feature_rsp, status_code) = _fetch_url(feature_url, session)
                        if feature_rsp is not None:
                            feature_json = JSON.loads(feature_rsp)
                            bytes = _feature_file_collections(feature_json)
                            if aggregate['type'] == 'all':
                                total_bytes = total_bytes + bytes
                            aggregate_bytes = aggregate_bytes + bytes
                            features_processed = features_processed + num_features
                            start_page = start_page + 1
                            num_features = (total_features - features_processed) if (total_features - features_processed) <= maximumRecords else maximumRecords
                        else:
                            features_processed = total_features

                if total_features > 0:
                    aggregate['links']['Files'] = {'href': url, 'size': total_features, 'bytes': _sizeof_fmt(aggregate_bytes)}
                    print("Bytes: ", str(aggregate['links']['Files']['bytes']))

        # We need to get the fileFormats available for refining search
        if 'description_url' in aggregate:
            fileFormats = _parse_description_xml_for_fileFormat(aggregate['description_url'])
            if len(fileFormats) > 1:
                aggregate['fileFormats'] = _parse_description_xml_for_fileFormat(aggregate['description_url'])
            dateRange = _parse_description_xml_for_StartEndDates(aggregate['description_url'])
            if dateRange:
                aggregate['dateRange'] = dateRange

    return total_bytes


def _parse_describeby(descriptions, project, id, describedby_url, extra_properties):
    
    print("Project '" + project + ":" + id + "' URL: " + describedby_url)
    if project not in descriptions:
        descriptions[project] = {}
        descriptions[project]['ecv'] = {}
        descriptions[project]['ecv']['catalogue_bytes'] = 0
        descriptions[project]['feature_collection'] = []
    description = descriptions[project]['feature_collection']
    feature_collection = {'id': id}
    (describedbyXML, status_code) = _fetch_url(describedby_url)

    mydoc  = minidom.parseString(describedbyXML)

    # There is only one 'TimePeiod'
    timePeriod = mydoc.getElementsByTagName('gml:TimePeriod')
    for time in timePeriod:
        start = time.getElementsByTagName('gml:beginPosition')
        start_date = (start[0].firstChild.nodeValue).split('T')[0]
        end = time.getElementsByTagName('gml:endPosition')
        end_date = (end[0].firstChild.nodeValue).split('T')[0]
        feature_collection['start'] = start_date
        feature_collection['end'] = end_date

        if 'min_date' not in descriptions[project]['ecv']:
            descriptions[project]['ecv']['min_date'] = start_date
        else:
            d1 = start_date.split('-')
            d2 = descriptions[project]['ecv']['min_date'].split('-')
            if date(int(d1[0]), int(d1[1]), int(d1[2])) < date(int(d2[0]), int(d2[1]), int(d2[2])):
                descriptions[project]['ecv']['min_date'] = start_date

        if 'max_date' not in descriptions[project]['ecv']:
            descriptions[project]['ecv']['max_date'] = end_date
        else:
            d1 = end_date.split('-')
            d2 = descriptions[project]['ecv']['max_date'].split('-')
            if date(int(d1[0]), int(d1[1]), int(d1[2])) > date(int(d2[0]), int(d2[1]), int(d2[2])):
                descriptions[project]['ecv']['max_date'] = end_date

    # There is only one 'abstract'
    abstracts = mydoc.getElementsByTagName('gmd:abstract')
    for abstract in abstracts:
        abstract_tmp = abstract.getElementsByTagName('gco:CharacterString')
        abstract_text = (abstract_tmp[0].firstChild.nodeValue)
        feature_collection['abstract'] = str(abstract_text)

    # There is only one 'dataQualityInfo'
    dataQualityInfos = mydoc.getElementsByTagName('gmd:statement')
    for dataQualityInfo in dataQualityInfos:
        dataQualityInfo_tmp = dataQualityInfo.getElementsByTagName('gco:CharacterString')
        dataQualityInfo_text = (dataQualityInfo_tmp[0].firstChild.nodeValue)
        feature_collection['eqc'] = dataQualityInfo_text

    # There is only one 'gmd:citation' which is the title of this feature
    citation = mydoc.getElementsByTagName('gmd:citation')
    title = citation[0].getElementsByTagName('gmd:title')
    title_tmp = title[0].getElementsByTagName('gco:CharacterString')
    title_text = (title_tmp[0].firstChild.nodeValue)
    feature_collection['title'] = title_text
    p = re.split(': +', title_text)
    feature_collection['feature_title'] = p[-1]

    # There is only one 'CI_OnlineResource'
    onlineResources = mydoc.getElementsByTagName('gmd:CI_OnlineResource')
    for onlineResource in onlineResources:
        name = onlineResource.getElementsByTagName('gmd:name')
        name_elem = name[0].getElementsByTagName('gco:CharacterString')
        name_str = (name_elem[0].firstChild.nodeValue).lower()
        if name_str in ['dataset homepage', 'ceda data catalogue page']:
            url_elem = onlineResource.getElementsByTagName('gmd:URL')
            url = url_elem[0].firstChild.nodeValue
            feature_collection['url'] =  url
        elif ' cci website' in name_str:
            url_elem = onlineResource.getElementsByTagName('gmd:URL')
            url = url_elem[0].firstChild.nodeValue
            if 'https://climate.esa.int/projects/' in url:
                # descriptions[project]['ecv']['homepage'] = url
                print("unused HOMEPAGE: " + url)
        elif name_str == 'sample image':
            url_elem = onlineResource.getElementsByTagName('gmd:URL')
            url = url_elem[0].firstChild.nodeValue
            feature_collection['sample_image'] = url
        elif 'product user guide' in name_str:
            url_elem = onlineResource.getElementsByTagName('gmd:URL')
            url = url_elem[0].firstChild.nodeValue
            feature_collection['user_guide'] = url
        else:
            url_elem = onlineResource.getElementsByTagName('gmd:URL')
            url = url_elem[0].firstChild.nodeValue
            if 'https://climate.esa.int/projects/' in url:
                # descriptions[project]['ecv']['homepage'] = url
                print("unused HOMEPAGE: " + url)

    for key, val in extra_properties.items():
        feature_collection[key] = val

    descriptions[project]['ecv']['catalogue_bytes'] = \
        descriptions[project]['ecv']['catalogue_bytes'] + _parse_file_collections(feature_collection, id)

    description.append(feature_collection)


def _parse_description_xml_for_ecv():
    _url = description_url + '?parentIdentifier=cci'

    ecvs = {}

    try:
        (rsp, error) = _fetch_url(_url)
        if rsp is not None and error is False:

            root = ET.fromstring(rsp)

            facet_root = None
            for elem in root.findall('cci:Url', ns):
                if elem.attrib['rel'] == 'results' and elem.attrib['type'] == 'application/geo+json':
                    facet_root = elem

            if facet_root is not None:
                for elem in facet_root.findall('param:Parameter', ns):
                    if elem.attrib['name'] in ['ecv']:
                        for option in elem.findall('param:Option', ns):
                            label = option.attrib['label'].split(' (')[0]
                            print("Label: ", label)
                            if option.attrib['value'] == 'ICESHEETS':
                                ecvs[option.attrib['value']] = ['Antarctic Ice Sheet', 'Greenland Ice Sheet']
                            elif option.attrib['value'] == 'LC':
                                ecvs[option.attrib['value']] = ['Land Cover', 'High Resolution Land Cover']
                            else:
                                ecvs[option.attrib['value']] = [ label ]
        else:
            print("NOT FOUND: _parse_description_xml_for_ecv description_url: " + " : " + str(_url))
            print("NOT FOUND: _parse_description_xml_for_ecv rsp: " + str(rsp) + " : " + str(status_code))

    except Exception as e:
        print(e)
        print("When parsing _parse_description_xml_for_ecv '" + _url + "'")
        pass
                                                                    
    return ecvs


def _parse_description_xml_for_drsId(_url):
    drsIds = []

    try:
        (rsp, error) = _fetch_url(_url)
        if rsp is not None and error is False:

            root = ET.fromstring(rsp)

            facet_root = None
            for elem in root.findall('cci:Url', ns):
                if elem.attrib['rel'] == 'results' and elem.attrib['type'] == 'application/geo+json':
                    facet_root = elem

            if facet_root is not None:
                for elem in facet_root.findall('param:Parameter', ns):
                    if elem.attrib['name'] in ['drsId']:
                        for option in elem.findall('param:Option', ns):
                            drsIds.append(option.attrib['value'])
        else:
            print("NOT FOUND: _parse_description_xml_for_drsId description_url: " + " : " + str(_url))
            print("NOT FOUND: _parse_description_xml_for_drsId rsp: " + str(rsp) + " : " + str(status_code))

    except Exception as e:
        print(e)
        print("When parsing _parse_description_xml_for_StartEndDates '" + description_url + "'")
        pass
                                                                    
    return drsIds


def _parse_description_xml_for_StartEndDates(_url):

    startEndDates = {}

    try:
        (rsp, error) = _fetch_url(_url)
        if rsp is not None and error is False:
            root = ET.fromstring(rsp)

            facet_root = None
            for elem in root.findall('cci:Url', ns):
                if elem.attrib['rel'] == 'results' and elem.attrib['type'] == 'application/geo+json':
                    facet_root = elem

            if facet_root is not None:
                for elem in facet_root.findall('param:Parameter', ns):
                    if elem.attrib['name'] in ['startDate'] and 'minInclusive' in elem.attrib:
                        startEndDates['min_date'] = elem.attrib['minInclusive'].split('T')[0]
                    elif elem.attrib['name'] in ['endDate'] and 'maxInclusive' in elem.attrib:
                        startEndDates['max_date'] = elem.attrib['maxInclusive'].split('T')[0]
        else:
            print("NOT FOUND: _parse_description_xml_for_StartEndDates description_url: " + " : " + str(_url))
            print("NOT FOUND: _parse_description_xml_for_StartEndDates rsp: " + str(rsp) + " : " + str(status_code))

    except Exception as e:
        print(e)
        print("When parsing _parse_description_xml_for_StartEndDates '" + _url + "'")
        pass
                                                                    
    return startEndDates


def _parse_description_xml_for_fileFormat(_url):

    fileFormats = []

    try:
        (rsp, error) = _fetch_url(_url)

        if rsp is not None and error is False:
            root = ET.fromstring(rsp)

            facet_root = None
            for elem in root.findall('cci:Url', ns):
                if elem.attrib['rel'] == 'results' and elem.attrib['type'] == 'application/geo+json':
                    facet_root = elem

            if facet_root is not None:
                for elem in facet_root.findall('param:Parameter', ns):
                    if elem.attrib['name'] in ['fileFormat']:
                        for option in elem.findall('param:Option', ns):
                            fileFormats.append({'label': option.attrib['label'], 'value': option.attrib['value']})
        else:
            print("NOT FOUND: _parse_description_xml_for_fileFormat description_url: " + " : " + str(_url))
            print("NOT FOUND: _parse_description_xml_for_fileFormat rsp: " + str(rsp) + " : " + str(status_code))

    except Exception as e:
        print(e)
        print("When parsing _parse_description_xml_for_fileFormat '" + _url + "'")
        pass
                                                                    
    return fileFormats

facet_order = [
    "ecv",
    "dataType",
    "sensor",
    "platform",
    "processingLevel",
    "project",
    "frequency",
    "institute",
    "productString",
    "productVersion"
]


def _parse_facets_xml():

    url = description_url + '?parentIdentifier=cci'

    print('Getting CCI description.........................')
    descriptions = {}
    (rsp, status_code) = _fetch_url(url)
    print("CCI description: " + str(url))

    root = ET.fromstring(rsp)

    facet_config = {}
    facet_values = []
    ecv_labels = {}

    facet_root = None
    for elem in root.findall('cci:Url', ns):
        if elem.attrib['rel'] == 'results' and elem.attrib['type'] == 'application/geo+json':
            facet_root = elem

    if facet_root is not None:
        for elem in facet_root.findall('param:Parameter', ns):
            if elem.attrib['name'] != 'drsId':
                for option in elem.findall('param:Option', ns):
                    if elem.attrib['name'] not in facet_config:
                        facet_config[elem.attrib['name']] = []

                    label = option.attrib['label']
                    value = option.attrib['value']

                    if elem.attrib['name'] == 'ecv':
                        ecv_labels[value] = label.split(' (')[0]

                    obj = None
                    if value not in facet_values:
                        facet_values.append(value)
                        if value.find("'") == -1:
                            obj = { 'label': label,
                                    'value': value,
                                    'displayLabel': label }
                        else:
                            fake_value = value.replace("'", "_")
                            obj = { 'label': label,
                                    'value': fake_value,
                                    'real_value': value,
                                    'displayLabel': label }
                    else:
                        fake_value = '_' + value
                        facet_values.append(fake_value)
                        obj = { 'label': label,
                                'value': fake_value,
                                'real_value': value,
                                'displayLabel': label }

                    facet_config[elem.attrib['name']].append( obj )
        facet_config['facet_order'] = facet_order

        print("ECV labels: ", str(ecv_labels))

    return (facet_config, ecv_labels)

tmp_output_filename = 'tmp_cci_ecv_config.json'
output_filename = 'cci_ecv_config.json'
required_output_filepath = '../../vue/src/assets/config/'
output_filepath = '../config/'
tmp_output_fullpath = output_filepath + tmp_output_filename
output_fullpath = output_filepath + output_filename
required_output_fullpath = required_output_filepath + output_filename

if __name__ == '__main__':

    parser = argparse.ArgumentParser()
    parser.add_argument("type", help="'deb': debian | 'dev': development branch | 'local': local deployments")
    args = parser.parse_args()

    clusterType = args.type
    print("Cluster type: " + clusterType)
    if clusterType in [ 'deb', 'dev', 'local' ]:
        if clusterType != 'local':
            os.chdir('/opt/ccike/odpui/utils/python')
    else:
        print("Invalid type: '" + clusterType + "' should be either [ 'deb', 'dev', 'local' ]. Try -h option")
        exit(0)

    base_url = opensearch_url + '/opensearch/request?parentIdentifier=cci&httpAccept=application/geo%2Bjson&maximumRecords='
    url = base_url + '1'

    ecvs = _parse_description_xml_for_ecv()
    print("ECVs facets: " + str(ecvs))

    if os.path.exists(tmp_output_fullpath):
        os.remove(tmp_output_fullpath)
    with open(tmp_output_fullpath, 'a') as f:
        f.write('{')

    print('Getting number of records.........................')
    (rsp, status_code) = _fetch_url(url)
    json = JSON.loads(rsp)

    url = base_url + str(json['totalResults'])

    print("CCI describe URL: " + str(url))
    (rsp, status_code) = _fetch_url(url)
    json = JSON.loads(rsp)

    ecvTitleIds = {}
    c3s_coverage = {}
    for (opensearchEcvId, ecvTitles) in ecvs.items():
        print("Opensearch ECV " + str(opensearchEcvId ) + " -> " + str(ecvTitles ))
        descriptions = {}
        for feature in json['features']:
            project = feature['properties']['title'].split(' (')[0].replace('ESA ', '').replace('Climate Change Initiative', '').rstrip().lstrip()
            if project == 'Biomass':
                project = 'Above-Ground Biomass'
            if project in ecvTitles:
                if project == 'Above-Ground Biomass':
                    project = 'Biomass'
                if project in ['Ocean Colour']:
                    maximumRecords = 2000
                    # maximumRecords = maxRecordsRetrieved
                else:
                    maximumRecords = maxRecordsRetrieved

                # if project in ['Land Cover', 'High Resolution Land Cover']:
                if project not in ['EMPTY']:
                    id = feature['properties']['identifier']
                    ftp_url = feature['properties']['links']['related'][0]['href']
                    describedby_url = feature['properties']['links']['describedby'][0]['href']

                    has_search = False
                    feature_description_url = description_url + '?parentIdentifier=' + id
                    if 'search' in feature['properties']['links']:
                        has_search = True
                        feature_description_url = feature['properties']['links']['search'][0]['href']

                    relationships = []
                    if 'relationships' in feature:
                        for index, relationship in enumerate(feature['relationships']):
                            if project not in c3s_coverage:
                                c3s_coverage[project] = {'min_date': relationship['related_dataset_start_date'],
                                                         'max_date': relationship['related_dataset_end_date']}
                            else:
                                if datetime.strptime(relationship['related_dataset_start_date'], '%Y-%m-%d') < datetime.strptime(c3s_coverage[project]['min_date'], '%Y-%m-%d'):
                                    c3s_coverage[project]['min_date'] = relationship['related_dataset_start_date']

                                if datetime.strptime(relationship['related_dataset_end_date'], '%Y-%m-%d') > datetime.strptime(c3s_coverage[project]['max_date'], '%Y-%m-%d'):
                                    c3s_coverage[project]['max_date'] = relationship['related_dataset_end_date']

                            relationships.append(relationship)
 
                    aggregations = []
                    if 'aggregations' in feature['properties']:
                        aggregation = {}
                        aggregation['id'] = ''
                        aggregation['type'] = 'all'
                        aggregation['description_url'] = feature_description_url
                        aggregation['links'] = {}
                        aggregations.append(aggregation)

                        for (k, v) in enumerate(feature['properties']['aggregations']):
                            aggregation = {}
                            aggregation['id'] = v['id']
                            if k == 0:
                                aggregation['heading'] = 'Homogeneous time series'
                            aggregation['description_url'] = feature_description_url + '&drsId=' + aggregation['id']
                            aggregation['type'] = 'aggregate'
                            aggregation['links'] = {}
                            for (k1, v1) in enumerate(v['properties']['links']['described_by']):
                                if v1['title'] == "opendap" and 'href' in v1:
                                    v1['href'] = v1['href'] + '.html'
                                aggregation['links'][v1['title'].replace(' ', '_')] = v1['href'] if 'href' in v1 else v1
                                if v1['title'] == "wms":
                                    aggregation['links']['wms_visualisation'] = 'http://cci.esa.int/sites/default/searchui/PEEP/peep.html?server=' + aggregation['links']['wms']
                            for (k1, v1) in enumerate(v['properties']['links']['related']):
                                if v1['title'] == "opendap" and 'href' in v1:
                                    v1['href'] = v1['href'] + '.html'
                                aggregation['links'][v1['title'].replace(' ', '_')] = v1['href'] if 'href' in v1 else v1
                                if v1['title'] == "wms":
                                    aggregation['links']['wms_visualisation'] = 'http://cci.esa.int/sites/default/searchui/PEEP/peep.html?server=' + aggregation['links']['wms']

                            aggregations.append(aggregation)
                    else:
                        if has_search:
                            aggregation = {}
                            aggregation['id'] = ''
                            aggregation['type'] = 'all'
                            aggregation['description_url'] = feature_description_url
                            aggregation['links'] = {}
                            aggregations.append(aggregation)

                            drsIds = _parse_description_xml_for_drsId(feature_description_url)
                            if len(drsIds) > 0:
                                for drsId in drsIds:
                                    aggregation = {}
                                    aggregation['id'] = drsId
                                    aggregation['description_url'] = feature_description_url + '&drsId=' + aggregation['id']
                                    aggregation['type'] = 'aggregate'
                                    aggregation['links'] = {}
                                    aggregations.append(aggregation)
                    
                    if has_search:
                        _parse_describeby(descriptions, project, id, describedby_url, {'ftp': ftp_url, 'aggregations': aggregations, 'relationships': relationships})

                        descriptions[project]['ecv']['opensearch'] = opensearchEcvId
                        # Graeme descriptions[project]['ecv']['label'] = project
                        if opensearchEcvId not in ecvTitleIds:
                            ecvTitleIds[opensearchEcvId] = [ project ]
                        else:
                            if project not in ecvTitleIds[opensearchEcvId]:
                                ecvTitleIds[opensearchEcvId].append(project)
                    else:
                        print("Feature '" + project + "' '" + str(id) + "' does not have a 'search' property")


        # if project in descriptions and len(c3s_coverage['min_date']) > 0:
        #     descriptions[project]['ecv']['c3s_coverage'] = c3s_coverage

        # We now open 'projects.json' to amalgamate all definitions here into 'descriptions'
        projects_json = {}
        #with open('../config/projects.json') as f:
        #    projects_json = JSON.load(f)
        #    for (k, v) in projects_json.items():
        #        if k in descriptions:
        #            for (key, val) in v.items():
        #                if key not in descriptions[k]['ecv']:
        #                    print("Adding: '" + key + "' '" + val + "'") 
        #                    descriptions[k]['ecv'][key] = val

        for (k, v) in descriptions.items():
            if k in c3s_coverage:
                descriptions[k]['ecv']['c3s_coverage'] = c3s_coverage[k]

            descriptions[k]['ecv']['catalogue_bytes'] = _sizeof_fmt(descriptions[k]['ecv']['catalogue_bytes'])
            homepage = descriptions[k]['ecv'].get('homepage','')
            if len(homepage) > 0:
                parts = homepage.split("/")
                if parts[-1] == '/': # Bug Fix?
                    neg_index = -2
                else:
                    neg_index = -1
                descriptions[k]['ecv']['slug'] = k.lower().replace( ' ', '-')
            else:
                descriptions[k]['ecv']['slug'] = k.lower().replace( ' ', '-')
            filesize = os.path.getsize(tmp_output_fullpath)
            with open(tmp_output_fullpath, 'a') as f:
                if filesize > 1:
                    f.write(', ')
                f.write('"' + k + '": ')
                JSON.dump(v, f, indent=4)

        descriptions = None
    
    print("ECV title ids" + str(ecvTitleIds ))
    (facets, ecv_labels) = _parse_facets_xml()
    if facets:
        filesize = os.path.getsize(tmp_output_fullpath)
        with open(tmp_output_fullpath, 'a') as f:
            if filesize > 1:
                f.write(', ')
            f.write('"facet_config": ')
            JSON.dump(facets, f, indent=4)

    if ecv_labels:
        filesize = os.path.getsize(tmp_output_fullpath)
        with open(tmp_output_fullpath, 'a') as f:
            if filesize > 1:
                f.write(', ')
            f.write('"ecv_labels": ')
            JSON.dump(ecv_labels, f, indent=4)

    # Now write all the ecvTitleIds to speed up the search
    filesize = os.path.getsize(tmp_output_fullpath)
    with open(tmp_output_fullpath, 'a') as f:
        if filesize > 1:
            f.write(', ')
        f.write('"ecv_title_ids": ')
        JSON.dump(ecvTitleIds, f, indent=4)

    # Now write all the available search results to speed up the search
    filesize = os.path.getsize(tmp_output_fullpath)
    with open(tmp_output_fullpath, 'a') as f:
        if filesize > 1:
            f.write(', ')
        f.write('"full_search_results": ')
        JSON.dump(json, f, indent=4)
        
    with open(tmp_output_fullpath, 'a') as f:
        f.write('}')

    # Now we try and read the file to ensure that json is valid
    #
    isValid = False
    with open(tmp_output_fullpath) as f:
        try:
            json_config = JSON.load(f)
            isValid = True
        except ValueError as e:
            print('invalid json: %s' % e)
            pass

    if isValid:
        shutil.move(tmp_output_fullpath, output_fullpath)

        #if clusterType == 'local':
            #shutil.copyfile(output_fullpath, required_output_fullpath)

    exit(0)