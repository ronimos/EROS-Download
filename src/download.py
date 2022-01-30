# -*- coding: utf-8 -*-
"""
Author: Ron Simenhois

This script download images and images data from the 
USGS/EROS Inventory Service Documentation (Machine-to-Machine API).
https://m2m.cr.usgs.gov/api/docs/json/

"""

import datetime
import json
import os
import sys
import time

import numpy as np
import requests
import tqdm
from pykml import parser

from config import (DATA_PATH, logger,
                    service_url, dataset_names,
                    username, password, kml_file)


def send_request(url, data, apiKey=None):
    """
    This function handles the API communication and returns errors. 
    Parameters
    ----------
    url : str
        API server url + requesrt command. 
        see here: https://m2m.cr.usgs.gov/api/docs/json/ for more details
    data : dict
        The payload for the API command. The payload can include location, time frame...
    apiKey : str, optional
        DESCRIPTION. The default is None.

    Returns
    -------
    TYPE
        The request result.
    """
    json_data = json.dumps(data)

    if apiKey is None:
        response = requests.post(url, json_data)
    else:
        headers = {'X-Auth-Token': apiKey}
        response = requests.post(url, json_data, headers=headers)

    try:
        http_status_code = response.status_code
        if response is None:
            logger.warning('No output from service')
            sys.exit()
        output = json.loads(response.text)
        if output['errorCode'] is not None:
            logger.warning(output['errorCode'], '- ', output['errorMessage'])
            sys.exit()
        if http_status_code == 404:
            logger.warning('404 Not Found')
            sys.exit()
        elif http_status_code == 401:
            logger.warning('401 Unauthorized')
            sys.exit()
        elif http_status_code == 400:
            logger.warning('Error Code', http_status_code)
            sys.exit()
    except Exception as e:
        logger.warning(f'Failed to fetch remote {url} info ({e})')
        response.close()
        sys.exit()
    response.close()
    return output['data']


def download_to_file(url, file_name):
    """
This function download data from a URL and saves it as a zip file

    Parameters
    ----------
    url : str
        url to download the data.
    file_name : str
        file path to save the data.

    Returns
    -------
    str
        path to the saved data.
    """
    r = requests.head(url)
    size = r.headers['Content-Length']
    with requests.get(url, stream=True) as r:
        r.raise_for_status()
        with open(file_name, 'wb') as f:
            for chunk in r.iter_content(chunk_size=8192):
                f.write(chunk)

    logger.info(f'{file_name} was downloadwd (size={size}')
    return file_name


def get_area_rect_from_klm(kml_file):
    """
    This function gets a KML file with a polygon and returns the upper right 
    and lower left corners of the polygon's bounding box

    Parameters
    ----------
    kml_file : str
        path to the KML file.

    Returns
    -------
    lower_left : dict
        {'latitude' : left_lat,  'longitude' : lower_long}.
    upper_right : dict
        {'latitude' : right_lat, 'longitude' : upper_long}.
    """
    with open(kml_file) as f:
        doc = parser.parse(f).getroot()
    cords = []
    for e in doc.Document.findall('.//{http://www.opengis.net/kml/2.2}Placemark'):
        cords = e.Polygon.outerBoundaryIs.LinearRing['coordinates']

    cords = np.asarray([p.split(',') for p in str(cords).strip().split()]).reshape(-1, 3).astype(float)
    lower_long = cords[:, 0].min()
    upper_long = cords[:, 0].max()
    left_lat = cords[:, 1].min()
    right_lat = cords[:, 1].max()
    lower_left = {'latitude': left_lat, 'longitude': lower_long}
    upper_right = {'latitude': right_lat, 'longitude': upper_long}
    return lower_left, upper_right


def get_avaliabole_datasets(dataset_names,
                            spatial_filter,
                            temporal_filter,
                            apiKey):
    """
    This function gets all the available datasets and information for a given 
    list of datasets names.

    Parameters
    ----------
    dataset_names : list of str
        list of datasets names.
    spatial_filter : dict
        upper right and lower left corners of the area to monitor.
    temporal_filter : dict
        start and end dates to monitor.
    apiKey : str
        API key for this session.

    Returns
    -------
    datasets : dict
        dict with available datasets names as key and information for each 
        datasets as values.
    """
    datasets = {}
    for dataset in dataset_names:
        payload = {'datasetName': dataset,
                   'spatialFilter': spatial_filter,
                   'temporalFilter': temporal_filter}

        logger.info(f'Searching dataset name: {dataset}...')
        dataset_data = send_request(service_url + 'dataset-search', payload, apiKey)
        logger.info(f'Found {len(dataset_data)} datasets')
        if len(dataset_data):
            datasets[dataset] = dataset_data[0]
    return datasets


def get_sences_for_datasets(datasets,
                            spatial_filter,
                            acquisition_filter,
                            apiKey) -> dict:
    """
This function retrieves all the sence available for the area we monitor
    and the time frame for each one of the datasets, 

    Parameters
    ----------
    datasets : dict
        dict with datasets as keys and information on each dataset as values.
    spatial_filter : dict
        upper right and lower left coords of the are polygon's bounding box.
    acquisition_filter : dict
        start and end dates time frame.
    apiKey : str
        API key for this session.

    Returns
    -------
    sence_to_downloads : dict
        dict with keys as dataset names, and a list with dicts of entity id 
        and product id for each sence that is available to download.
    """
    sence_to_downloads = {}
    for dataset_name, dataset_info in datasets.items():
        dataset_name = dataset_info['datasetAlias']
        # Look for data from the last 2 weeks:    

        payload = {'datasetName': dataset_name,
                   'maxResults': 10,
                   'startingNumber': 1,
                   'sceneFilter': {'spatialFilter': spatial_filter,
                                   'acquisitionFilter': acquisition_filter}}

        # Now I need to run a scene search to find data to download
        logger.info(f'Searching scenes in dataset: {dataset_name}...')

        scenes = send_request(service_url + 'scene-search', payload, apiKey)

        # Did we find anything?
        if scenes['recordsReturned'] > 0:
            # Aggregate a list of scene ids
            scene_ids = [result['entityId'] for result in scenes['results']]

            # Find the download options for these scenes
            # NOTE :: Remember the scene list cannot exceed 50,000 items!
            payload = {'datasetName': dataset_name, 'entityIds': scene_ids}

            download_options = send_request(service_url + 'download-options', payload, apiKey)
            downloads = [{'entityId': product['entityId'], 'productId': product['id']} for product in download_options \
                         if product['available']]
            if downloads:
                sence_to_downloads[dataset_name] = downloads
        else:
            logger.warning(f'Search found no results for {dataset_info["collectionName"]}.\n')

    return sence_to_downloads


def get_download_urls(sence_to_download, apiKey) -> dict:
    """
    This function inserts download requests to the download queue and retrieves
    the url for download as soon as they are ready with data to download

    Parameters
    ----------
    sence_to_download : dict
        dict with a key for every dataset. The dict values are lists of dicts
        with entity id and product id for every sence we want to download.
    apiKey : str
        API key for this session.

    Returns
    -------
    ready_downloads_info : dict
        dict with download id as keys. The dict value is a dict with entity id 
        and download URL
    """
    ready_downloads_info = {}
    for dataset_name, downloads in sence_to_download.items():
        requested_downloads_count = len(downloads)
        # set a label for the download request
        label = 'download-sample'
        payload = {'downloads': downloads,
                   'label': label}
        # Insert the requested downloads into the download queue and get the available download URLs.
        request_results = send_request(service_url + 'download-request', payload, apiKey)
        # PreparingDownloads has a valid link that can be used but data may not be immediately available
        # Call the download-retrieve method to get download that is available for immediate download
        payload = {'label': label}
        ready_downloads = send_request(service_url + 'download-retrieve', payload, apiKey)
        # Keep record of all the ready to download data
        for download in ready_downloads['available']:
            info = {'entityId': download['entityId'],
                    'url': download['url']}
            ready_downloads_info[download['downloadId']] = info

        # Didn't get all of the reuested downloads, wait for 30 sed. and call the download-retrieve method again.
        while len(ready_downloads_info) < requested_downloads_count:
            preparingDownloads = requested_downloads_count - len(ready_downloads_info)
            logger.info(f'{preparingDownloads}, downloads are not available. Waiting for 30 seconds.')
            time.sleep(30)
            ready_downloads = send_request(service_url + 'download-retrieve', payload, apiKey)
            for download in ready_downloads['available']:
                if download['downloadId'] not in ready_downloads_info:
                    info = {'entityId': download['entityId'],
                            'url': download['url']}
                    ready_downloads_info[download['downloadId']] = info
        logger.info(f'All {requested_downloads_count} downloads from {dataset_name} are available to download.')

    return ready_downloads_info


if __name__ == '__main__':

    logger.info(f'Logging into {service_url}')
    payload = {'username': username, 'password': password}
    apiKey = send_request(service_url + 'login', payload)

    lower_left, upper_right = get_area_rect_from_klm(kml_file)

    start = (datetime.datetime.now() - datetime.timedelta(days=14)).strftime('%Y-%m-%d')
    end = datetime.datetime.now().strftime('%Y-%m-%d')

    spatial_filter = {'filterType': 'mbr',
                      'lowerLeft': lower_left,
                      'upperRight': upper_right}

    temporal_filter = {'start': start, 'end': end}

    datasets = get_avaliabole_datasets(dataset_names,
                                       spatial_filter,
                                       temporal_filter,
                                       apiKey)

    sence_to_download = get_sences_for_datasets(datasets,
                                                spatial_filter,
                                                temporal_filter,
                                                apiKey)

    ready_downloads_info = get_download_urls(sence_to_download,
                                             apiKey)

    # Download new images:
    existing_entities = os.listdir(DATA_PATH)
    for download in tqdm.tqdm(ready_downloads_info.values()):
        file_name = download['entityId'] + '.zip'
        if file_name not in existing_entities:
            url = download['url']
            saved_path = download_to_file(url, os.path.join(DATA_PATH, file_name))

    # Logout so the API Key cannot be used anymore
    endpoint = 'logout'
    if send_request(service_url + endpoint, None, apiKey) is None:
        logger.info('Logged Out\n\n')
    else:
        logger.warning('Logout Failed\n\n')
