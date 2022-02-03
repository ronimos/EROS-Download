import json
import os
import sys
import time

import requests
from dotenv import load_dotenv

from config import (service_url, dataset_names, kml_file)
from download import Download


class DownloadEORS(Download):

    def __init__(self, date_range: int = 14):
        super().__init__(date_range, 'EROS')
        load_dotenv()
        self.service_url = service_url
        self.username = os.getenv('EROS_user')
        self.password = os.getenv('EROS_password')
        self.service_url = service_url
        self.logger.info(f'Logging into {service_url}')
        payload = {'username': self.username, 'password': self.password}
        self.api_key = self.send_request('login', payload)
        lower_left, upper_right = self.get_area_rect_from_klm(kml_file)
        self.spatial_filter = {'filterType': 'mbr',
                               'lowerLeft': lower_left,
                               'upperRight': upper_right}
        self.temporal_filter = dict(start=self.start, end=self.end)
        self.dataset_names = dataset_names

    def send_request(self,
                     request,
                     data,
                     api_key=None):
        """
        This function handles the comunication with the API server

        Parameters
        ----------
        request : str
            The request type from the database see here: https://m2m.cr.usgs.gov/api/docs/json/.
        data : dict
            payload - used as time and location filter.
        api_key : str, optional
            API key to comunicate with the API. The default is None.

        Returns
        -------
        dict or str (when loggin - API key)
            Requested data.

        """

        json_data = json.dumps(data)
        url = self.service_url + request
        if api_key is None:
            response = requests.post(url, json_data)
        else:
            headers = {'X-Auth-Token': api_key}
            response = requests.post(url, json_data, headers=headers)

        try:
            http_status_code = response.status_code
            if response is None:
                self.logger.warning('No output from service')
                sys.exit()
            output = json.loads(response.text)
            if output['errorCode'] is not None:
                self.logger.warning(output['errorCode'], 
                                    '- ', 
                                    output['errorMessage'])
                sys.exit()
            if http_status_code == 404:
                self.logger.warning('404 Not Found')
                sys.exit()
            elif http_status_code == 401:
                self.logger.warning('401 Unauthorized')
                sys.exit()
            elif http_status_code == 400:
                self.logger.warning('Error Code', http_status_code)
                sys.exit()
        except Exception as e:
            self.logger.warning(f'Failed to fetch remote {url} info ({e})')
            response.close()
            sys.exit()
        response.close()
        return output['data']

    def get_available_datasets(self) -> dict:
        """
        This function fetch the datasets with avaliabole data for time 
        and place. The time range and place are a class variables

        Returns
        -------
        dict
            Return a dict with information on each dataset.
            The dict keys are the dataset's name .

        """
        _datasets = {}
        for dataset in self.dataset_names:
            payload = {'datasetName': dataset,
                       'spatialFilter': self.spatial_filter,
                       'temporalFilter': self.temporal_filter}

            self.logger.info(f'Searching dataset name: {dataset}...')
            dataset_data = self.send_request('dataset-search', payload, self.api_key)
            self.logger.info(f'Found {len(dataset_data)} datasets')
            if len(dataset_data):
                _datasets[dataset] = dataset_data[0]
        return _datasets

    def get_scenes_for_datasets(self,
                                _datasets) -> dict:
        """
        Fetch a dict of all the avaliabole sinces (images) in each dataset 
        for a give time amd place. The time and place are class variaboles

        Parameters
        ----------
        _datasets : dict
            dict with information on the dataset to explore. the _dataset
            dict keys are thr datasets names.

        Returns
        -------
        dict
            dict with keys - dataset names
                      values - a list of entityId and productId dict keys for 
                      all the avaliabole scines for each dataset 

        """
        scenes_to_downloads = {}
        for dataset_name, dataset_info in _datasets.items():
            dataset_name = dataset_info['datasetAlias']
            # Look for data from the last 2 weeks:

            payload = {'datasetName': dataset_name,
                       'maxResults': 10,
                       'startingNumber': 1,
                       'sceneFilter': {'spatialFilter': self.spatial_filter,
                                       'acquisitionFilter': self.temporal_filter}}

            # Now I need to run a scene search to find data to download
            self.logger.info(f'Searching scenes in dataset: {dataset_name}...')

            scenes = self.send_request('scene-search', payload, self.api_key)

            if scenes['recordsReturned'] <= 0:
                self.logger.warning(f'Search found no results for {dataset_info["collectionName"]}.\n')
            # Did we find anything?
            else:
                # Aggregate a list of scene ids
                scene_ids = [result['entityId'] for result in scenes['results']]

                # Find the download options for these scenes
                # NOTE :: Remember the scene list cannot exceed 50,000 items!
                payload = {'datasetName': dataset_name, 'entityIds': scene_ids}

                download_options = self.send_request('download-options', payload, self.api_key)
                downloads = [{'entityId': product['entityId'], 'productId': product['id']} for product in
                             download_options \
                             if product['available']]
                if downloads:
                    scenes_to_downloads[dataset_name] = downloads

        return scenes_to_downloads

    def get_download_urls(self,
                          scenes_to_download) -> dict:
        """
        This function fetch the download url for each sence

        Parameters
        ----------
        scenes_to_download : dict
            dict with keys - dataset names
                      values - a list of entityId and productId dict keys for 
                      all the avaliabole scines for each dataset 

        Returns
        -------
        dict
            dict with downloadId as keys and download url and entityId for each 
            avaliabole sence.

        """
        ready_downloads_info = {}
        for dataset_name, downloads in scenes_to_download.items():
            requested_downloads_count = len(downloads)
            # set a label for the download request
            label = 'download-sample'
            payload = {'downloads': downloads,
                       'label': label}
            # Insert the requested downloads into the download queue and get the available download URLs.
            request_results = self.send_request('download-request', payload, self.api_key)
            # PreparingDownloads has a valid link that can be used but data may not be immediately available
            # Call the download-retrieve method to get download that is available for immediate download
            payload = {'label': label}
            ready_downloads = self.send_request('download-retrieve', payload, self.api_key)
            # Keep record of all the ready to download data
            for download in ready_downloads['available']:
                info = {'entityId': download['entityId'],
                        'url': download['url']}
                ready_downloads_info[download['downloadId']] = info

            # Didn't get all the requested downloads, wait for 30 sed. and call the download-retrieve method again.
            while len(ready_downloads_info) < requested_downloads_count:
                preparing_downloads = requested_downloads_count - len(ready_downloads_info)
                self.logger.info(f'{preparing_downloads}, downloads are not available. Waiting for 30 seconds.')
                time.sleep(30)
                ready_downloads = self.send_request('download-retrieve', payload, self.api_key)
                for download in ready_downloads['available']:
                    if download['downloadId'] not in ready_downloads_info:
                        info = {'entityId': download['entityId'],
                                'url': download['url']}
                        ready_downloads_info[download['downloadId']] = info
            self.logger.info(
                f'All {requested_downloads_count} downloads from {dataset_name} are available to download.')

        return ready_downloads_info

    def download_all_to_files(self,
                              download_urls):
        """
        This function loop through the download URLs and call the
        download_to_file function

        Parameters
        ----------
        download_urls : dict
            dict with downloadId as keys and download url and entityId for each 
            avaliabole sence.

        Returns
        -------
        None.

        """
        file_name: str
        for download_info in download_urls.values():
            url = download_info['url']
            file_name = download_info['entityId'] + ".zip"
            _ = self.download_to_file(url, file_name)


if __name__ == '__main__':
    dl = DownloadEORS(date_range=5514)
    datasets = dl.get_available_datasets()
    scenes_to_download = dl.get_scenes_for_datasets(datasets)
    download_urls = dl.get_download_urls(scenes_to_download)
    dl.download_all_to_files(download_urls)
    # Logout so the API Key cannot be used anymore
    endpoint = 'logout'
    if dl.send_request(service_url + endpoint, {}, dl.api_key) is None:
        dl.logger.info('Logged Out\n\n')
    else:
        dl.logger.warning('Logout Failed\n\n')

