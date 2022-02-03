import json
import os
import sys
import time

import requests
from dotenv import load_dotenv

from config import (service_url, dataset_names,
                    kml_file)
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

        Parameters
        ----------
        request
        data
        api_key

        Returns
        -------

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
                self.logger.warning(output['errorCode'], '- ', output['errorMessage'])
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

        Returns
        -------

        """
        datasets = {}
        for dataset in self.dataset_names:
            payload = {'datasetName': dataset,
                       'spatialFilter': self.spatial_filter,
                       'temporalFilter': self.temporal_filter}

            self.logger.info(f'Searching dataset name: {dataset}...')
            dataset_data = self.send_request('dataset-search', payload, self.api_key)
            self.logger.info(f'Found {len(dataset_data)} datasets')
            if len(dataset_data):
                datasets[dataset] = dataset_data[0]
        return datasets

    def get_scenes_for_datasets(self,
                                _datasets) -> dict:
        """

        Parameters
        ----------
        _datasets : dict

        Returns
        -------
        dict

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

        Parameters
        ----------
        scenes_to_download

        Returns
        -------

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

        Parameters
        ----------
        download_urls : dict

        Returns
        -------
        None
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

