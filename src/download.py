import os
import numpy as np
import requests
from datetime import datetime, timedelta
from pykml import parser
import logging

from config import LOG_PATH, DATA_PATH


class Download:
    def __init__(self, date_range=14, log_mame=''):

        os.makedirs(LOG_PATH, exist_ok=True)
        logger = logging.getLogger(f'{log_mame}_download')
        fh = logging.FileHandler(os.path.join(LOG_PATH, logger.name))
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fh.setFormatter(formatter)
        fh.setLevel(logging.DEBUG)
        logger.addHandler(fh)
        self.logger = logger
        self.start = (datetime.now() - timedelta(days=date_range)).strftime('%Y-%m-%d')
        self.end = datetime.now().strftime('%Y-%m-%d')

    def send_request(self, request, data, apiKey=None):

        raise NotImplementedError("Subclasses should implement this!")

    def download_to_file(self, download_url, file_name) -> str:
        """
        This function download data from a URL and saves it as a zip file

        Parameters
        ----------
        download_url : str
            url to download the data.
        file_name : str
            file path to save the data.

        Returns
        -------
        str
            path to the saved data.
        """
        r = requests.head(download_url)
        size = r.headers['Content-Length']
        try:
            filename = r.headers['Content-Disposition']
            file_name = filename.replace('"', '').split('=')[1]
        except KeyError:
            pass
        file_name = os.path.join(DATA_PATH, file_name)
        if file_name not in os.listdir(DATA_PATH):
            with requests.get(download_url, stream=True) as r:
                r.raise_for_status()
                with open(file_name, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)

        self.logger.info(f'{file_name} is downloaded (size={size}')
        return file_name

    @ classmethod
    def get_area_rect_from_klm(cls, kml_file):
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




