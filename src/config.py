# -*- coding: utf-8 -*-
"""
Created on Sat Jan 29 16:39:01 2022

@author: Ron Simenhois
"""
import logging
import os

from dotenv import load_dotenv

DATA_PATH = '../data'
LOG_PATH = '../LOGS'
service_url = 'https://m2m.cr.usgs.gov/api/api/json/stable/'
kml_file = 'AvalancheDetectionArea.kml'
dataset_names = ['WORLDVIEW-1', 'WORLDVIEW-2', 'WORLDVIEW-3']

os.makedirs(DATA_PATH, exist_ok=True)
os.makedirs(LOG_PATH, exist_ok=True)
logger = logging.getLogger('EROS_download')
fh = logging.FileHandler(os.path.join(LOG_PATH, 'download.log'))
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
fh.setFormatter(formatter)
fh.setLevel(logging.DEBUG)
logger.addHandler(fh)

load_dotenv()
username = os.getenv('user')
password = os.getenv('password')
