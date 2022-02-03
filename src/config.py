# -*- coding: utf-8 -*-
"""
Created on Sat Jan 29 16:39:01 2022

@author: Ron Simenhois
"""
import logging
import os


DATA_PATH = '../data'
LOG_PATH = '../LOGS'
service_url: str = 'https://m2m.cr.usgs.gov/api/api/json/stable/'
kml_file = 'AvalancheDetectionArea.kml'
dataset_names = ['WORLDVIEW-1', 'WORLDVIEW-2', 'WORLDVIEW-3']
