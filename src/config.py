from configparser import ConfigParser

DATA_PATH = '../data'
LOG_PATH = '../LOGS'
service_url: str = 'https://m2m.cr.usgs.gov/api/api/json/stable/'
kml_file = 'AvalancheDetectionArea.kml'
config_file = 'cfng'
config = ConfigParser()
config.read(config_file)
try: 
    dataset_names = config['DATASETS']['dataset'].split()
except KeyError:
    dataset_names = ['WORLDVIEW-1', 'WORLDVIEW-2', 'WORLDVIEW-3']
