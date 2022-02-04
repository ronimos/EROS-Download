# -*- coding: utf-8 -*-
"""
Created on Fri Feb  4 10:45:50 2022

@author: Ron Simenhois

This example shows hoe to use the EROS download module to download data from 
Earth Explorer to a local folder
"""

from EROS_Download import DownloadEORS

if __name__ == '__main__':
    
    dl = DownloadEORS(date_range=14)
    # Look to see if there are datasets with new data from the last 14 days
    datasets = dl.get_available_datasets()
    if len(datasets) > 0:
        # There is new data...
        # Get the new scenes from these datasets
        scenes_to_download = dl.get_scenes_for_datasets(datasets)
        # Send request to the databasedownload queue and fetch download URLs
        download_urls = dl.get_download_urls(scenes_to_download)
        # Download the new data
        dl.download_all_to_files(download_urls)
    # Close the connection to the API
    dl.close_api()
