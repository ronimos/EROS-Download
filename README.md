# EROS-Download
This module downloads data from EROS Earth Explorer. It uses the m2m API (https://m2m.cr.usgs.gov/api/docs/json/) to search for data for a given location and date range. 
To use this module, register at Earth Explorer and request the m2m API (https://ers.cr.usgs.gov/profile/access).
After getting an API username and password, create a .env file with:
EROS_user=your user name
EROS_password=your password
And save it in the src folder. 
This module's default datasets are WORLDVIEW-1, WORLDVIEW-2, and WORLDVIEW-3
To use different datasets, update the cfng file in the src folder.

