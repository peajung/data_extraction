# import libraries
from urllib.request import urlretrieve
from datetime import datetime
import pandas as pd
import bz2

# download flight plan zipfile and unzip
flight_plan_url = 'http://sfuelepcn1.thaiairways.co.th/etl/latest/qars.json.bz2'
urlretrieve(flight_plan_url,'qar.json.bz2')

# unzip file
zipfile = bz2.BZ2File('qar.json.bz2') # open the file
data = zipfile.read() # get the decompressed data
newfilepath = 'C:/Users/peaju/THAI AIRWAYS INTERNATIONAL PUBLIC CO.,LTD/DP - PC - Team 1 - Documents/PC - Team 1/2022/python/data_extraction/data/qar.json.bz2'[:-4] # assuming the filepath ends with .bz2
open(newfilepath, 'wb').write(data) # write a uncompressed file