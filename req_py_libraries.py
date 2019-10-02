from bs4 import BeautifulSoup
import re
from datetime import datetime, timedelta
import datetime as dt
from dateutil.relativedelta import relativedelta
import numpy as np
import csv
import os
import time
import json
import math
import xml.etree.ElementTree as ET
import xmltodict
import pandas as pd
import decimal
import requests
import time
import urllib.parse
import feedparser
import certifi
import urllib3



# dump data to json file
def dump_json(path,data):
    with open(path, 'w') as outfile:
        json.dump(data, outfile)


#load json file
def read_json(path):
    with open(path) as json_data:
        data = json.load(json_data)
        json_data.close()
    return data


