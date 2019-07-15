import xml.etree.ElementTree as ET
from urllib.request import urlopen
from datetime import datetime
from pymongo import MongoClient
from bs4 import BeautifulSoup
import json
import glob
from io import open
import re
import sys, time
import Crawl_Records

client = MongoClient('localhost:27017')
db = client.bvs
collection_all = db.training_collection_Ankush
collection_None_Indexed_t1 = db.training_collection_None_Indexed_t1
collection_None_Indexed_t2 = db.training_collection_None_Indexed_t2

collection_Update_info = db.training_collection_Update_info
errors = db.errors_training_Ankush

docs = collection_all.find({},{"_id":1})
print(type(docs))
for doc in docs:
    print(doc['_id'])
    input()


"""alternate_id = 'ibc-ET3-1422'
base_url = 'http://pesquisa.bvsalud.org/portal/resource/pt/ibc-ET3-1422?lang=en'
url = base_url + alternate_id + '?lang=en'
content = urlopen(url)
bsObj = BeautifulSoup(content,'html') 
data_string = (bsObj.find(attrs = {'class' :'data'})).text
found_object = re.search(r"(?<=ID:).*",data_string)
id = found_object.group().strip()
print(id)
"""


