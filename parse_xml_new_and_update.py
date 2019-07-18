"""**MongoDB**:

    .. warning:: MongoDb must be running. Otherwise it will give you an error. 
    .. note:: MongoDB  is initialized just by calling the module *parse_xml_new_and_update*.

    +----------------+-------------------------------------------+
    |   Data base    |                Collection                 |
    +================+===========================================+
    |     bvc        | training_collection_All                   |
    +                +-------------------------------------------+
    |                | training_collection_None_Indexed_t1       |
    +                +-------------------------------------------+
    |                | training_collection_None_Indexed_t2       |
    |                +-------------------------------------------+
    |                | training_collection_Update_info           |
    |                +-------------------------------------------+
    |                | errors_training                           |
    +----------------+-------------------------------------------+    


"""
import xml.etree.ElementTree as ET
from urllib.request import urlopen
from datetime import datetime
from pymongo import MongoClient
from bs4 import BeautifulSoup
import json
import glob
from io import open
import re
import os
import sys, time
import Crawl_Records

client = MongoClient('localhost:27017')
db = client.bvs
collection_all = db.training_collection_All
collection_None_Indexed_t1 = db['training_collection_None_Indexed_t1']
collection_None_Indexed_t2 = db['training_collection_None_Indexed_t2']
collection_Update_info = db.training_collection_Update_info
errors = db.errors_training

def save_to_mongo_updated_info(id,type,db):
    """This method is for saving the data like _id, type, db and date, in MongoDB *data base: bvc* and collection*. 

    :param id: Article document's id.
    :type id: string
    :param type: Type is new or update. It depends on article if it's new or just being updated.
    :type type: string (new or update)
    :param db: The name of article's data base (LILACS or IBECS)
    :type db: sting
    :returns: Nothing to return

    .. note:: The date will be saved automatically. It will be actual date obtained by ``datetime.utcnow()``.
"""
    date = datetime.utcnow()
    dictionary = dict({'_id':id,'type':type,'db':db,'parsing_date':date})     
    collection_Update_info.insert_one(dictionary)

def download_document(id):
    """This method is for downloading a single *article document in **xml** format*, by the **id of article**.

    :param id: Article's alternate id. If it's a normal id than it will return the same. 
    :type id: string (Ex: biblio-986217)
    :returns: url, xml (xml is a article document downloaded by id)
    :rtype: string, xml
"""
    base_url = 'http://pesquisa.bvsalud.org/portal/?output=xml&lang=en&from=&sort=&format=&count=&fb=&page=1&q=id%3A'
    url = base_url+id
    document = urlopen(url)
    time.sleep(5)
    return url,document


def find_id_by_alternate_id(alternate_id):
    """Method for obtained article's **id** by **alternate id**. It finds a document by document's *id* or *alternate_id*. 
    The logic of this method is use for find a **id** by **alternate id**.

    :param alternate_id: Article's alternate id. If it's a normal id than it will return the same. 
    :type alternate_id: string (Ex: biblio-986217)
    :returns: Article's id.
    :rtype: string (Ex: biblio-1001042)
"""

    base_url = 'http://pesquisa.bvsalud.org/portal/resource/en/'
    url = base_url + alternate_id
    content = urlopen(url)
    bsObj = BeautifulSoup(content,'html') 
    data_string = (bsObj.find(attrs = {'class' :'data'})).text  #Get the string whose class is data, for extracing the id.
    found_object = re.search(r"(?=ID:).*",data_string) # Regex For get id from the string
    doc_id = found_object.group().strip()
    time.sleep(10)
    return doc_id  

def xml_to_dictionary(document_xml):
    """The method converts a **xml document** to a **dictionary (json) format**. The method is just for article BVSalud **LILACS** or **IBECS**.
difference_between_entry_update_date.

    :param document_xml: A **single article document** in the **xml** format. 
    :type document_xml: xml
    :returns: A **single article document** in the **dictionary (json)** format.
    :rtype: dictionary/json
    """
    document_dict = dict()
    document_values = ['id','type','ur','au','afiliacao_autor','ti_es','ti_pt',
                       'ti_en','ti','fo','ta','is','la','cp','da','ab_pt','ab_en','ab_es','ab',
                       'entry_date','_version_','ct','mh','sh','cc','mark_ab_es','mark_ab_pt','mark_ab_en',
                       'db','alternate_id','update_date']

    file = open("xml_formated","w")
    file.write(document_xml.prettify())
    for code in document_values:
        try:
            value =document_xml.find(attrs= {'name':code}) # Find the value by code. If it doesn't exit than returns none
            if not value: # Check if the value is None
                document_dict[code] = value #Saving the value to the dictionary by code as key. In this case it must be None.      
            elif code == 'da':
                try:
                    document_dict[code] = (datetime.strptime(value.text[:6],'%Y%m%d'))
                except:
                    try:
                        document_dict[code] = (datetime.strptime(value.text[:4],'%Y'))
                    except:
                        document_dict[code] =value.text
            elif code in ['entry_date','update_date']:
                try:
                    document_dict[code] = (datetime.strptime(value.text,'%Y%m%d'))
                except:
                    try:
                        document_dict[code] = datetime.strptime(value.text[:6], '%Y%m%d')
                    except:
                        try:
                            document_dict[code] = datetime.strptime(value.text[:4], '%Y')
                        except (TypeError, AttributeError) as e:
                            errors.insert_one(dict(date_time = datetime.utcnow(),
                                doc_id = document_xml.find(attrs= {"name":"id"}).text,
                                type_error = 'Extract and converting date format single doc',
                                detail_error = code,
                                exception_str = str(e)))
                            document_dict[code] =value.text
            else:                
                children = value.findChildren() #Find if it has children. If it doesn't have than returns None.
                if len(children) > 0:    #If children exists, so get into the loop for appending all child
                    if code in ['type','ti_es','ti_pt','fo','cp','ab_pt','ab_en','ab_es','cc','ti_en', 'db', 'alternate_id']:
                        document_dict[code] = children[0].text
                    elif code in ['ur','au','afiliacao_autor','ti','ta','is','la','ab','ct','mh','sh']:
                        strings_list = []
                        for child in children:
                            strings_list.append(child.text)
                        document_dict[code] = strings_list              
                    elif code in ['mark_ab_es','mark_ab_pt','mark_ab_en']:
                        document_dict[code] = children[0].text             
                else:
                    document_dict[code] = value.text
        except (TypeError, AttributeError) as e:
            errors.insert_one(dict(date_time = datetime.utcnow(),
                                doc_id = document_xml.find(attrs= {"name":"id"}).text,
                                type_error = 'Extract field information from single doc',
                                detail_error = code,
                                exception_str = str(e)))
                                
    document_dict['_id'] = document_dict.pop('id')
    document_dict['parsing_entry_date'] = datetime.utcnow()
    return document_dict

def parse_file(path_to_file,mode=None):
    """The method parse a files and extract all documents one by one, 
    and after it converts **each document** by calling the function **xml_to_dictionary**.
    After **all the documents** one by one will be saved in the **data base MongoDB** as well all **ERROR**.

    :param path_to_file: The root of file to be parsed. 
    :type path_to_file: string (Ex: ./crawled/IBECS_LILACS_17072019_pg_1.xml).
    :param mode: The mode is condition if it receives **"compare"** will saved into a collection time 2. 
                Otherwise in the collection normal, maybe time 1. By default it's None.
    :type mode: string
    :returns: Nothing to return.

"""
    try:
        file = open(path_to_file)
        xml_content = file.read()
        bsObj = BeautifulSoup(xml_content,features='lxml')
        documents = bsObj.findAll("doc")
        documents_list = []
        try:
            for i, document_xml in enumerate(documents):
                document_dict = xml_to_dictionary(document_xml)
                document_dict['file'] = path_to_file

                if mode == "compare":
                    collection_None_Indexed_t2.insert_one(document_dict)
                else:
                    collection_all.insert_one(document_dict)
                    if document_dict['mh'] is None:
                        collection_None_Indexed_t1.insert_one(document_dict)
                
        except Exception as e:
            errors.insert_one(dict(date_time=datetime.utcnow(),
                            doc_id=document_xml.find(attrs= {"name":"id"}).text,
                            type_error='Extract one <doc> from single XML file',
                            detail_error=document_xml.find(attrs= {"name":"id"}).text,
                            exception_str=str(e)))                         
    except Exception as e:
        errors.insert_one(dict(date_time=datetime.utcnow(),
                        doc_id=document_xml.find(attrs= {"name":"id"}).text,
                        type_error='Extract multiple <doc> from single XML file',
                        detail_error=path_to_file,
                        exception_str=str(e)))

def document_compare():
    """This method is just for compare all document none indexed, *DATA BASE time1 by time2 and time2 by time1*.
    New will be inserted into the main DataBase and others will be updated by **id, mh, sh, alternat_id**
    unless in time2 documents have **mh** as **None** 
    
    .. note:: It receive nothing as parameter and nethier return. It just compare two collaction none indexed of time1 and time2.
    """

    all_ids_cursor_t2 =  collection_None_Indexed_t2.find({},{"_id":1})
    all_ids_t2 = []
    for item in all_ids_cursor_t2:
        all_ids_t2.append(item['_id'])

    all_ids_cursor_t1 =  collection_None_Indexed_t1.find({},{"_id":1})
    all_ids_t1 = []
    for item in all_ids_cursor_t1:
        all_ids_t1.append(item['_id'])

    i = 1
    for id_t2 in all_ids_t2:
        try:
            print("t2",i)
            i += 1
            document_t2 = collection_None_Indexed_t2.find_one({"_id":id_t2})
            document_t1 = collection_None_Indexed_t1.find_one({'_id':id_t2})
            if document_t1 is None:
                try:
                    print("Saving new Document",document_t2['_id'])
                    collection_all.insert_one(document_t2)
                    save_to_mongo_updated_info(id_t2,'new',document_t2['db'])                                                        
                except (TypeError, AttributeError) as e:
                    errors.insert_one(dict(date_time = datetime.utcnow(),
                        doc_id = id_t2,
                        type_error = 'Save new none indexed document into mongo',
                        detail_error = id_t2,
                        exception_str = str(e)))       
        except (TypeError, AttributeError) as e:
            errors.insert_one(dict(date_time = datetime.utcnow(),
                doc_id = id_t2,
                type_error = 'Save new none indexed document into mongo',
                detail_error = id_t2,
                exception_str = str(e))) 
    i = 1
    for id_t1 in all_ids_t1:
        print("t1",i)
        i += 1
        document_t1 = collection_None_Indexed_t1.find_one({'_id':id_t1})     
        document_t2 = collection_None_Indexed_t2.find_one({'_id':id_t1})
        if document_t2 is None:
            if document_t1['db'] == 'IBEBCS':
                doc_id_t1 = find_id_by_alternate_id(id_t1)                
            else:
                doc_id_t1 = id_t1

            url, xml= download_document(doc_id_t1)
            bsObj = BeautifulSoup(xml,features='lxml')
            document_xml = bsObj.find('doc')
            
            if document_xml is not None:
                try:
                    document_dict = xml_to_dictionary(document_xml)
                    print("Updating a document: ", id_t1)
                    collection_all.update_one({'_id': id_t1},
                                        {'$set':
                                            {'_id': document_dict['_id'],
                                            'alternate_id': document_dict['alternate_id'],
                                            'mh': document_dict['mh'],
                                            'sh': document_dict['sh'],
                                            'parsing_update_date': datetime.utcnow()
                                            }
                                        })
                    save_to_mongo_updated_info(document_dict['_id'],'update',document_dict['db'])
                except Exception as e:
                    errors.insert_one(dict(date_time=datetime.utcnow(),
                                            doc_id=document_dict['_id'],
                                            type_error='Update information from single <doc>',
                                            detail_error=url,
                                            exception_str=str(e)))  
                          
def change_collections_name_mongo():
    """It changes the name of a collaction if the target name is exist than it will delete that collaction.
(Ex: vs.training_collection_None_Indexed_t2  -> vs.training_collection_None_Indexed_t1). 

    :returns: Nothing to return     
    """
    try:
        client.admin.command("renameCollection", "bvs.training_collection_None_Indexed_t2", to= "bvs.training_collection_None_Indexed_t1",dropTarget=True)
    except:
        pass
    
def process_dir_t2(path_to_dir):
    """Method to get all file from a folder. All files one by one will be passed to the method **parse_file** with the condion **"compare"**
    
    :param path_to_dir: The root of the directory where all files in **xml** format are saved. 
    :type: string (Ex: ./crawled_no_indexed/)
    :returns: Nothing to return.

    .. seealso:: You should take a look at the method **parse file** with **mode "compare"**. 
                it would help you to handle better this method. 
    """  

    files = glob.glob(path_to_dir+'*.xml')
    for file in files:
        print("file",file)
        parse_file(file,"compare")

def process_dir_t1(path_to_dir):
    """Method to get all file from a folder. All files one by one will be passed to the method **parse_file** without any condition (None).
    
    :param path_to_dir: The root of the directory where all files **xml** format are saved. 
    :type: string (Ex: ./crawled/)
    :returns: Nothing to return.

    .. seealso:: You should take a look at the method **parse file** with **mode "compare"**. 
                it would help you to handle better this method. 
    """     
    print("..........")
    files = glob.glob(path_to_dir+'*.xml')
    for file in files:
        print("file",file)
        parse_file(file)

def main(arguments):
    """The method main is just for calling all other methods. It recives a argument, but not required. 
If it recives the argument **"first_time"** than it will download all documents and parse those to save in the MongoDB.
Otherwise it will just download to be comared with others already existing.

    :param argument: This a condition if the program is being excecuted for first time.
    :type: string
    :returns: Nothing to return

    .. note:: If the program is being executed first time, you must pass a argument **first_time**. Otherwise it doesn't need any.
            First time: *python parse_xml_new_and_update.py first_time*
            Otherwise: python parse_xml_new_and_update.py*

"""
    num_all_records = Crawl_Records.get_records("all", "count")
    num_none_indexed =  Crawl_Records.get_records("all_none_indexed", "count")

    if len(arguments) == 2:
        if arguments[1] == 'first_time':
            Crawl_Records.get_records("all")
            process_dir_t1("./crawled/")
        else:
            print("\nError: Wrong argument.\n")
            print("\tfirst_time: If you are doing it first time.")
            print("\t            Otherwise don't have to pass any argument.")
    elif len(arguments) == 1:
        Crawl_Records.get_records("none_indexed_ibecs")
        Crawl_Records.get_records("none_indexed_lilacs")
        change_collections_name_mongo()
        process_dir_t2('./crawled_no_indexed/')
        document_compare()

    else:
        print("\nError: Wrong argument.\n") 
        print("\tfirst_time: If you are doing it first time.\n\t\tOtherwise do not pass any argument")

if __name__ == '__main__':
    main(sys.argv)

