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
collection_None_Indexed_t1 = db.training_collection_None_Indexed_t1
collection_None_Indexed_t2 = db.training_collection_None_Indexed_t2

collection_Update_info = db.training_collection_Update_info
errors = db.errors_training_Ankush

def save_to_mongo_updated_info(id,type,db):
    date = datetime.utcnow()
    dictionary = dict({'_id':id,'type':type,'db':db,'parsing_date':date})     
    collection_Update_info.insert_one(dictionary)

def download_document(id):
    base_url = 'http://pesquisa.bvsalud.org/portal/?output=xml&lang=pt&from=&sort=&format=&count=&fb=&page=1&q=id%3A'
    url = base_url+id
    document = urlopen(url)
    time.sleep(5)
    return url,document


def find_id_by_alternate_id(alternate_id):
    base_url = 'http://pesquisa.bvsalud.org/portal/resource/en/'
    url = base_url + alternate_id
    content = urlopen(url)
    bsObj = BeautifulSoup(content,'html') 
    data_string = (bsObj.find(attrs = {'class' :'data'})).text
    found_object = re.search(r"(?=ID:).*",data_string)
    doc_id = found_object.group().strip()
    time.sleep(5)
    return doc_id  

def xml_to_dictionary(document_xml):
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
                    document_dict[code] = (datetime.strptime(value.text,'%Y%m'))
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
                        document_dict[code] = datetime.strptime(value.text[:6], '%Y%m')
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
    document_dict['parsing_date'] = datetime.utcnow()
    return document_dict

def parse_file(path_to_file,mode=None):
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

def process_dir_t2():
    folder_to_save = './crawled_no_indexed/' 
    #if os.path.isdir(folder_to_save):
    #    os.remove(folder_to_save)
    #Crawl_Records.get_records("none_index_ibecs")
    #Crawl_Records.get_records("none_index_lilacs")
    files = glob.glob(folder_to_save+'*.xml')
    for file in files:
        print("file",file)
        parse_file(file,"compare")

def document_compare():
    process_dir_t2()

    all_ids_cursor_t2 =  collection_None_Indexed_t2.find({},{"_id":1})
    all_ids_t2 = []
    for item in all_ids_cursor_t2:
        all_ids_t2.append(item['_id'])

    all_ids_cursor_t1 =  collection_None_Indexed_t1.find({},{"_id":1})
    all_ids_t1 = []
    for item in all_ids_cursor_t1:
        all_ids_t1.append(item['_id'])

    print("t1:",len(all_ids_t1))
    print("t2:",len(all_ids_t2))
    i = 1
    for id_t2 in all_ids_t2:
        print("t1",i)
        i += 1
        document_t2 = collection_None_Indexed_t2.find_one({"_id":id_t2})
        document_t1 = collection_None_Indexed_t1.find_one({'_id':id_t2})
        if document_t1 is None:
            try: 
                collection_None_Indexed_t1.insert_one(document_t2)
                collection_all.insert_one(document_t2)
                save_to_mongo_updated_info(id_t2,'new',document_t2['db'])                                                        
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
            if document_t1['db'] == 'LILACS':
                doc_id_t1 = id_t1
                doc_alternate_id = None
            else:
                doc_alternate_id = document_t1['alternate_id']
                doc_id_t1 = find_id_by_alternate_id(doc_alternate_id)

            url, xml= download_document(doc_id_t1)
            bsObj = BeautifulSoup(xml,features='lxml')
            document_xml = bsObj.find('doc')
            
            if document_xml is not None:
                try:
                    document_dict = xml_to_dictionary(document_xml)
                    collection_all.update_one({'_id': document_dict['_id']},
                                        {'alternate_id': doc_alternate_id},
                                        {'$set': {'mh': document_dict['mh'],
                                                    'sh': document_dict['sh'],
                                                    'parsing_date': datetime.utcnow()}
                                                    })
                    collection_None_Indexed_t1.delete_one({'_id': document_dict['_id']})
                    save_to_mongo_updated_info(document_dict['_id'],'update',document_dict['db'])
                except Exception as e:
                    errors.insert_one(dict(date_time=datetime.utcnow(),
                                            doc_id=document_dict['_id'],
                                            type_error='Update information from single <doc>',
                                            detail_error=url,
                                            exception_str=str(e)))                

def process_dir_t1(path_to_dir):
    """
    :param path_to_dir:
    :return:
    """
    Crawl_Records.get_records("all")
    print("..........")
    files = glob.glob(path_to_dir+'*.xml')
    for file in files:
        print("file",file)
        parse_file(file)

def main(arguments):
    if len(arguments) == 2:
        if arguments[1] == 'first_time':
            process_dir_t1("./crawled/")
        else:
            print("\nError: Wrong argument.\n")
            print("\tfirst_time: If you are doing it first time.")
            print("\t            Otherwise don't have to pass any argument.")
    elif len(arguments) == 1:
        document_compare()
    else:
        print("\nError: Wrong argument.\n") 
        print("\tfirst_time: If you are doing it first time.\n\t\tOtherwise do not pass any argument")

if __name__ == '__main__':
    main(sys.argv)


