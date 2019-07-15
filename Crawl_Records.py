"""
    Requirement: BeautifulSoup 
        linux:  $ sudo apt-get install python-bs4
        Macs:   $ sudo easy_install pip
        pip:    $ pip install beautifulSoup
"""

from bs4 import BeautifulSoup
from urllib.request import urlopen, urlretrieve
import time
import math 
import os ,sys
from datetime import datetime

    
def make_base_url(argument):
 
    if argument == "ibecs":
        base_url = 'http://pesquisa.bvsalud.org/portal/?output=xml&lang=en&sort=YEAR_DESC&format=abstract&filter%5Bdb%5D%5B%5D=IBECS&q=&index=tw&'
        folder_to_save = './crawled/'
        data_name = "IBECS"

    elif argument == "lilacs":
        base_url = 'http://pesquisa.bvsalud.org/portal/?output=xml&lang=en&sort=YEAR_DESC&format=abstract&filter%5Bdb%5D%5B%5D=LILACS&q=&index=tw&'
        folder_to_save = './crawled/'
        data_name = "LILACS"


    elif argument == "none_index_ibecs":
        base_url = 'http://pesquisa.bvsalud.org/portal/?u_filter%5B%5D=fulltext&u_filter%5B%5D=collection&u_filter%5B%5D=db&u_filter%5B%5D=mj_cluster&u_filter%5B%5D=type_of_study&u_filter%5B%5D=clinical_aspect&u_filter%5B%5D=limit&u_filter%5B%5D=pais_assunto&u_filter%5B%5D=la&u_filter%5B%5D=year_cluster&u_filter%5B%5D=type&u_filter%5B%5D=ta_cluster&u_filter%5B%5D=jd&u_filter%5B%5D=pais_afiliacao&fb=&output=xml&lang=en&sort=&format=summary&q=no_indexing%3A1&index=tw&where=&filter%5Bcollection%5D%5B%5D=06-national%2FES&'
        folder_to_save = './crawled_no_indexed/'
        data_name = "IBECS"
    

    elif argument == "none_index_lilacs":
        base_url = 'http://pesquisa.bvsalud.org/portal/?u_filter%5B%5D=fulltext&u_filter%5B%5D=collection&u_filter%5B%5D=db&u_filter%5B%5D=mj_cluster&u_filter%5B%5D=type_of_study&u_filter%5B%5D=clinical_aspect&u_filter%5B%5D=limit&u_filter%5B%5D=pais_assunto&u_filter%5B%5D=la&u_filter%5B%5D=year_cluster&u_filter%5B%5D=type&u_filter%5B%5D=ta_cluster&u_filter%5B%5D=jd&u_filter%5B%5D=pais_afiliacao&fb=&output=xml&lang=en&sort=&format=summary&q=no_indexing%3A1&index=tw&where=&filter%5Bdb%5D%5B%5D=LILACS&'
        folder_to_save = './crawled_no_indexed/'
        data_name = "LILACS"

    elif argument == "all":
        base_url = 'http://pesquisa.bvsalud.org/portal/?output=xml&lang=en&sort=YEAR_DESC&format=abstract&filter[db][]=LILACS&filter[db][]=IBECS&q=&index=tw&'
        folder_to_save = './crawled/'
        data_name = "IBECS_LILACS"


    print("Data name: ",data_name)
    print("Folder name: ",folder_to_save)
    print("url: ",base_url)
    return data_name,base_url,folder_to_save

def make_url(base_url,start_record, per_page,page):
    final_url = base_url+f'from={start_record}&count={per_page}&page={page}'
    return final_url

def print_arguments_error():
    print()
    print("1.     all:                   For all articles, IBECS and LILACS.\n")
    print("2.     ibecs:                 For articles IBECS.\n")
    print("3.     none_index_ibecs:      For articles IBECS none indexed.\n")
    print("4.     lilacs:                For articles LILACS.\n")
    print("5.     none_index_lilacs:     For articles LILACS none indexed.\n")
    return False
 
 #-----------------------------------------------------------------------#
 #-----------------------------------------------------------------------#

def count_records(url):
    print(url)
    xml_content = urlopen(url)
    bsObj = BeautifulSoup(xml_content.read(),'lxml') 
    xml_data = bsObj.find(attrs = {'name' :'response'})
    total_records = int(xml_data["numfound"])
    return total_records


def save_all_xml(data_name,base_url,folder_to_save,total_records, per_page):
    print("----------Saving Records------------")
    num_pages = math.ceil(total_records/per_page)
    if not os.path.isdir(folder_to_save):
        os.mkdir(folder_to_save)
    
    date = datetime.utcnow().strftime('%d%m%Y')

    for i in range(5):
        file_name = f"{data_name}_{date}_pg_{i+1}.xml"
        destine = os.path.join(folder_to_save,file_name)
        print(i+1)
        url = make_url(base_url,(500*i)+1,per_page,i+1)
        urlretrieve(url,destine)
        time.sleep(60)
    return True

def get_records(argument):
    data_name,base_url,folder_to_save = make_base_url(argument)
    url = make_url(base_url,1,5,1) #url for getting total number of records
    print()
    total_records = count_records(url)
    print("Total records:\n",total_records)
    save_all_xml(data_name,base_url,folder_to_save,total_records, 500)       



"""
def main(argument):    
    get_records(argument)

if __name__ == '__main__':
    try: 
        arguments = sys.argv
    except:
        print(f"Aspects 1 argument, but given 0.")
        print_arguments_error()

    if len(arguments) != 2:
        print(f"\n  Aspects 1 argument, but given {len(arguments)}.")
        print_arguments_error() 
    
    elif not arguments[1] in ["all","ibecs","lilacs","none_index_ibecs","none_index_lilacs."]:
        print(f"\nERROR: Argument doesn't match. Please make sure and try again.")  
        print_arguments_error()  
    else:
        main(arguments[1])
        """