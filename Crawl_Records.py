""" Program INFO

    - **Requirement BeautifulSoup**::

            linux:  $ sudo apt-get install python-bs4
            Macs:   $ sudo easy_install pip
            pip:    $ pip install beautifulSoup
"""

from bs4 import BeautifulSoup
from urllib.request import urlopen, urlretrieve
import time
import math 
import os ,sys,shutil
from datetime import datetime
    
def make_base_url(doc_type):
    """
    *With the type of Pesquisa articles it returns a base url, folder to save, and data name, depending of type received by it.*

        :param doc_type: Receives documents type (ibecs, lilacs, none_indexed_ibecs, none_indexed_lilacs, all, all_none_indexed).
        :type doc_type: string
        :returns: data_name, base_url, folder_to_save
        :rtype: string, string, string
    """

    if doc_type == "ibecs":
        base_url = 'http://pesquisa.bvsalud.org/portal/?output=xml&lang=en&sort=YEAR_DESC&format=abstract&filter%5Bdb%5D%5B%5D=IBECS&q=&index=tw&'
        folder_to_save = './crawled/'
        data_name = "IBECS"
    elif doc_type == "lilacs":
        base_url = 'http://pesquisa.bvsalud.org/portal/?output=xml&lang=en&sort=YEAR_DESC&format=abstract&filter%5Bdb%5D%5B%5D=LILACS&q=&index=tw&'
        folder_to_save = './crawled/'
        data_name = "LILACS"
    elif doc_type == "none_indexed_ibecs":
        base_url = 'http://pesquisa.bvsalud.org/portal/?u_filter%5B%5D=fulltext&u_filter%5B%5D=collection&u_filter%5B%5D=db&u_filter%5B%5D=mj_cluster&u_filter%5B%5D=type_of_study&u_filter%5B%5D=clinical_aspect&u_filter%5B%5D=limit&u_filter%5B%5D=pais_assunto&u_filter%5B%5D=la&u_filter%5B%5D=year_cluster&u_filter%5B%5D=type&u_filter%5B%5D=ta_cluster&u_filter%5B%5D=jd&u_filter%5B%5D=pais_afiliacao&fb=&output=xml&lang=en&sort=&format=summary&q=no_indexing%3A1&index=tw&where=&filter%5Bcollection%5D%5B%5D=06-national%2FES&'
        folder_to_save = './crawled_no_indexed/'
        data_name = "IBECS"
    elif doc_type == "none_indexed_lilacs":
        base_url = 'http://pesquisa.bvsalud.org/portal/?u_filter%5B%5D=fulltext&u_filter%5B%5D=collection&u_filter%5B%5D=db&u_filter%5B%5D=mj_cluster&u_filter%5B%5D=type_of_study&u_filter%5B%5D=clinical_aspect&u_filter%5B%5D=limit&u_filter%5B%5D=pais_assunto&u_filter%5B%5D=la&u_filter%5B%5D=year_cluster&u_filter%5B%5D=type&u_filter%5B%5D=ta_cluster&u_filter%5B%5D=jd&u_filter%5B%5D=pais_afiliacao&fb=&output=xml&lang=en&sort=&format=summary&q=no_indexing%3A1&index=tw&where=&filter%5Bdb%5D%5B%5D=LILACS&'
        folder_to_save = './crawled_no_indexed/'
        data_name = "LILACS"
    elif doc_type == "all":
        base_url = 'http://pesquisa.bvsalud.org/portal/?output=xml&lang=en&sort=YEAR_DESC&format=abstract&filter[db][]=LILACS&filter[db][]=IBECS&q=&index=tw&'
        folder_to_save = './crawled/'
        data_name = "IBECS_LILACS"
    elif doc_type == "all_none_indexed":
        base_url = 'http://pesquisa.bvsalud.org/portal/?u_filter[]=fulltext&u_filter[]=collection&u_filter[]=db&u_filter[]=mj_cluster&u_filter[]=type_of_study&u_filter[]=clinical_aspect&u_filter[]=limit&u_filter[]=pais_assunto&u_filter[]=la&u_filter[]=year_cluster&u_filter[]=type&u_filter[]=ta_cluster&u_filter[]=jd&u_filter[]=pais_afiliacao&fb=&output=xml&lang=en&sort=&format=summary&q=no_indexing:1&index=tw&where=&filter[db][]=LILACS&filter[db][]=IBECS&'
        folder_to_save = './crawled_no_indexed/'
        data_name = "IBECS_LILACS"
    else:
        print("Error: Wrong argument.")
        return False
    return data_name,base_url,folder_to_save


def make_url(base_url,start_record, per_page,page):
    """*Method to make a url, joining the base_url, start position of records, number of documents per page and page number.
All parameters are required.*

        :param base_url: A base url from where you want to download all contents.
        :type base_url: string
        :param start_record: Start position for records .Records will be start by this number.
        :type start_record: Int
        :param per_page: Number of total records by a page.
        :type per_page: Int
        :param page: Number of the page.
        :type page: Int  
        :returns: final_url
        :rtype: string
"""
    final_url = base_url+f'from={start_record}&count={per_page}&page={page}'
    return final_url

 
 #-----------------------------------------------------------------------#
 #-----------------------------------------------------------------------#

def count_records(url):
    """
    *The method extract the total number of records from the xml downloading bye the url received as parameter.*
        
        :param url: A url for downloading documents in a single xml file.
        :type url: string
        :returns: Number of total records.
        :rtype: Int

        .. note:: It's better to pass a *url* which conteins just 1 to 5 documents and it will count quickly.
                Otherwise it may take more time, because of the file's size. 
    """
    xml_content = urlopen(url)
    bsObj = BeautifulSoup(xml_content.read(),'lxml') 
    xml_data = bsObj.find(attrs = {'name' :'response'})
    total_records = int(xml_data["numfound"])
    return total_records

def save_all_xml(data_name,base_url,folder_to_save,total_records, per_page):
    """*This method download all *XML files* and save in a folder, the path of which is received by argument.*
    
        :param data_name:  Journal's name like (IBECS, LILACS, or IBECS_LILACS).
        :type data_name: string
        :param base_url:  A base url to make a new url with number of documents, start position and page number.
        :type base_url: string (Ex: 'http://pesquisa.bvsalud.org/portal/?output=xml&lang=en')
        :param folder_to_save: Path of a folder, where the all documents will be stored. If it doesn't exist it will be created.
        :type folder_to_save: string ('./crawled/')
        :param total_records: Number of all records.
        :type total_records: Int
        :param per_page: Number of records by a page.
        :type per_page: Int
        :returns: True. It returns always true.
        :rtype: Boolean

        .. note::   All records will be saved by the name created with data_name + date + file number + .xml./n
                    (Ex: IBECS_LILACS_17072019_pg_1.xml) 
    """

    print("----------Saving Records------------")

    if not os.path.isdir(folder_to_save):
        os.mkdir(folder_to_save)
    else:
        shutil.rmtree(folder_to_save)
        os.mkdir(folder_to_save)

    date = datetime.utcnow().strftime('%d%m%Y')
    num_pages = math.ceil(total_records/per_page)
    print("Total records: ", total_records)
    print("Total pages: ", num_pages)
    for i in range(num_pages):
        file_name = f"{data_name}_{date}_pg_{i+1}.xml"
        destine = os.path.join(folder_to_save,file_name)
        print(i+1)
        url = make_url(base_url,(500*i)+1,per_page,i+1)
        urlretrieve(url,destine)
        time.sleep(20)
    return True

def get_records(doc_type, mode = None):
    """*The method works as main, because all other methods are called from here. 
It just receives two argument, the first (doc_type) is requeued and must be the type of document (journal).
But the other one (mode) is for if you just want to count records, not to download and save.*

        :param doc_type: The type of documents (Ex: ibecs, lilacs, none_indexed_ibecs, none_indexed_lilacs, all, all_none_indexed).
        :type doc_type: string
        :param mode: If you just want to count records mode should be **"count"**.
        :type mode: string (Ex: "count").
        :returns: Number of records.
        :rtype: Int  
    """

    data_name,base_url,folder_to_save = make_base_url(doc_type)
    url = make_url(base_url,1,3,1) #url for getting total number of records
    total_records = count_records(url)
    if mode == "count":
        return total_records
    save_all_xml(data_name,base_url,folder_to_save,total_records, 500)
    return total_records    



"""
def print_arguments_error():
    print()
    print("1.     all:                   For all articles, IBECS and LILACS.\n")
    print("2.     ibecs:                 For articles IBECS.\n")
    print("3.     none_indexed_ibecs:      For articles IBECS none indexed.\n")
    print("4.     lilacs:    all_none_index         For articles LILACS.\n")
    print("5.     none_indexeall_none_indexilacs:     For articles LILACS none indexed.\n")
    print("6.     all_none_inall_none_indexed:      For articles LILACS and IBECS none indexed.\n")

    return False

def main(argument):    
    get_records(argument)

if __name__ == '__main__':
    try: 
        arguments = sys.argv
    except:
        print(f"Aspects 1 argument, but given 0.")
        print_arguments_error()

    if len(arguments) != 2:
        print(f"\n  Aspects 1 argument, b.. note::ut given {len(arguments)}.")
        print_arguments_error() 
    
    elif not arguments[1] in ["all","ibecs","lilacs","none_indexed_ibecs","none_indexed_lilacs","all_none_indexed"]:
        print(f"\nERROR: Argument doesn't match. Please make sure and try again.")  
        print_arguments_error()  
    else:
        main(arguments[1])
        """