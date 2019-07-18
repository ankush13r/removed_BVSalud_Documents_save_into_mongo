from datetime import datetime
from pymongo import MongoClient
from io import open
import csv, os


client = MongoClient('localhost:27017')
db = client.bvs
collection_all = db.training_collection_All

def select_records():

    date = datetime.utcnow().strftime('%Y%h%d') 
    folder_to_save = "./data/"
    if not os.path.isdir(folder_to_save):
        os.mkdir(folder_to_save)
    path_to_file = folder_to_save + date +'.csv'
    cursor_documents =  collection_all.find({})   
    documents_scv = []
    
    with open(path_to_file ,'w') as csv_file:
        fieldnames = ['Library', 'Journal', 'ID','Entry date','Update date','Difference','mh']
        writer = csv.DictWriter(csv_file, fieldnames=fieldnames)
        writer.writeheader()

        for document in cursor_documents:
            if document['mh'] is not None: 
                dictionary = dict()
                entry_date = document['entry_date']
                update_date = document['update_date']
                difference = (update_date - entry_date).days

                dictionary['Library'] = document['cc']
                try:
                    dictionary['Journal'] = document['ta'][0]
                except:
                    dictionary['Journal'] = document['fo']
                dictionary['ID'] = document['_id']
                dictionary['Entry date'] = entry_date
                dictionary['Update date'] = update_date
                dictionary['Difference'] = difference
                dictionary['mh'] = document['mh'][0]

                writer.writerow(dictionary)


select_records()


"""
db.training_collection_All.aggregate([
{$match:{mh:{$ne: null}}},
    {$project:{"Library": "$cc", 
        "Journal":{$cond:
            { if: 
                { $ne: [ "$ta", null]}, 
        then:
            {$arrayElemAt: [ "$ta", 0 ] }, 
        else:
            "$fo"}},
        _id :1,
        entry_date :1, 
        update_date :1,
        "Difference" : {$divide:[{$subtract: [ "$entry_date", "$update_date" ] },1000 * 60 * 60 * 24]}
        }
    },
    {$out: "collaction_scv"}

]);


sudo mongoexport --db bvs --collection training_collection_All --type=csv --fields Library,Journal,entry_date,update_date,Difference --out contacts.csv


"""
