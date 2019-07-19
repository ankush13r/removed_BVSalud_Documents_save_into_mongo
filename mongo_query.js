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
            "Difference" : {$divide:[{$subtract: [ "$update_date", "$entry_date" ] },1000 * 60 * 60 * 24]}
            }
        },
        {$out: "collaction_scv"}
    ]);

    // Are grouped by Library

 /*   db.training_collection_All.aggregate([
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
                "Difference" : {$divide:[{$subtract: [ "$update_date", "$entry_date" ] },1000 * 60 * 60 * 24]}
                }
            },
            {$group: {
                _id : "$Library",
                Days_Average : {$avg :"$Difference"}
                }
             },
             {$project:{"Library":"$_id", "Days_Average":1}},
             {$out: "collection_days_difference_avg"}
        ]);*/

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
            "Difference" : {$divide:[{$subtract: [ "$update_date", "$entry_date" ] },1000 * 60 * 60 * 24]}
            }
        },
        {$group: {
            _id : {"Library": "$Library", "Journal": "$Journal"},
            Days_Average : {$avg :"$Difference"}
            }
            },
            {$project:{"Library":"$_id.Library", "Journal":"$_id.Journal","Days_Average":1}},
            {$out: "collection_days_difference_avg"}
    ]);

// sudo mongoexport --db bvs --collection collaction_scv --type=csv --fields Difference,entry_date,update_date,Library,Journal,_id --out difference_entry_update_date.csv

// sudo mongoexport --db bvs --collection collection_days_difference_avg --type=csv --fields Days_Average,Library,Journal --out avg_difference_entry_update_date.csv
