'''
Created on 2019. 5. 8.

@author: Playdata
'''

import pymongo



def find():
    ooo = []
    con = pymongo.MongoClient("127.0.0.1", 27777)
    # 앞 괄호는 db 뒤 괄호는 collection
    t = con['news']['news_main']
    # db.news_main.ensureIndex({"url" : 1 } , {unique : true})
    t.create_index([("url", pymongo.ASCENDING)], unique=True)
    for i in t.find({"category" : "정치"}, {"_id" : 0 , "url": 1 , "contents" : 1 }).limit(100):
        ooo.append(i)
    return ooo