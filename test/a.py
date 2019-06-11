import pymongo

con = pymongo.MongoClient("localhost",27777)
t = con['news']['news_main']


ma = int(t.find({}).sort([("news_number", pymongo.DESCENDING)]).limit(1)[0]["news_number"])
print(ma)
for i in t.find({"news_number" : -1}).sort([("_id" ,pymongo.ASCENDING)]):
    t.update_one({"_id" : i["_id"]},{"$set" : {"news_number" : ma + 1}})
    ma += 1