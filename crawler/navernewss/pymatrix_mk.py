import os
import time

import pymongo

import pandas as pd


class pymatrixmk():
    con = pymongo.MongoClient("localhost",27777)
    an = con['news']['news_analyze']
    
    day = time.strftime("%Y%m%d")
    #day="20190619"
    
    def find(self):
        ooo = []
        for i in self.an.find({"posttime" :self.day,"chk" : 0},{"_id" : 0 , "news_number" : 1 , "data" : 1}):              
            di = {}
            for j in i["data"]:
                #print(i["data"].get(j))
                if j in di:
                    di[j] += {str(i["news_number"]) : i["data"].get(j)}
                else: 
                    di[j] = {str(i["news_number"]) :i["data"].get(j)}
            ooo.append(pd.DataFrame(di).transpose())
        return ooo
    
    def mkmtx(self,abc):
        
        print(len(abc))
        
        df_p = abc[0].join(abc[1:],how='outer')
        df_p.fillna(0, inplace=True) 
        
        if os.path.exists("C:\\MyPython\\news_engine\\crawler\\navernewss\\analyzed_data\\wordcount\\res.csv"):
            print("csv already exist, merge to previous one")
            df_ex = pd.read_csv("C:\\MyPython\\news_engine\\crawler\\navernewss\\analyzed_data\\wordcount\\res.csv")
            df_p.join(df_ex,how="outer").fillna(0, inplace=True)
            df_p.to_csv("C:\\MyPython\\news_engine\\crawler\\navernewss\\analyzed_data\\wordcount\\res.csv")
            
        else:    
            print("save dataframe")
            df_p.to_csv("C:\\MyPython\\news_engine\\crawler\\navernewss\\analyzed_data\\wordcount\\res.csv")
        
        self.an.update_many({"posttime" : self.day,"chk" : 0},{"$set" : {"chk" : 1}})
        
        
    def gomt(self):
        self.mkmtx(self.find())  
        
    
if __name__ == '__main__':
    tt = time.time()
    pmx = pymatrixmk()
    pmx.gomt()
    print(time.time()-tt)        