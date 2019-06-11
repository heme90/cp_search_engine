import pymongo
import pandas as pd
import time

class pymatrixmk():
    con = pymongo.MongoClient("localhost",27777)
    an = con['news']['news_analyze']
    day = time.strftime("%Y%m%d")
    
    def find(self):
        ooo = []
        #날짜변경
        for i in self.an.find({"posttime" : self.day},{"_id" : 0 , "news_number" : 1 , "data" : 1}).limit(100):
        #for i in self.an.find({"posttime" : self.day},{"_id" : 0 , "news_number" : 1 , "data" : 1}):
            di = {}
            for j in i["data"]:
                #print(i["data"].get(j))
                if j in di:
                    di[j] += {str(i["news_number"]) : i["data"].get(j)}
                else: 
                    di[j] = {str(i["news_number"]) :i["data"].get(j)}
            ooo.append(di)
        
        return ooo
    
    def mkmtx(self,reses):
        print(len(reses))
        df_pre = pd.DataFrame(reses[0]).transpose()
        #print(df_pre)
        for i in range(1,len(reses)):
            df_pos = pd.DataFrame(reses[i]).transpose()
            #print(df_pos)
            df_pre = pd.merge(df_pre,df_pos,how='outer',left_index=True,right_index=True)
            print(i)
        df_pre.fillna(0, inplace=True)  
        df_pre.to_pickle("matrix_news"+ self.day +".pkl")
        #이후 날짜별로 있는 dataframe merge하는 함수도 필요
    def gomt(self):
        self.mkmtx(self.find())    
        
        
if __name__ == '__main__':
    pmx = pymatrixmk()
    pmx.gomt()        