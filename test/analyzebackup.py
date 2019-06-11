"""
from cpcloser
"""

# -*- coding:utf-8 -*-
# 사전준비 numpy, JPype1-0.6.3-cp27-cp27m-win_amd64.whl, KoNLPy

from konlpy.tag import Kkma
#from konlpy.utils import pprint
import time
import pymongo
from konlpy.tag import Komoran
import multiprocessing
import datetime

class cp_news_data:
    #전역변수들의 목록
    #오늘 날짜를 YYYYmmdd 포맷으로 표현합니다, 날짜를 리스트로 받는 형식으로 수정할 예정입니다
    day = time.strftime("%Y%m%d")
    
    
    con = pymongo.MongoClient("127.0.0.1",27777)
    #뉴스 데이터를 분석한 뒤 적재할 컬렉션
    an = con['news']['news_analyze']
    
    an.create_index([("url",pymongo.ASCENDING)],unique=True)
    #뉴스 데이터를 뽑아올 컬렉션
    t = con['news']['news_main']
    
    # db.news_main.ensureIndex({"url" : 1 } , {unique : true})
    t.create_index([("url", pymongo.ASCENDING)], unique=True)
    #from word_analying import analyze_contents
    #Kakma = Kkma()
    #단어 추출에서 제외할 목록들이 들어갈 단어사전입니다
    komoran = Komoran(userdic="../crawler/navernewss/user_dic.txt") #\u로 시작하기 때문에 \\ 해야 합니다.
    #단어사전이 없다면 기본 생성자를 사용합니다
    #komoran = Komoran()
    
    
    """def konlpy_analying(self,newscontents):
        print(self.komoran.pos(newscontents))
    """
    
    
    #아직 분석되지 않는 뉴스(chk==0)들을 찾아오는 함수입니다
    def find(self,d):
        ooo = []
        
        for i in self.t.find({"posttime" : "20190610"}, {"_id" : 0 ,"news_number" : 1,"category" : 1 ,"url": 1 , "contents" : 1 ,"posttime" : 1}):
            ooo.append(i)
        
        return ooo
    
    def abc(self):
        
        #병렬 스레드 작업을 처리할 풀입니다    
        p = multiprocessing.Pool(8)
        
        all_content=self.find(self.day) # 뉴스 객체 리스트 찾기
        
        print(len(all_content))
        
        #all_content의 요소들을 all_append 함수에 차례대로 매핑합니다    
        p.map(self.all_append,all_content)
      
    def all_append(self,cont):        
        
        try:
            #분석할 콘텐츠 대입
            analyze_contents = cont['contents'].replace("// flash 오류를 우회하기 위한 함수 추가\nfunction _flash_removeCallback() {}","").replace("\n","").replace("\t","").replace(".com","").replace("등","").replace("갓","").replace(".","")    
            #분석
            ac = self.komoran.nouns(analyze_contents)
            #몽고디비에 삽입할 bson 포맷 생성
            di = {}
            for i in ac:
                if str(i) in di:
                    di[str(i)] += 1
                else: 
                    di[str(i)] = 1
            news = {"news_number" : cont["news_number"],"category" : cont['category'],"url" : cont['url'] ,"posttime" : cont['posttime'] ,'data' : di}
                
            del analyze_contents,ac,cont,di
            #몽고디비에 입력
            self.mongodata(news)
                
        except Exception as e:
            print(cont['url'])
            print(e)
            pass    
                
            
            
    def mongodata(self,j):
        try:
            self.an.insert_one(j)
        except Exception as e:
            print(e)
            pass    

if __name__ == '__main__':
    stt = time.time()   
    cnd = cp_news_data()
    cnd.abc()
    #cnd.t.update_many({"posttime":cnd.day,"chk" : 0},{"$set" :{"chk" : 1}})
    print(time.time() - stt)