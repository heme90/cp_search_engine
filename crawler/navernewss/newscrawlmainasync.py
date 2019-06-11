'''
Created on 2019. 5. 2.

@author: Playdata
'''
import datetime
import multiprocessing
import os
import time

import navernewss.navercrawlasync
os.system("compath.bat")

def hi():
    now = time.strftime("%Y%m%d")
    e = datetime.datetime(int(now[0:4]), int(now[4:6]), int(now[6:]))
    #오늘부터 몇일치 뉴스를 크롤링할지 결정하는 변수입니다 --> n일치 => numdays = n
    numdays = 1
    date_list = [(e - datetime.timedelta(days=x)).strftime('%Y%m%d') for x in range(0, numdays)]
   
    #정치 섹션
    pol = ["https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=100&sid2=264",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=100&sid2=265",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=100&sid2=268",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=100&sid2=266",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=100&sid2=267",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=100&sid2=269", ]
    
    #경제 섹션
    eco = ["https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=101&sid2=259",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=101&sid2=258",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=101&sid2=261",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=101&sid2=771",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=101&sid2=260",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=101&sid2=262",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=101&sid2=310",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=101&sid2=263"]
    
    #사회 섹션
    soc = ["https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=102&sid2=249",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=102&sid2=250",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=102&sid2=251",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=102&sid2=254",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=102&sid2=252",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=102&sid2=59b",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=102&sid2=255",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=102&sid2=256",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=102&sid2=276",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=102&sid2=257"]
    
    #생활/문화 섹션
    lifeandculture = ["https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=103&sid2=241",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=103&sid2=239",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=103&sid2=240",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=103&sid2=237",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=103&sid2=238",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=103&sid2=376",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=103&sid2=242",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=103&sid2=243",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=103&sid2=244",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=103&sid2=248",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=103&sid2=245"]
    
    #세계 섹션
    world = ["https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=104&sid2=231",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=104&sid2=232",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=104&sid2=233",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=104&sid2=234",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=104&sid2=322"]
    
    #it/기술 섹션
    itgi = ["https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=105&sid2=731",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=105&sid2=226",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=105&sid2=227",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=105&sid2=230",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=105&sid2=732",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=105&sid2=283",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=105&sid2=229",
         "https://news.naver.com/main/list.nhn?mode=LS2D&mid=shm&sid1=105&sid2=228"]
    p = multiprocessing.Pool(8)
    newspagelist = [pol, eco, soc, lifeandculture, world, itgi]
    
    params = []
    for d in date_list:
        for sec in newspagelist:
            params.append((d,sec))
                    
    p.map(navernewss.navercrawlasync.navercrawl,params)

if __name__ == '__main__':
    st = time.time()
    
    
    hi()
    
    print(time.time() - st)

    

     
    
