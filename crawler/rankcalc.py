'''
Created on 2019. 5. 7.

@author: Playdata
'''
#-*- coding:utf-8 -*-
#bs4, selenium 인스톨 필요
import bs4
from selenium.webdriver.chrome.options import Options



from selenium import webdriver





#name = input("찾고싶은 라이더명을 입력해주세요  = ")
def calc_self(name):
    chrome_options = Options()
    chrome_options.add_argument("--headless")   
    chrome_options.add_argument("--window-size=1920x1080")     
    chrome_options.add_argument("disable-gpu")   
    driver = webdriver.Chrome('chromedriver.exe',options= chrome_options)
    driver.get("http://tmi.nexon.com/kart/user?nick="+name+"&matchType=1")
    d = bs4.BeautifulSoup(driver.page_source,"lxml")
    
    c = dict()
    
    matches = d.select("div.right > div > section > div > p.result")
    
    ##inner > div.info > div.right > div > section:nth-child(16) > div > p.result
    
    for mt in range(0,len(matches)):
        res = matches[mt].text
        rank = ""
        if(res == "#리타이어"):
            rank = res
        else:
            rank = res[0:2]
                
        c[rank] = c.get(rank, 0) + 1
    
    bc = None
    bw = None
    for res,count in c.items():
        if bc == None or count > bc:
            bw = res
            bc = count
            
    print("최근  500회 게임중 가장 자주 달성한 등수\n")
    
    print(str(bw) + "\n")
    
    for i in range(1,9):
        ran = "#" + str(i)
        print(str(i) + "등 : " + str(c.get(ran,0)) + " 회 \n")
    
    print("리타이어 : " + str(c.get("#리타이어",0)) + " 회\n" )
    
    driver.close()
    
if __name__ == '__main__':

    
    name = input("찾고싶은 라이더명을 입력해주세요  = ")           
    
    calc_self(name)
    
    


