'''
Created on 2019. 5. 9.

@author: Playdata
'''
import multiprocessing
import time


#뷰티풀 수프 임포트
import bs4
import pymongo
from selenium import webdriver
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.chrome.options import Options

#크롬 드라이버의 옵션을 설정하는 구역입니다
chrome_options = Options()
chrome_options.add_argument("--headless")   
chrome_options.add_argument("--window-size=1920x1080")     
chrome_options.add_argument("disable-gpu")
chrome_options.add_argument("user-agent=Mozilla/5.0 (Macintosh; Intel Mac OS X 10_12_6) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/61.0.3163.100 Safari/537.36")

#셀레늄 모듈은 드라이버를 열때 가장 많은 시간을 소요합니다, 드라이버를 열어두고 필요할때마다 받아쓰는 것이 효율적입니다   
driver_a = webdriver.Chrome('chromedriver.exe',options= chrome_options)


def mongoconnection():
    #몽고디비 컬렉션 객체를 받아오는 함수입니다
    t = pymongo.MongoClient("127.0.0.1",27777)
    tt = t['api']['Dartsearch']
    return tt

def dartparser_a(i):
    #ssl 프로토콜로 데이터 보안을 갖추기 위해 http --> https 로 url을 변경했습니다
    #https://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190423000328
    conurl ="https://dart.fss.or.kr/dsaf001/main.do?rcpNo="+ str(i)
    driver_a.get(conurl)
    try:
        #xpath를 사용하면 셀렉터 문법에서 유일성을 갖출수 없는 상황을 대처할 수 있습니다
        driver_a.find_element_by_xpath("//*[@id='ext-gen10']/div/li[6]/div/a/span").click()
        iframes = driver_a.find_elements_by_tag_name('iframe')
        
        #셀레늄 라이브러리는 동적 파싱에 특화되있으며 눈에 보이는 모든 화면을 파싱할 수 있습니다
        #iframe 또는 자바스크립트로 동적 구성된 웹페이지도 이런 방식으로 스크래핑 합니다
        driver_a.switch_to_frame(iframes[0])
        
        #lxml은 xml을 더욱 효율적으로 사용할 수 있게 도와주는 라이브러리입니다
        d = bs4.BeautifulSoup(driver_a.page_source,"lxml")
        
        doc = d.select("body")
        print(doc[0].text)        
    
    except NoSuchElementException:
        pass
    except Exception as e:
        print(e)
        pass
              
        
        
def dartparser_b(i):
    #http://dart.fss.or.kr/dsaf001/main.do?rcpNo=20190423000328
    conurl = "https://dart.fss.or.kr/dsaf001/main.do?rcpNo=" + str(i)
    driver_a.get(conurl)
    
    try:
        driver_a.find_element_by_xpath("//*[@id='ext-gen10']/div/li[7]/ul/li[1]/div/a/span").click()
        iframes = driver_a.find_elements_by_tag_name('iframe')
        driver_a.switch_to_frame(iframes[0])
        d = bs4.BeautifulSoup(driver_a.page_source,"lxml")
        doc = d.select("table")
        """di = dict()
        for t in doc.select("tr"):
            tn = t.select("td")[0].text
            di[tn] = """
        print(doc[0])    
                    
        #tabs.append(doc[0].text)        
    except NoSuchElementException:
        pass
    except Exception as e:
        print(e)
        pass
             
        

#예상치 못한 오류로 모듈이 강제종료 당했을시 크롬 드라이버가 프로세스상에 남습니다
#그 경우에는 killchromedriver.bat를 관리자 권한으로 실행해야 합니다
if __name__ == '__main__':
    st = time.time()
    
    con = mongoconnection()
    res = []
    for i in con.find({}, {"_id" : 0 , "list.rcp_no": 1 }).limit(100):
        res.append(i["list"]["rcp_no"])
    #멀티프로세싱 라이브러리는 임계지점에 대한 관리를 기본적으로 지원합니다
    #driver_a라는 단일 객체에 대한 동시 접근을 관리해줍니다
    #스레드 개수는 cpu 코어 개수의 2배까지가 적절합니다
    p = multiprocessing.Pool(8)
    #res 리스트에 있는 요소들을 타겟 메소드에 차례대로 매핑해줍니다
    p.map(dartparser_b,res)
    print(time.time() -st)   
    driver_a.close()