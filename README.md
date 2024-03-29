실행방법

		하둡 클러스터 저장소에 /compath 폴더 이하 모든 디렉토리를 갖춥니다 ( table_analyze.py 참조 )
		python에 bs4, konlpy, gensim, cython, pandas, numpy, pyspark, pymongo를 각각 설치해둡니다
		crawl/navernewss/cp_crawl.bat / cp_hdfs.bat에 있는 경로들을 윈도우 환경에 갖춥니다
		mongodb 4.0.8 이상 버전을 설치한뒤 27777번 포트를 열어둡니다
		cp_crawl.bat과 cp_hdfs.bat을 각각 실행합니다



cp_search_engine 개요

1. 목적
		
			사용자가 뉴스를 검색할때 중복된 결과(단순 글자의 중복이 아닌 문서의 내용 자체의 중복)
			를 제거하여 더 적은 링크횟수로 더 다양한 정보를 제공하기 위한 웹 서비스의 하부 프로젝트

2. 구현 방법
		
	2-1. 데이터 수집및 전처리기의 구현
		
			수집하는 데이터는 네이버 뉴스(news.naver.com)의
			정치, 경제, 사회, 생활/문화, 세계, IT/과학 의 하부 목록들에 업로드되는 뉴스들이다
			데이터 수집기는 
			링크 수집, 수집한 링크들의 중복 여부 확인, 문서 요청, 저장, 분석 모듈 호출의 단계로 설계되었다
		
	1. 링크 수집	
		
			requests를 통해 최상위 페이지(ex 2019년 4월 30일 정치 > 청와대 섹션)에 접근한 뒤 페이지를 이동해가며
			뉴스로 이동할 수 있는 링크들을 수집한다

			매 페이지 이동시에 이전 페이지의 데이터와 대조하며 마지막 페이지에 도달했는지 점검한다
			마지막 페이지에 도달하면 링크 수집 과정을 마치고 다음 단계를 호출한다	
		
	2. 중복 여부 확인
		
			크롤링 머신이 수집한 링크들에는 당연하게도 이전에 수집한 뉴스의 url이 포함되어있다.
			이러한 중복링크를 제거하지 않고 테스트해본 결과 10000건의 문서 수집에 
			10 ~ 15분 정도의 시간이 지속적으로 소요되었다

			이는 쿼드코어 cpu에서 무리없이 지원하는 멀티프로세싱(8core)과 
			task의 불균등함을 해소하기 위한 비동기 코루틴(후술)이 
			모두 적용된 수치였기때문에 중복 제거 과정을 넣는것은 필수불가결한 일이였다

			링크의 수집을 마치면 적재된 뉴스 본문들의 url과 필터링된 뉴스 url을 가져와 수집 데이터와 비교한다, 
			당일 수집한 데이터들과 비교하는 것이므로 무의미한 데이터 인출을 줄이기 위해 
			비교하는 기존 데이터또한 그날 수집한 뉴스들로 제한한다 
			뉴스의 필터링 기준은 300자 이하의 포토뉴스와 메타 데이터가 일치하지 않는 연예 뉴스이다

			차집합 연산을 빠르게 수행하기 위해 기존 array를 numpy의 ndarray로 대체하였으며 중복 제거 과정을 적용한 뒤 
			반복되는 실행에서 수집기 전체의 실행시간이 1분에 근접한 정도로 단축되었다

	3. 문서 요청
		
			수집한 링크중 중복되지 않은 url들을 선별하고 나면 문서를 요청하고 적재할 데이터를 뽑아내는 과정이 시작된다
			검색엔진에 사용하기 위해 적재할 데이터는 일자, 제목, 분류, 본문, url, 분석 여부(후술) 이며 
			html 문서의 파싱은 bs4와 lxml을 사용하였고 파싱 입력 과정은 비동기적으로 이루어진다


			검색엔진이 탑제될 상위 프로젝트의 목적(이하 compath)이 뉴스 검색을 통한 양질의 정보 전달이므로 
			적재하는 문서에도 일정 기준을 적용하였다 (글자수 제한, 포토뉴스 제거, 연예뉴스 제거 등) 
			기준에 미달한 문서들은 별도의 db에 url만 저장되어  중복 제거 프로세스에서 활용된다

	4. 문서 저장
		
			사용할 뉴스 데이터들의 형태를 살핀 결과 테이블간의 관계보다는 i/o 속도가 중요하다고 판단, RDBMS인 Oracle보다는 
			mongoDB에 적재하기로 하였다
			위의 과정을 모두 마친 뉴스 데이터는 pyhthon의 py-mongo를 통해 mongodb에 적재된다 

			초기 설계에서는 requests로 문서를 파싱한 뒤 적재하는 과정을 싱글 스레드로 처리하여 
			1회 실행에 1시간 정도를 소요하였다 
			실행 시간을 감당 가능한 수준으로 단축시키기 위해 python 3.6++ 버전에서 지원되는 asyncio 와 
			기존의 multiprocessing을 조합해 문서의 요청, 저장 부분을 담당하는 함수들을 하나의 코루틴으로 결합하고 
			최상위 문서를 호출할때 multiprocessing.Pool 모듈을 사용하여  
			여러 주제의 뉴스를 동시에 비동기적으로 수집, 적재하는 형태가 되었다

			1시간에 근접하던 실행시간은 15000건에 15분 ~ 20분 정도로 단축되었으며(기존 1시간 20분) 
			이후 실행시간을 더욱 단축시킨 결과는 2. 중복 여부 확인에서 언급한 바와 같다
		
	5. 분석 모듈 호출
		
			데이터의 수집, 적재를 마친 후 인덱싱을 위해 분석 모듈을 호출한다, 분석 모듈은 konlpy를 사용하였으며 
			페이지 스코어링을 위한 TF/IDF 과정의 준비단계이기도 하다
			뉴스 데이터가 적재된 db에서 분석 여부 필드가 0인 레코드들을 추출하여 분석 모듈에 인자로 담는다
			분석 모듈의 실행이 끝나고 난 뒤 분석 여부 필드를 1로 update하여 중복 실행을 방지한다

			수집한 데이터의 본문을 적절히 전처리한 뒤 단어 단위로 분철하여 배열 형태로 반환하고 재사용을 위해
			pymongo를 통해 mongoDB에 적재된다

			komoran 형태소 분석기를 사용하여 뉴스 본문에 대하여 모든 명사들의 tf값을 구하여 별도의 컬렉션에 저장한다 
			구성된 tf 테이블을 통헤 tf * idf 테이블을 구성하여 단어와 문서의 유사도를 구하고 코사인 유사도 테이블을 구성하여 
			문서와 문서간 유사도를 구한다
		
	2-2. 데이터 인덱싱과 검색 알고리즘의 구현
	
			기존 검색 엔진은 입력된 단어를 포함하는 문서를 기준으로 페이지 스코어링을 적용하여
			검색어가 포함된 모든 문서를 정렬 기준에 따라 보여주는 방식이라면
			c-search는 주어진 단어에 대한 상관지수가 높은 단어들을 모델에서 추출한 뒤 함수를 생성하여
			실제로는 그 단어를 포함하지 않더라도 검색어와 관련이 있는 문서들도 검색될 수 있다

			c-search의 검색 알고리즘은 단어 입력 -> 선형방정식 변수 추출 -> 스코어링을 통한 문서 정렬, 
			최상위 문서들과의 중복 제거로 이루어진다
		
	1. 입력 단어에 대한 선형방정식 생성과 문서정렬
		
			#수집된 뉴스 문서들은 전처리 과정을 통해 강화학습 가능한 데이터로 가공된 후 doc2vec을 통해 훈련 모델을 만들어낸다 
			//개인 프로젝트로 이관
			#생성된 모델은 입력된 검색어와 상관지수가 높은 단어들을 모델 내부에서 추출하여 키-값 쌍의 형태로 반환한다

			단어를 입력한 뒤 분석 단계에서 생성한 단어 - 단어간 관계 테이블을 통해 추출한다
			ex){word1 : 0.7454, word2 : 0.6464 ....}

			반환된 결과를 통해 
			score = 0.7454 * count_of_word1 + 0.6464 * count_of_word2 ... 와 같은 다원일차함수을 생성할 수 있다
			이 함수의 반환값인 tf값들의 합이 검색어에 대한 문서의 정렬 순서를 결정짓게 된다
		
	2. 최상위 문서들과의 중복 제거
		
			효율적인 문서 전달을 위해 c-search는 미리 생성해둔 문서간 유사도를 통해 내용이 중복된 문서를 걸러낸다
			위 방식대로 검색 알고리즘을 설계한다면 최상위 5개(혹은 그 이상) 의 문서는 검색을 요청한 사용자가 
			원하는 정보를 원하는 만큼 적절히 나타내 줄 것이다
			하지만 그와 비슷한 내용이 10번, 20번 반복해서 나타난다면 사용자는 추가적인 정보를 얻기 위해 
			이미 파악한 내용을 해쳐나가며 다음 페이지로 넘어가는 노동을 반복하게 된다
			하여 c-search에서는 최상위 5(++)개 문서와의 문서간 유사도가 지나치게 높은 문서들을 검색 결과에서 사전에 제거한다
			(중복 제거에 앞서 데이터 수집 과정에서 konlpy를 통해 mongoDB에 저장해둔 제목 - 배열 쌍을 통해 
			문서간 유사도를 코사인 유사도 방식으로 구해두었다)
			c-search의 최종적인 목표는 검색어에 대한 유사 단어중 과반수(미정) 이상이
			검색 결과의 첫번째 페이지에 표시될 문서들에 포함되도록 하는 것이다		
			문서 -> 문서간 링크의 알고리즘은 문서간 코사인 유사도를 사용한 별도의 알고리즘으로 이루어진다
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		




		
	
	
	
	

