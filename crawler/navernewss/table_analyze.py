'''
Created on 2019. 6. 11.

@author: Playdata
'''
import math
import time
import os
from numpy import dot
from numpy.linalg import norm

import numpy as np
import pandas as pd

from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext



class tf_cos():
    day = time.strftime("%Y%m%d")      
    
    def tf(self,data):
        
		matrix_data = data.as_matrix()
		#rows를 리턴
		mat_rows = matrix_data.shape[0]
		#cols를 리턴
		mat_cols = matrix_data.shape[1]
		arr = []
        
		for line in matrix_data:
        
			arr.append(self.tf_udf((line,mat_rows,mat_cols)))
        
		#memoryouterror를 방지하기 위해 로컬에 잠시 저장하는 과정을 거친다
        pd.DataFrame(arr, index=data.index, columns=data.columns).transpose().to_csv('C:\\MyPython\\news_engine\\crawler\\navernewss\\analyzed_data\\tf_idf\\tf_idf' + self.day + '.csv')
    
    #shape (row,cols)
        
    def tf_udf(self,ar):
        
        
        line = ar[0]
        mat_rows = ar[1]
        mat_cols = ar[2]
        
        count =0
		
        for j in line:
			if j == 0:
				count+=1
				
        IDF = math.log(mat_rows/(mat_rows-count))
        
		for j in range(1,mat_cols):
            line[j] = math.log(line[j]+1) * IDF
        
        return line
        
    def cos_sim(self,A, B):
	
        return dot(A, B)/(norm(A)*norm(B))
    
    def cos_mat(self,data):
        
        mat = data.as_matrix()
        mat_cols = mat.shape[1]
        
        cos_matrix = np.zeros((mat_cols-1, mat_cols-1))
        #헤더를 제거하기 위해 0번 인덱스는 스킵합니다
		for i in range(1,mat_cols - 1):
  
            cos_matrix[i-1] =  self.cos_mat_udf(mat[::,i], mat)
        
        
        #인덱스 , 컬럼을 명시적으로 지정하여 만듭니다
        df = pd.DataFrame(data=cos_matrix, index=data.columns[1::], columns=data.columns[1::])
        df.fillna(0, inplace=True)
        df.transpose().to_csv('C:\\MyPython\\news_engine\\crawler\\navernewss\\analyzed_data\\cos_sim\\cos_sim' + self.day + '.csv')
        
    def cos_mat_udf(self, line,matrix_data):    
        abc = []
        for j in range(1,matrix_data.shape[1]):
            abc.append(self.cos_sim(line ,matrix_data[::,j]))
        
        return abc      
        

def tf_idf():
    day = time.strftime("%Y%m%d") 
    localcsvpath = 'C:\\MyPython\\news_engine\\crawler\\navernewss\\analyzed_data\\tf_idf\\tf_idf' + day + '.csv'
    hdfscsvoutputpath = "hdfs://192.168.56.104:9000/compath/idf_csv/"
    df_res = sqlc.read.csv(localcsvpath).coalesce(1)
        
    df_res.write.csv(hdfscsvoutputpath ,mode="overwrite",header=True)
    os.remove(localcsvpath)
        
    del df_res   
    
def cos_sv():
    day = time.strftime("%Y%m%d") 
    localcsvpath = 'C:\\MyPython\\news_engine\\crawler\\navernewss\\analyzed_data\\cos_sim\\cos_sim' + day + '.csv'
    hdfscsvoutputpath = "hdfs://192.168.56.104:9000/compath/cos_csv/"
    
        
    df_res = sqlc.read.csv(localcsvpath)
                 
    df_res.write.csv(hdfscsvoutputpath ,mode="overwrite",header=True)
           
if __name__ == '__main__':
    ti = time.time()
	print("앱 시작")
    sc_conf = SparkConf()
    
	sc = SparkContext(conf=sc_conf)
    sqlc = SQLContext(sc)
	print("sql context 생성")
	csvinputpath = "C:\\MyPython\\news_engine\\crawler\\navernewss\\zxc.csv"
    data = pd.read_csv(csvinputpath)
    print("dataframe 로딩 완료")
	t = tf_cos()
    t.tf(data)
	print("tf_idf 테이블 생성 완료")
    t.cos_mat(data)
    print("코사인 유사도 테이블 생성 완료")
	tf_idf()
	print("tf_idf 테이블 저장 완료")
    cos_sv()
	print("코사인 유사ㅗ 테이블 저장 완료")
    print(time.time()-ti)
    