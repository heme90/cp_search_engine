'''
Created on 2019. 6. 11.

@author: Playdata
'''
"""from pyspark.ml.feature import IDF
from pyspark.mllib.feature import HashingTF
from pyspark.mllib.feature import HashingTF, IDF
from pyspark.mllib.feature import Normalizer
from pyspark.mllib.linalg.distributed import IndexedRowMatrix"""
#from pyspark.sql.types import Row
#{'Unnamed: 0': '-3', '154029': 0.0, '154030': 0.0, '154031': 0.0 }

from array import array
import math
import time

from numpy import dot
from numpy.linalg import norm
from pyspark import SparkContext
from pyspark.conf import SparkConf
from pyspark.sql.context import SQLContext
from pyspark.sql.functions import UserDefinedFunction

import numpy as np
import pandas as pd


def tf_udf(arrg):
        count =0
        line = arrg[0]
        mat_rows = arrg[1]
        mat_cols = arrg[2]
        ks = line.keys()
        print(ks)
        for j in ks:
            if line[j] == 0:
                count+=1
        IDF = math.log(mat_rows/(mat_rows-count))
        for j in range(1,mat_cols):
            line[ks[j]] = math.log(line[ks[j]]+1) * IDF
        return line


def cos_sim(self,A, B):
        return dot(A, B)/(norm(A)*norm(B))


def cos_mat_udf(self, line, data):    
        abc = []
        for j in range(1,data.shape[1]):
            abc.append(self.cos_sim(line ,data[::,j]))
        return abc

#from pyspark.sql import SQLContext
class tf_cos():
    day = time.strftime("%Y%m%d")
    tt = time.time()
    
    sc_conf = SparkConf()
    #sc_conf.setSparkHome("/usr/local/spark")
    #sc_conf.setMaster("spark://192.168.56.104:7077")
    #sc_conf.setAppName("dog")
    #sc_conf.set('spark.cores.max', '4')
    #sc_conf.set('spark.logConf', True)
    #sc_conf.
    sc = SparkContext(conf=sc_conf)
    sqlc = SQLContext(sc)
    
    
    
    
    # 문서의 tf값을 구하는 함수입니다
    def tf(self,data):
        
        #
        spark_table = self.sqlc.createDataFrame(data)
        matrix_data = data.as_matrix()
        mat_rows = matrix_data.shape[0]
        mat_cols = matrix_data.shape[1]
        fun = UserDefinedFunction(tf_udf)
        abc =  spark_table.rdd.map(lambda x : fun(array(x.asDict(),mat_rows,mat_cols)))
        print(SQLContext.createDataFrame(self.sqlc,abc))
        
    def cos_sim(self,A, B):
        return dot(A, B)/(norm(A)*norm(B))
    
    def cos_mat(self,data):
        matrix_data = data.as_matrix()
        mat_cols = matrix_data.shape[1]
        cos_matrix = np.zeros((mat_cols-1, mat_cols-1))
        for i in range(1,mat_cols - 1):
            cos_matrix[i-1] =  self.cos_mat_udf(matrix_data[::,i], matrix_data)
        
        
        
        df = pd.DataFrame(data=cos_matrix, index=data.columns[1::], columns=data.columns[1::])
        df.fillna(0, inplace=True)
        df.to_csv('res_cos.csv')
        
    def cos_mat_udf(self, line, data):    
        abc = []
        for j in range(1,data.shape[1]):
            abc.append(self.cos_sim(line ,data[::,j]))
        return abc
        
        
    
    def gotf(self):
        data = pd.read_csv("zxc.csv")
        self.tf(data)
        #self.cos_mat(data)
        print("tf done")
        
        print("cos sim done")    
            
if __name__ == '__main__':
    ti = time.time()
    t = tf_cos()
    t.gotf()  
    print(time.time()-ti)
    