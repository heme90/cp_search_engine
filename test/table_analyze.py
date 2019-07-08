'''
Created on 2019. 6. 11.

@author: Playdata
'''
import math
import time

from numpy import dot
from numpy.linalg import norm

import numpy as np
import pandas as pd




class tf_cos():
    day = time.strftime("%Y%m%d")
    
            
    def tf(self,data):
        matrix_data = data.as_matrix()
        mat_rows = matrix_data.shape[0]
        mat_cols = matrix_data.shape[1]
        arr = []
        for line in matrix_data:
            #arr.append((line,mat_rows,mat_cols))    
            arr.append(self.tf_udf((line,mat_rows,mat_cols)))
        
        
        #res = pol.map(self.tf_udf,arr)
        
        
        pd.DataFrame(arr, index=data.index, columns=data.columns).to_csv("res.csv")
    
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
        for j in range(1,mat_cols): # 만일 데이터 단어의 이름이 안들어가 있다면 range를 고칠 것
            line[j] = math.log(line[j]+1) * IDF
        
        return line
        
    def cos_sim(self,A, B):
        return dot(A, B)/(norm(A)*norm(B))
    
    def cos_mat(self,data):
        
        mat = data.as_matrix()
        mat_cols = mat.shape[1]
        
        cos_matrix = np.zeros((mat_cols-1, mat_cols-1))
        for i in range(1,mat_cols - 1):
            #arr.append(mat[::,i])
            cos_matrix[i-1] =  self.cos_mat_udf(mat[::,i], mat)
        
        #res = pol.map(self.cos_mat_udf,arr)
        
        """for i in range(len(res)):
            cos_matrix[i] = res[i]
        """      
        
        df = pd.DataFrame(data=cos_matrix, index=data.columns[1::], columns=data.columns[1::])
        df.fillna(0, inplace=True)
        df.to_csv('res_cos.csv')
        
    def cos_mat_udf(self, line,matrix_data):    
        abc = []
        for j in range(1,matrix_data.shape[1]):
            abc.append(self.cos_sim(line ,matrix_data[::,j]))
        
        return abc
        
        
        
    def gotf(self):
        #data = pd.read_csv('zxc.csv')
        #self.cos_mat(self.data)
        self.tf(self.data)
        print("tf done")
        self.cos_mat(self.data)
        print("cos sim done")        
        
    
            
if __name__ == '__main__':
    
    ti = time.time()
    data = pd.read_csv('zxc.csv')
    t = tf_cos()
    t.tf(data)
    t.cos_mat(data)
    print(time.time()-ti)
    