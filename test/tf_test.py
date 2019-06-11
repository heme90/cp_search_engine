'''
Created on 2019. 6. 11.

@author: Playdata
'''
import numpy as np
import csv
import pandas as pd
import math
# 1. 데이터 불러오기 불러온 데이터는 dataframe 형태로 지정되어야 함

data = pd.read_pickle('matrix_news20190611.pkl')
# print(data.columns)
# print(data)
matrix_data = data.as_matrix()
print(matrix_data.shape[1])
#TF = np.matrix()
#print(TF)

#for i in range(len(matrix_data)):
    
#TF
    # TF = 문서 내 단어가 있는 총 횟수     
#IDF
    # log 전체문서 수/해당 단어가 들어가 있는 문서 수
for i in range(len(matrix_data)):
    count =0
    for j in range(matrix_data.shape[1]):
        if matrix_data[i][j] == 0:
            count+=1
    print(str(i) + '번 단어의 IDF는 '+ str(math.log(len(matrix_data)/(len(matrix_data)-count))))
    
    IDF = math.log(len(matrix_data)/(len(matrix_data)-count))
    # TF * IDF
    for j in range(1,matrix_data.shape[1]): # 만일 데이터 단어의 이름이 안들어가 있다면 range를 고칠 것
        matrix_data[i][j] = math.log(matrix_data[i][j]+1) * IDF
#print(matrix_data[::,1])

#print(data[data.columns[1]])
# Matrix -> Dataframe = df의 value 값을 matrix에 있는 값으로 대체

#df 내에 있는 value 삭제

#print(data)
# df 내에 matrix 값으로 삽입
for i in range(1,matrix_data.shape[1]):
    data[data.columns[i]] = matrix_data[::,i]

# print(data)    

# csv 체크

data.to_csv('tf_to_csv.csv',encoding='utf-8')

# 이후 코사인 유사도