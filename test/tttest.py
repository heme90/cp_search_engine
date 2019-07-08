'''
Created on 2019. 6. 11.

@author: Playdata
'''
import pandas as pd
abc = ['a','b','b','b','b','b','c']
aadef = {}
for i in abc:
    if i in aadef:
        aadef[i] += 1
    else: 
        aadef[i] =1
print(aadef)    

f = pd.read_pickle("matrix_news20190611.pkl")
open("aa.txt","w").write(str(f))
print(f['134246'])
