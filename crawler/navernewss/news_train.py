'''
Created on 2019. 5. 8.

@author: jjackjjack
'''
#-*- coding:utf-8 -*-


import multiprocessing
import time

import gensim
from gensim.models import doc2vec
import pymongo
import os
from collections import namedtuple

class trainer:
    con = pymongo.MongoClient("localhost",27777)
    ta = con["news"]["news_main"]
    cores = multiprocessing.cpu_count()
    day = time.strftime("%Y%m%d")
    
    def find(self):
        ooo = []
        # db.news_main.ensureIndex({"url" : 1 } , {unique : true})
        for i in self.ta.find({"posttime" : self.day}, {"contents" : 1 ,"posttime" : 1}):
            ooo.append(i)
        return ooo
    
    def train_conf(self):
        md = doc2vec.Doc2Vec(
    dm=0,  # PV-DBOW / default 1
    dbow_words=1,  # w2v simultaneous with DBOW d2v / default 0
    window=8,  # distance between the predicted word and context words
    vector_size=100,  # vector size
    alpha=0.025,  # learning-rate
    seed=1234,
    min_count=-1,  # ignore with freq lower
    min_alpha=0.025,  # min learning-rate
    workers=self.cores,  # multi cpu
    hs=1,  # hierarchical softmax / default 0
    negative=10,  # negative sampling / default 5
    )
        return md
   
    #파일의 텍스트값에서 벡터 추출, 단어간 유사도 측정
    def news_train(self):
        td = namedtuple('TaggedDocument', 'words tags')
        ooo = self.find()
        if(os.path.exists("news.model")):
            md = doc2vec.Doc2Vec.load("news.model")
        else:
            md = self.train_conf()
        
        
        md.build_vocab(ooo)
        
        md.train(ooo, epochs=md.iter, total_examples=md.corpus_count)
        #강화학습을 위한 반복문
        """for epoch in range(10):
            md.train(sentences, total_examples=md.corpus_count, epochs=md.iter)
            md.alpha -= 0.002 # decrease the learning rate
            md.min_alpha = md.alpha # fix the learning rate, no decay"""
    
        model_name = 'news.model'
        #모델을 저장하면 이후 불러올 수 있다
        md.save(model_name)
        #재대입
        md = gensim.models.Doc2Vec.load(model_name)



if __name__ == '__main__':
    start = time.time()
    tn = trainer()
    tn.news_train()
    
    end = time.time()
    print("During Time: {}".format(end-start))
    
    