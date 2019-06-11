'''
Created on 2019. 5. 8.

@author: Playdata
'''
#-*- coding:utf-8 -*-

from doc2vectest.contents_url import find
import multiprocessing
import time

import gensim
from gensim.models import doc2vec




#pullrec == contents_url
#gensim-3.7.2-cp37-cp37m-win_amd64.whl 파일을 받은 뒤에
#pip install gensim-3.7.2-cp37-cp37m-win_amd64.whl
#그냥 인스톨하면 cython을 못잡아서 gensim.FAST_MODE == -1임
if __name__ == '__main__':
    #할당 코어 개수
    cores = multiprocessing.cpu_count()
    #pullrec에서 가져옴
    """ooo = find()
    print(ooo[1]['contents'].replace("\n", ""))"""
    
    #가장 간단한 메소드에서 파일을 사용하길래, 근데 이렇게하면 문서별로 식별자는 구할수없음, json으로 만들어볼까
    """for i in ooo:
        f = open("test.txt","a",encoding="utf-8")
        f.write(i['contents'])
"""
    
    #인공신경망 모델 생성
    md = doc2vec.Doc2Vec(
    dm=0,  # PV-DBOW / default 1
    dbow_words=1,  # w2v simultaneous with DBOW d2v / default 0
    window=8,  # distance between the predicted word and context words
    vector_size=100,  # vector size
    alpha=0.025,  # learning-rate
    seed=1234,
    min_count=-1,  # ignore with freq lower
    min_alpha=0.025,  # min learning-rate
    workers=cores,  # multi cpu
    hs=1,  # hierarchical softmax / default 0
    negative=10,  # negative sampling / default 5
    )
    
    #파일의 텍스트값에서 벡터 추출, 단어간 유사도 측정
    sentences = doc2vec.TaggedLineDocument("news.json")
    
    
    md.build_vocab(sentences)
    print(str(md))
    
    start = time.time()
    
    #단어 학습을 통해 유사도 모델 개선
    md.train(sentences, epochs=md.iter, total_examples=md.corpus_count)
    #강화학습을 위한 반복문
    """for epoch in range(10):
        md.train(sentences, total_examples=md.corpus_count, epochs=md.iter)
        md.alpha -= 0.002 # decrease the learning rate
        md.min_alpha = md.alpha # fix the learning rate, no decay"""
    end = time.time()
    print("During Time: {}".format(end-start))

    #doc2vec은 _pickle을 기본적으로 지원한다, 모델을 ansi 포맷으로 피클링되기 때문에 열어서 확인할 수는 없다
    model_name = 'news.model'
    #모델을 저장하면 이후 불러올 수 있다
    md.save(model_name)
    #재대입
    md = gensim.models.Doc2Vec.load(model_name)
    
    print(md.wv.distance("미국", "북한"))