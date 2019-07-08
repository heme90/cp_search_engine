import os
#import shutil
import time

import py4j
from pyspark.conf import SparkConf
from pyspark.context import SparkContext
from pyspark.sql.context import SQLContext


#from hdfs import InsecureClient
class pymatrix_spark():
    tt = time.time()
    day = time.strftime("%Y%m%d")
    #day = "20190619"
    sc_conf = SparkConf()
    #sc_conf.setSparkHome("/usr/local/spark")
    #sc_conf.setMaster("spark://192.168.56.104:7077")
    #sc_conf.setAppName("dog")
    #sc_conf.set('spark.cores.max', '4')
    #sc_conf.set('spark.logConf', True)
    #sc_conf.
    sc = SparkContext(conf=sc_conf)
    print(sc)

    sqlc = SQLContext(sc)
    print(sqlc)
    #fs = hdfs.connect("192.168.56.104", 9000, "hadoop04")
    #client_hdfs = InsecureClient('hdfs://192.168.56.104:9000')
    
    
    def ac(self):
        localcsvpath = 'C:\\MyPython\\news_engine\\crawler\\navernewss\\temp_csv\\matrix_news_temp'+self.day + ".csv"
        localcsvmvpath = 'C:\\MyPython\\news_engine\\crawler\\navernewss\\temp_csv\\after_save\\matrix_news_temp'+self.day + ".csv"
        hdfscsvoutputpath = "hdfs://192.168.56.104:9000/compath/tf_csv/"
        
        df_res = self.sqlc.read.csv(localcsvpath).coalesce(1)
        
        #self.fs.mkdir(self.sc._jvm.org.apache.hadoop.fs.Path("hdfs://192.168.56.104:9000/compath/tf_csv/"+self.day),True)
        
        try:
            df_pre = self.sqlc.read.csv("hdfs://192.168.56.104:9000/compath/tf_csv/")
            df_res.join(df_pre,'_c0',how="outer").fillna(0).coalesce(1).write.csv(hdfscsvoutputpath ,mode="overwrite",header=True)
            os.rename(localcsvpath,localcsvmvpath)
        
        except Exception:
            df_res.write.csv(hdfscsvoutputpath ,mode="overwrite",header=True)
            os.rename(localcsvpath,localcsvmvpath)
        
        
        
        del df_res
        
    
    def aac(self):
        hdfscsvinputpath = "hdfs://192.168.56.104:9000/compath/tf_csv/"+self.day + "/*.csv"
        hdfstotalcsvpath = "hdfs://192.168.56.104:9000/compath/tf_csv/res/"
        #self.fs.mkdir(self.sc._jvm.org.apache.hadoop.fs.Path("hdfs://192.168.56.104:9000/compath/tf_csv/res"),True)
        
        df_res = self.sqlc.read.format('com.databricks.spark.csv').option('header','true').load(hdfstotalcsvpath + "*.csv")
        
        df_today = df_pre = self.sqlc.read.format('com.databricks.spark.csv').option('header','true').load(hdfscsvinputpath)
        try:
            df_res = df_res.join(df_today,'_c0',how="outer").fillna(0)
        except Exception:
            df_res = df_pre
             
        df_res.saveAsTextFile("hdfs://192.168.56.104:9000/compath/tf_csv/res/",mode="overwrite")
    
        
if __name__ == '__main__':
    tt = time.time()
    pmx = pymatrix_spark()
    pmx.ac()     
    #pmx.aac()
    pmx.sc.stop()
    print(time.time() - tt) 