#导入SparkContext,SparkConf
from pyspark import SparkConf,SparkContext
#导入numpy
import numpy as np
#创建一个SparkConf对象
conf = SparkConf().setAppName('ck').setMaster('spark://hadoop4:7077')
#初始化一个SparkContext对象
sc = SparkContext(conf=conf)
#调用parallelize方法创建RDD
rdd = sc.parallelize(np.arange(10))
#用于获取注册到集群的应用的id
print('applicationId',sc.applicationId)
#调用Action算子collect方法,将RDD类型的数据转化为数组
print(rdd.collect())
#关闭SparkContext,释放资源
sc.stop()