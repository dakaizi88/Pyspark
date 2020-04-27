#导入SparkContext,SparkConf
from pyspark import SparkContext ,SparkConf
#导入numpy
import numpy as np
#创建一个SparkConf对象
conf = SparkConf().setAppName('ck').setMaster('spark://hadoop4:7077')
#初始化一个SparkContext对象
sc = SparkContext(conf=conf)
#创建RDD
rdd = sc.binaryFiles('/datas/pics/')
#用于获取注册到集群的应用的id
print('applicationId:',sc.applicationId)
result = rdd.collect()
for data in result:
#数组的第一个元素展示的是标题
	print(data[0],data[1][:10])
sc.stop()
