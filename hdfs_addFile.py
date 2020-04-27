#导入SparkContext,SparkConf,SparkFiles
from pyspark import SparkConf,SparkContext,SparkFiles
#导入os
import os
#导入numpy
import numpy as np
#文件地址
path = 'hdfs://hadoop2:9000/spark/words'
#创建一个SparkConf对象
conf = SparkConf()
#设置为本地模式
conf.set('master','spark://hadoop4:7077')
#初始化一个SparkContext对象
sc = SparkContext(conf=conf)
#将本地文件上传到集群
sc.addFile(path)
#调用parallelize方法创建RDD
rdd = sc.parallelize(np.arange(10))
#定义一个函数
def fun(iterable):
	with open(SparkFiles.get('words')) as f:
		value = int(f.readline())
		return [x * value for x in iterable]
#
print(rdd.mapPartitions(fun).collect())
#关闭SparkContext,释放资源
sc.stop()