#导入SparkContext
from pyspark import SparkContext
#导入SparkConf
from pyspark import SparkConf
#导入numpy
import numpy as np
#创建一个SparkConf对象
conf = SparkConf()
conf.set('master','spark://hadoop4:7077')
#初始化一个SparkContext对象
sc = SparkContext(conf=conf)
#获取默认最小的分区数
print('defaultMinPartitions:',sc.defaultMinPartitions)
#关闭SparkContext,释放资源
sc.stop()