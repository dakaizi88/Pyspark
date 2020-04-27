#导入SparkContext
from pyspark import SparkContext
#导入SparkConf
from pyspark import SparkConf
#导入numpy
import numpy as np
#创建一个SparkConf对象
conf = SparkConf()
#
conf.set('master','spark://hadoop4:7077')
#初始化一个SparkContext对象
sc = SparkContext(conf=conf)
#广播一个hello字符串
broad = sc.broadcast(' hello ')
#调用parallelize方法创建RDD,指定3个分区
rdd = sc.parallelize(np.arange(27),3)
#用于获取注册到集群的应用的id
print('applicationId:',sc.applicationId)
#将rdd中的每个元素与广播变量字符串拼接,调用Action算子collect方法触发任务
print(rdd.map(lambda x:str(x)+broad.value).collect())
#关闭SparkContext,释放资源
sc.stop()