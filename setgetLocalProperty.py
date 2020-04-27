#导入SparkContext
from pyspark import SparkContext
#导入SparkConf
from pyspark import SparkConf
#创建一个SparkConf对象
conf = SparkConf()
conf.set('master','spark://hadoop4:7077')
#初始化一个SparkContext对象
sc = SparkContext(conf=conf)
#getLocalProperty和setLocalProperty获取和设置在本地线程中的属性信息。
#通过setLocalProperty设置，设置的属性只能对当前线程提交的作业起作用，对其他作业不起作用。
sc.setLocalProperty('abc','hello')
sc.getLocalProperty('abc')
#关闭SparkContext,释放资源
sc.stop()

