#导入SparkContext
from pyspark import SparkContext
#导入SparkConf
from pyspark import SparkConf
#创建一个SparkConf对象
conf = SparkConf()
conf.set('master','spark://hadoop4:7077')
#初始化一个SparkContext对象
sc = SparkContext(conf=conf)
#创建一个空的RDD
rdd = sc.emptyRDD()
rdd.collect()
#关闭SparkContext,释放资源
sc.stop()