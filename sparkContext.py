#导入SparkContext
from pyspark import SparkContext
#导入SparkConf
from pyspark import SparkConf
#创建一个SparkConf对象
conf = SparkConf()
#设置为本地模式
conf.set('master','local')
#初始化一个SparkContext对象
sc = SparkContext(conf=conf)
#调用parallelize方法创建RDD
rdd = sc.parallelize(range(100))
#调用Action算子collect方法,将RDD类型的数据转化为数组，同时会从远程集群是拉取数据到driver端
print(rdd.collect())
#关闭SparkContext,释放资源
sc.stop()