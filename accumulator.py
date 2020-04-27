#导入SparkContext,SparkConf
from pyspark import SparkContext,SparkConf
#导入numpy
import numpy as np
#创建一个SparkConf对象
conf = SparkConf()
#设置为本地模式
conf.set('master','spark://hadoop4:7077')
#初始化一个SparkContext对象
sc = SparkContext(conf=conf)
#给累加器赋值初始值0
acc = sc.accumulator(0)
#打印累加器的类型和值
print(type(acc),acc.value)
#调用parallelize方法创建RDD,指定5个分区
rdd = sc.parallelize(np.arange(101),5)
#定义一个函数:将累加器的值加上参数,并返回参数
def acc_add(a):
	acc.add(a)
	return a
#调用map方法接收一个函数,作用于rdd中的每个元素,并返回新的rdd2
rdd2 = rdd.map(acc_add)
#调用Action算子collect方法,将RDD类型的数据转化为数组，同时会从远程集群是拉取数据到driver端
print(rdd2.collect())
#打印累加器的值
print(acc.value)
#关闭SparkContext,释放资源
sc.stop()