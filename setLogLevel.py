#导入SparkContext
from pyspark import SparkContext
#导入SparkConf
from pyspark import SparkConf
#创建一个SparkConf对象
conf = SparkConf()
conf.set('master','spark://hadoop4:7077')
#初始化一个SparkContext对象
sc = SparkContext(conf=conf)
#setLogLevel设置日志级别,通过这个设置将会覆盖任何用户自定义的日志等级设置。
#取值有：ALL, DEBUG, ERROR, FATAL, INFO,OFF, TRACE, WARN
sc.setLogLevel('ERROR')
sc.parallelize(range(10),3).map(lambda x:x**2).collect()
#关闭SparkContext,释放资源
sc.stop()

