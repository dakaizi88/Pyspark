#导入SparkContext
from pyspark import SparkContext
#导入SparkConf
from pyspark import SparkConf
#创建一个SparkConf对象
conf = SparkConf()
conf.set('master','spark://hadoop4:7077')
#初始化一个SparkContext对象
sc = SparkContext(conf=conf)

#导入os执行Linux命令删除hdfs上已经存在的文件
import os
os.system('hadoop fs -rm -r -f hdfs://myha01/datas/pickles/bbb')

#saveAsPickleFile和pickleFile将RDD保存为python中的pickle压缩文件格式。
#保存在hdfs
sc.parallelize(range(100),3).saveAsPickleFile('/datas/pickles/bbb', 5)
#读取文件排序输出
print(sorted(sc.pickleFile('/datas/pickles/bbb', 3).collect()))
#读取文件排序输出,倒序
print(sorted(sc.pickleFile('/datas/pickles/bbb', 3).collect(),reverse=True))
#关闭SparkContext,释放资源
sc.stop()

