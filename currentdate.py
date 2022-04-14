# -*- coding: utf-8 -*-
"""
Created on Thu Oct 24 22:42:50 2019

@author: prabha
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.functions import to_timestamp, current_timestamp
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, LongType

spark = SparkSession.builder.appName('SparkByExamples.com').getOrCreate()

schema = StructType([
            StructField("seq", StringType(), True)])

dates = [['1']]

df = spark.createDataFrame([list('1')], schema=schema)
df1 = spark.createDataFrame(dates, schema=schema)

df.show()
df1.show()


# createDataFrame()里面传的data是一个rdd模型，具有嵌套的list，相当于[[]]
#单纯的传一个list，会报错TypeError: StructType can not accept object '1' in type <class 'str'>