from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import desc, avg
from pyspark.sql.types import IntegerType
from pyspark import SparkContext, SparkConf
import pandas as pd
from pyspark.sql import SQLContext


if __name__ == "__main__":
    conf = SparkConf().setAppName("statistics").setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)
    sqlContext = SQLContext(sc)


    # sql
    business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF()
    business = business.select(business._1.alias('business_id'), business._2.alias('address'),
                               business._3.alias('category'))
    business=business.filter(business.address.like('%Stanford%'))
    business.registerTempTable("businesst")
    #business.write.format('com.databricks.spark.csv').save('question3/Stanford_business.csv')

    review = sc.textFile("review.csv").map(lambda line: line.split("::")).toDF()
    review = review.select(review._1.alias('review_id'), review._2.alias('user_id'), review._3.alias('business_id'),
                           review._4.alias('stars').cast("Int"))

    review = review.select('business_id', 'stars','user_id')
    review.registerTempTable("reviewt")
    #review.write.format('com.databricks.spark.csv').save('question3/all_review.csv')

    joined = sqlContext.sql('select user_id, stars from businesst,reviewt where businesst.business_id==reviewt.business_id ')

    joined.write.format('com.databricks.spark.csv').save('question3/joined.csv')
