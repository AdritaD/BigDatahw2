from pyspark.sql import SparkSession
from pyspark.sql.functions import split
from pyspark.sql.functions import desc, avg
from pyspark.sql.types import IntegerType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext

import pandas as pd



if __name__ == "__main__":
    conf = SparkConf().setAppName("statistics").setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)


    sqlContext = SQLContext(sc)

    # sql
    business = sc.textFile("business.csv").map(lambda line: line.split("::")).toDF()
    business1 = business.select(business._1.alias('business_id'), business._2.alias('address'),
                               business._3.alias('category'))
    business1.registerTempTable("businesst")
    #business.write.format('com.databricks.spark.csv').save('question4/business.csv')



    review = sc.textFile("review.csv").map(lambda line: line.split("::")).toDF()
    review = review.select(review._1.alias('review_id'), review._2.alias('user_id'), review._3.alias('business_id'),
                           review._4.alias('stars').cast("Int"))

    review1 = review.select('business_id', 'stars')
    review1.registerTempTable("reviewt")
    #review.write.format('com.databricks.spark.csv').save('question4/review.csv')

    #joined = business.join(review, business('business_id') == review('business_id'))
    #joined = joined.select('business_id', 'address', 'category', 'stars')
    # joined.show()
    joined= sqlContext.sql('select reviewt.business_id, first(address), first(category),avg(stars) from businesst,reviewt where businesst.business_id==reviewt.business_id group by reviewt.business_id order by avg(stars) desc').limit(10)
    joined.write.format('com.databricks.spark.csv').save('question4/top-10.csv')
