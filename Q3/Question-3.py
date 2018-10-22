import sys, csv, decimal
from pyspark import SparkContext, SparkConf
from pyspark import SparkConf
from operator import add
from pyspark.sql import Row
from pyspark.sql import SparkSession

if __name__ == "__main__":
    conf = SparkConf().setAppName("statistics").setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)
    spark = SparkSession(sc)

    # businesses in Stanford
    business = sc.textFile("business.csv").map(lambda line: line.split("::"))
    business_stanford = business.map(lambda x: [str(x[0]), x[1]]).filter(lambda x: "Stanford" in x[1])
    businesses_in_stanford = business_stanford.map(lambda x:(x[0],(x[1])))

    #businesses_in_stanford.coalesce(1).saveAsTextFile("question_3/stanford_businesses")



    # users that reviewed
    reviewed = sc.textFile("review.csv").map(lambda line: line.split("::")).map(lambda x: (x[2], (x[3],x[1])))

    #reviewed.coalesce(1).saveAsTextFile("question_3/users_who_reviewed")

    #users that reviewed businesses in standford
    joined= businesses_in_stanford.join(reviewed).map(lambda x: (x[1][1][1],x[1][1][0]))
    joined1=joined.map(lambda x:"{0}\t{1}".format(x[0],x[1]))
    joined1.coalesce(1).saveAsTextFile("question3/joined")
    #join_format=joined.map(lambda x: (x[1][0]))
    #final_join= join_format.join(users_that_reviewed)
    #join_format = joined.map(lambda x: (x[0][2], x[1][1], x[1][2]))
    #join_format.coalesce(1).saveAsTextFile("question_3/users_who_reviewed_stanford")







  
