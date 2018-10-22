

from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

from pyspark.sql.functions import split, explode

def find_mutual(row):
    friend1 = row[0].strip()
    friend_list = row[1]
    if(friend1 != ''):
        friend1 = int(friend1)
        mutual_friend_list = []
        for friend2 in friend_list:
            friend2 = friend2.strip()
            if(friend2 != ''):
                value = int(friend2)
                if(int(friend2) < int(friend1)):
                    result = (str(friend2)+","+str(friend1),set(friend_list))
                else:
                    result = (str(friend1)+","+str(friend2),set(friend_list))
                mutual_friend_list.append(result)
        return(mutual_friend_list)

if __name__ == "__main__":

    #spark
    conf1 = SparkConf().setMaster("local").setAppName("Q1")
    sc = SparkContext(conf=conf1)
    spark = SparkSession(sc)

    common_friends = sc.textFile("soc-LiveJournal1Adj.txt")
    lines1 = common_friends.map(lambda x : x.split("\t")).filter(lambda x : len(x) == 2).map(lambda x: [x[0],x[1].split(",")])


    common_friends2 =lines1.flatMap(find_mutual)
    
    common_friends_list = common_friends2.reduceByKey(lambda x,y: x.intersection(y))
   
    mutual_friends_final_list = common_friends_list.map(lambda x:[x[0],",".join(list(x[1]))])
    #print(mutual_friends_final_list.first())
    
    final_result=mutual_friends_final_list.map(lambda x:"{0}\t{1}".format(x[0],x[1]))
    final_result.coalesce(1).saveAsTextFile("question1/output1")



    #sql
   # friendlist = sc.textFile("soc-LiveJournal1Adj.txt").map(lambda x: x.split("\t")).filter(lambda x: len(x) == 2).map(lambda x: [x[0], x[1].split(",")]).toDF()
    # mutual_friends.show()
    #friendlist = friendlist.select(friendlist._1.alias('friend1'), explode(friendlist._2.alias('friend-list')))
    #friendlist.show()


    #friendlist.write.format("com.databricks.spark.csv").option("header", "true").save("question1/output2")


    sql= mutual_friends_final_list.toDF()
    sql=sql.select(sql._1.alias('friend-pair'), sql._2.alias('friends'))
    sql.write.format('com.databricks.spark.csv').save('question1/mutual_friends.csv')
#








