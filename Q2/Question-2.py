from pyspark.sql import SparkSession
from pyspark.sql import SQLContext
from pyspark import SparkContext, SparkConf
from pyspark.sql.functions import split, explode, col, array

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

    # spark
    conf1 = SparkConf().setMaster("local").setAppName("Q1-1")
    sc = SparkContext(conf=conf1)
    spark = SparkSession(sc)
    sqlContext = SQLContext(sc)
   
    common_friends = sc.textFile("soc-LiveJournal1Adj.txt")
    lines1 = common_friends.map(lambda x: x.split("\t")).filter(lambda x: len(x) == 2).map(lambda x: [x[0], x[1].split(",")])

    common_friends2 =lines1.flatMap(find_mutual)

    common_friends_count = common_friends2.reduceByKey(lambda x,y: x.intersection(y)).map(lambda x:[x[0],len(x[1])])

    top_ten_common_list =common_friends_count.takeOrdered(10, key = lambda x: -x[1])
    temp = sc.parallelize(top_ten_common_list)

    top_ten_pairs = temp.map(lambda x:[x[0].split(","),x[1]]).map(lambda x:[x[0][0],(x[0][1],x[1])])
    #top_ten_pairs.coalesce(1).saveAsTextFile("question2/top10")
    
    
    userdataFile = sc.textFile("userdata.txt")
    user_Data_format = userdataFile.map(lambda x:x.split(",")).map(lambda x:[x[0],(x[1],x[2],x[3])])
    #user_Data_format.coalesce(1).saveAsTextFile("question2/data")

    friends_join_userdata = top_ten_pairs.join(user_Data_format)
    #friends_join_userdata.coalesce(1).saveAsTextFile("question2/output2")
    friends_join_userdata_format = friends_join_userdata.map(lambda x:[x[1][0][0],((x[0],x[1][0][1]),x[1][1])])
    #friends_join_userdata_format.coalesce(1).saveAsTextFile("question2/outputx")
    final_join = friends_join_userdata_format.join(user_Data_format)
    Resultrdd = final_join.map(lambda x:"{0}\t{1}\t{2}\t{3}\t{4}\t{5}\t{6}".format(str(x[1][0][0][1]),x[1][0][1][0],x[1][0][1][1],x[1][0][1][2],x[1][1][0],x[1][1][1],x[1][1][2]))
    Resultrdd.coalesce(1).saveAsTextFile("question2/output1")


    #sql
    pairs_df=top_ten_pairs.toDF()
    #pairs_df.show(1)
    pairs_df = pairs_df.select(pairs_df._1.alias('user_id'), pairs_df._2.alias('data'))
    userdata_df=user_Data_format.toDF()
    #userdata_df.show(1)
    userdata_df = userdata_df.select(userdata_df._1.alias('user_id'), userdata_df._2.alias('data'))
    pairs_df.registerTempTable("pairs_df")
    userdata_df.registerTempTable("userdata_df")
    join1= sqlContext.sql('select pairs_df.user_id as id, pairs_df.data as id2, userdata_df.data as data from pairs_df,userdata_df where pairs_df.user_id==userdata_df.user_id ')

    join1.show()
    join1.registerTempTable("join1")
    joined=sqlContext.sql('select join1.data as data1, userdata_df.data from join1,userdata_df where join1.id==userdata_df.user_id ')
    #joined.write.format('com.databricks.spark.csv').save('question2/joined.csv')
    joined.show()
    
    
