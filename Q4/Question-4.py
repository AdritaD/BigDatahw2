


from pyspark import SparkContext, SparkConf


if __name__ == "__main__":
    conf = SparkConf().setAppName("statistics").setMaster("local")
    sc = SparkContext.getOrCreate(conf=conf)

    # all businesses
    business = sc.textFile("business.csv").map(lambda line: line.split("::")).map(lambda x: [str(x[0]),x[1],x[2]]).map(lambda x: (x[0],(x[1],x[2])))
    #business.coalesce(1).saveAsTextFile("question4/business")

    #average rating
    review = sc.textFile("review.csv").map(lambda line: line.split("::")).map(lambda x: [str(x[2]),x[3]]).map(lambda x: [str(x[0]), float(x[1])])

    avg_Rating = review.combineByKey(lambda value: (value, 1), lambda x, value: (x[0] + value, x[1] + 1),lambda x, y: (x[0] + y[0], x[1] + y[1]))#.filter(lambda x: len(x) == 2)
    average_rating = avg_Rating.map(lambda x: (x[0], x[1][0] / x[1][1]))
    result1 = average_rating.map(lambda x: (x[0], x[1]))
    #result1.coalesce(1).saveAsTextFile("question4/avg-ratings")

    #business with their avg ratings
    join1=business.join(result1).map(lambda x: (x[1][1],(x[0],x[1][0][0],x[1][0][1])))
    sortedResults = join1.sortByKey(ascending=False).distinct()

    #top_10= sortedResults.take(10)

    #sortedResults.coalesce(1).saveAsTextFile("question4/join2")
    top_10 = sortedResults.top(10, key=lambda x: x[0])
    top_10=sc.parallelize(top_10)
    top_10 = top_10.map(lambda x: "{0}\t{1}\t{2}\t{3}".format(x[1][0], x[1][1], x[1][2], x[0]))
    top_10.coalesce(1).saveAsTextFile("question4/top_10")





