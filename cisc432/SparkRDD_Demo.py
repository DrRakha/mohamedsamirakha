# Spark RDD Demo - Helloworld
# Credits: M. S. Rakha, Ph.D., Queen\'s University, CISC/CMPE 432
# HortonWorks SandBox with HDP 2.6.4
#ambari-admin-password-reset [ in case you want to use ambari admin (username=admin, password=you need to add), instead of maria_dev
#data1: wget http://mohamedsamirakha.info/cisc432/mapReduceData.dat
#Spark Demo: wget http://mohamedsamirakha.info/cisc432/SparkRDD_Demo.py
#Run on hadoop
#spark-submit ./SparkRDD_Demo.py

from pyspark import SparkConf, SparkContext
 
# The spark map-reduce function here count the number of reviews per each movies
def readInputFile(line):
    data = line.split(",")
	## First field key
    return (int(data[1]), (1.0))

if __name__ == "__main__":
    #  create   SparkContext
    conf = SparkConf().setAppName("Cisc432Spark")
    sc = SparkContext(conf = conf)
 

    # RDD fileLines: read data from HDFS, same as I did in Hadoop mapReduce Demo, you should add yours!
    fileLines = sc.textFile("hdfs:///user/maria_dev/inputMapReduce/mapReduceData.dat")

    # RDD  movieRatings : Convert to (movieID, (rating, 1.0))
    movieRatings = fileLines.map(readInputFile)

    # RDD: Reduce to (movieID, (sumOfRatings))
    sumOfCountsandRating = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[1] + movie2[1]) )

    # RDD: Sort by sum
    sortedResults = sumOfCountsandRating.sortBy(lambda x: -x[1])

    # Take the top 5 results
    sortedResultsLimit10 = sortedResults.take(10)

    # Print them out:
    for result in sortedResultsLimit10:
        print(result[0], result[1])
