# Spark RDD Demo - Helloworld
# Credits: M. S. Rakha, Ph.D., Queen\'s University, CISC/CMPE 432
# HortonWorks SandBox with HDP 2.6.4
#data1: wget http://mohamedsamirakha.info/cisc432/mapReduceData.dat
#Spark Demo: wget http://mohamedsamirakha.info/cisc432/SparkRDD_Demo.py
#Run on hadoop

#spark-submit ./SparkRDD_Demo.py
from pyspark import SparkConf, SparkContext
 

# Take each line of u.data and convert it to (movieID, (rating, 1.0))
# This way we can then add up all the ratings for each movie, and
# the total number of ratings for each movie (which lets us compute the average)
def readInputFile(line):
    data = line.split(",")
    return (int(data[1]), (float(data[2]), 1.0))

if __name__ == "__main__":
    #  create   SparkContext
    conf = SparkConf().setAppName("DemoSparkRDD")
    sc = SparkContext(conf = conf)
 

    # RDD fileLines: read data from HDFS, same as I did in Hadoop mapReduce Demo
    fileLines = sc.textFile("hdfs:///user/maria_dev/inputMapReduce/mapReduceData.dat")

    # RDD  movieRatings : Convert to (movieID, (rating, 1.0))
    movieRatings = fileLines.map(readInputFile)

    # RDD: Reduce to (movieID, (sumOfRatings, totalRatings))
    sumOfCountsandRating = movieRatings.reduceByKey(lambda movie1, movie2: ( movie1[0] + movie2[0], movie1[1] + movie2[1] ) )

    # RDD: Map to (rating, averageRating)
    getAverageRating = sumOfCountsandRating.mapValues(lambda totalAndCount : totalAndCount[0] / totalAndCount[1])

    # RDD: Sort by average rating
    sortedResults = getAverageRating.sortBy(lambda x: -x[1])

    # Take the top 5 results
    sortedResultsLimit10 = sortedResults.take(5)

    # Print them out:
    for result in sortedResultsLimit10:
        print(result[0], result[1])
