# Spark RDD Demo - Helloworld
# Credits: M. S. Rakha, Ph.D., Queen\'s University, CISC/CMPE 432
# HortonWorks SandBox with HDP 2.6.4
#ambari-admin-password-reset [ in case you want to use ambari admin (username=admin, password=you need to add), instead of maria_dev
#data1: wget http://mohamedsamirakha.info/cisc432/mapReduceData.dat
#Spark Demo: wget http://mohamedsamirakha.info/cisc432/Spark_SQL_Demo.py
#Run on hadoop
#spark-submit ./Spark_SQL_Demo.py


#This example count the total number of reviews per movie (item)

from pyspark.sql import SparkSession
from pyspark.sql import Row
from pyspark.sql import functions
 
 

def readInputFile(line):
    data = line.split(",")
	## First field key
    return Row(itemId=int(data[1]))
	
	
	
if __name__ == "__main__":
    #  create   SparkContext
    sc = SparkSession.builder.appName("Cisc432Spark_SQL").getOrCreate()
    # RDD fileLines: read data from HDFS, same as I did in Hadoop mapReduce Demo, you should add yours!
    fileLines = sc.sparkContext.textFile("hdfs:///user/maria_dev/inputMapReduce/mapReduceData.dat")
    # RDD  movieRatings : Convert to (itemId) rows
    movieRatings = fileLines.map(readInputFile)
     # convert moveRatings RDD to a dataFrame
    moviesDataFrame = sc.createDataFrame(movieRatings)
	 # Count the number of reviews, which also equal to the number of movie rows, add it to a new DataFrame called reviewCounts
    reviewCounts = moviesDataFrame.groupBy("itemId").count()
     # Pull the top 10  results 
	 
    limitReviews = reviewCounts.take(10)
	
    for movie in limitReviews:
        print (movie[1], movie[2])
	 
	    # Pull the top 5  results
    limitMovies = reviewCounts.orderBy("count").take(10)
    # Print them out, converting movie ID's to names as we go.
    for movie in limitMovies:
        print (movie[1], movie[2])

    # Stop the session
    sc.stop()
