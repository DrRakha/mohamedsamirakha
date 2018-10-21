# MapReduce Demo - Helloworld
# Credits: M. S. Rakha, Ph.D., Queen\'s University, CISC/CMPE 432
# HortonWorks SandBox with HDP 2.6.4
# To know the number of cores on your VM. :nproc --all
# Connect SSH IP Port=2222 (Using putty)
# Switch to root : sudo -i
#yum install nano [editor] optional
#yum install python-pip
#pip install google-api-python-client==1.6.4
#pip install mrjob==0.5.11
#data1: wget http://mohamedsamirakha.info/cisc432/mapReduceData.dat
#MapReduce2: wget http://mohamedsamirakha.info/cisc432/mapReduce3.py
#Run on hadoop
#test if it is working locally: python mapReduce1.py ./mapReduceData.dat
#python mapReduce2.py  --hadoop-streaming-jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar -r hadoop hdfs:///user/maria_dev/inputMapReduce/mapReduceData.dat
from mrjob.job import MRJob 
from mrjob.step import MRStep
from datetime import datetime
import sys

# Multi-step jobs
# The map-reduce function here count the number of reviews per each movie, output is sorted by number of reviews
class MoviesReviewsCount(MRJob):
    def steps(self):
           return [ 
                MRStep(mapper=self.mapper_get_movies,	
                     reducer=self.reducer_count_reviews),MRStep(reducer=self.reducer_sortby_print)

                  ]
    #Mapping Function
    def mapper_get_movies(self, _, line):
         (userId, movieId, rating, timestamp) = line.split(',')
         yield movieId,1
	#Reduce Function
    def reducer_count_reviews(self, key, values):
         yield sum(values), key
    ##Output is a string in stdout.. padding zeros 	 
    def reducer_sortby_print(self, ratingSum,  key):
          yield key,ratingSum 

if __name__ == '__main__':
    sys.stderr.write("starting your first MapReduce job \n")
    start_time = datetime.now()
    MoviesReviewsCount.run()
    end_time = datetime.now()
    elapsed_time = end_time - start_time
	#Print the time diff in seconds.
    sys.stderr.write("Total execution time "+str(elapsed_time.seconds)+" Seconds\n")