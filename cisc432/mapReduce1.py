# MapReduce Demo - Helloworld
# Credits: M. S. Rakha, Ph.D., Queen\'s University, CISC/CMPE 432
# HortonWorks SandBox with HDP 2.6.4
# Connect SSH IP Port=2222 (Using putty)
# Switch to root
#yum install python-pip
#pip install google-api-python-client==1.6.4
#pip install mrjob==0.5.11
#yum install nano [editor] optional
#data1: wget http://mohamedsamirakha.info/cisc432/ratings.dat
#MapReduce1 wget http://mohamedsamirakha.info/cisc432/mapReduce1.py
#Run on hadoop
#test if it is working locally: python mapReduce1.py ./ratings.dat
#python mapReduce1.py -r hadoop --hadoop-streaming-jar/usr/hdp/current/hadoop-mapreduce-clienthadoop-streaming.jar rating.dat
from mrjob.job import MRJob 
from mrjob.step import MRStep

class MoviesReviewsCount(MRJob):
    def steps(self):
           return [ 
                MRStep(mapper=self.mapper_get_movies,	
                     reducer=self.reducer_count_reviews)
                  ]
    #Mapping Function
    def mapper_get_ratings(self,_,line):
         (userId, movieId rating, timestamp) = line.split(',')
        yield movieId,1
	#
    def reducer_count_reviews(self, key,values):
        yield key, sum(values)

    if __name__ == '__main__':
         MoviesReviewsCount.run()