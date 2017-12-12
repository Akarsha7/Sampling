from pyspark.sql import Row
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.types import *
from  random import randrange
import random
from operator import add
import time
sc = SparkContext()
sqlContext = SQLContext(sc)
time_sampling=''
time_cal_med=''
#f=open("result/report_now.txt","w")
sampling_size=0.02
def reading_file():
	print "reading file "
        ebird_file = sc.textFile("file:///home/abyad001/ds/areawater_small.csv")
        parts = ebird_file.map(lambda l:l.split(","))
        lat = parts.map(lambda p:(float(p[2])))
        longi = parts.map(lambda p:(float(p[3])))
        lat_long = parts.map(lambda p:(float(p[2]),float(p[3])))
        return  lat_long


def k_med(lat_long_rdd):
	flag=0
	k_med_points =4
	priliminary_round_sample = lat_long_rdd.sample(False,sampling_size,None).collect()
	chosen_medoids =  random.sample(priliminary_round_sample,k_med_points)
	print "The type of randomly chosen medoids"+str(chosen_medoids)

if __name__=='__main__':
	lat_long_rdd = reading_file()
	k_med(lat_long_rdd)
