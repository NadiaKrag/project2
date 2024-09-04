
from pyspark import SparkContext, SparkConf
import sys
argv = sys.argv[2]
indir = sys.argv[1]
conf = SparkConf().setMaster("local[{}]".format(argv))
sc = SparkContext(conf=conf)
sc.setLogLevel("ERROR")
import time
total_time_start = time.time()


##################################################
#                 PREPROCESSING                  #
##################################################
rddfile = sc.textFile("../{}/chicago_taxi_trips_2016_*.csv".format(indir))

#Strips header
header = rddfile.first()
rddheader = rddfile.filter(lambda x: x != header)

#Splits lines
rdd2 = rddheader.map(lambda l: l.split(","))
##################################################
#               TOTAL PAYMENTS                   #
##################################################
time_start = time.time()


#Save the Total_trip column into a variable
rdd = rdd2.map(lambda tup: tup[13])

#Removes all empty spots
rdd = rdd.filter(lambda x: x)

#Converts to floats, since that in RDD the values are strings
rdd = rdd.map(lambda x: float(x))

#Calculates the answer
rdd = rdd.reduce(lambda a,b: a+b )

print(" ")
print(" ")
print("Query 1:")
print(rdd)
time_end1 = time.time() - time_start
print('Time for query 1: ', time_end1)


##################################################
#              PAYMENTS - COMPANY                #
##################################################
time_start = time.time()


#Removes all empty spots for index 13(trip_total)
rdd = rdd2.filter(lambda x: x[13])

#Converts to floats, since that in RDD the values are strings
rdd = rdd.map(lambda x: (x[15],float(x[13])))

#Calculates the answer
rdd = rdd.reduceByKey(lambda a,b: a + b)
print(rdd.collect())
time_end2 = time.time() - time_start
print('Time for query 2: ', time_end2)

##################################################
#                PAYMENTS - CASH                 #
##################################################
time_start = time.time()


#Filters every line out that doesn't contain 'Cash' in it
rdd = rddfile.filter(lambda x: 'Cash' in x)

#Splits lines and saves the total_trip tuple
rdd = rdd.map(lambda l: l.split(",")[13])

#Removes all empty spots
rdd = rdd.filter(lambda x: x)

#Converts to floats, since that in RDD the values are strings
rdd = rdd.map(lambda x: float(x))

#Calculates the answer
print(" ")
print("Query 3:")

print(rdd.sum())
time_end3 = time.time() - time_start
print('Time for query 3: ', time_end3)


##################################################
#               TAXI RIDER NAMES                 #
##################################################
time_start = time.time()
#Read in files

drivers = sc.textFile("../other_data/chicago_taxi_drivers.csv")


#Filter out evrything but companies/taxi id's
rdd = rdd2.map(lambda x: (x[0],x[15]))
rdd = rdd.filter(lambda x: x[1] == "11")
rdd = rdd.distinct()

#Splits up the drivers
drivers = drivers.map(lambda line: line.split(','))

#Merges the files
names = rdd.join(drivers)

#Extract the names
names = names.map(lambda x: x[1])
names = names.values()
names = names.collect()

print(" ")
print("Query 4:")

print(names)
time_end4 = time.time() - time_start
print('Time for query 4: ', time_end4)






print(" ")
print(" ")
print(" ")

#time_total = time_end1 + time_end2 + time_end3 + time_end4
time_total = time.time() - total_time_start
print("TOTAL TIME!:   ", time_total)
