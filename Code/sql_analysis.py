from pyspark.sql import SparkSession, SQLContext, functions as fs
from pyspark.sql.types import DoubleType, IntegerType
import sys
import time

total_time_start = time.time()
argv = sys.argv[2]
indir = sys.argv[1]
spark = SparkSession.builder.appName('taxi_sql').master('local[{}]'.format(argv)).getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

#Loading and preprocessing data
taxi_df = spark.read.csv('/home/group1/{}/*'.format(indir), header = True)
#taxi_df = spark.read.csv('/home/group1/taxi_trips_data/*', header = True, inferSchema = True) #READ IN WITH INFERSCHEMA
taxi_df = taxi_df.withColumn('trip_total', taxi_df['trip_total'].cast(DoubleType())) #REMOVE WHEN READIN WITH INFERSCHEMA
taxi_df.createOrReplaceTempView('taxi_df')
taxi_df = spark.sql('SELECT * FROM taxi_df')
#spark.catalog.cacheTable(taxi_df) #CASHING WORKING IN PYSPARK


drivers_df = spark.read.csv('/home/group1/other_data/chicago_taxi_drivers.csv', header = False)
#drivers_df = spark.read.csv('/home/group1/other_data/chicago_taxi_drivers.csv', header = False, inferSchema = True) #READ IN WITH INFERSCHEMA
drivers_df = drivers_df.toDF('taxi_id', 'Name')
drivers_df.createOrReplaceTempView('drivers_df_temp')
drivers_df = spark.sql("SELECT * FROM drivers_df_temp")

joined_df = taxi_df.join(drivers_df, on = 'taxi_id', how = 'left_outer')

#Q1
time_start = time.time()

total_payments = taxi_df.groupBy().sum('trip_total').rdd.map(lambda x: x[0]).collect()[0]

time_end = time.time() - time_start

print('Total payments: ', total_payments)
print('time, total payments: ', time_end)

#Q2
time_start = time.time()

total_payments_company = set(taxi_df.groupBy('company').sum('trip_total').rdd.map(lambda x: x[0:2]).collect())

time_end = time.time() - time_start
print('Total payments per company: ', total_payments_company)
print('time, Total payments per company: ', time_end)

#Q3
time_start = time.time()

total_payments_cash = taxi_df.where(taxi_df.payment_type == 'Cash').groupBy().sum('trip_total').rdd.map(lambda x: x[0]).collect()[0]

time_end = time.time() - time_start

print('Total payments in cash: ', total_payments_cash)
print('time, Total payments in cash: ', time_end)

#Q4
time_start = time.time()

drivers_11 = joined_df.where(joined_df.company == '11').select(joined_df.Name).distinct()
drivers_11.show()

time_end = time.time() - time_start
print('time, Drivers at company 11: ', time_end)

total_time_end = time.time() - total_time_start
print('TOTAL TIME: ', total_time_end)
