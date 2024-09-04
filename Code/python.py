from collections import defaultdict
import os
import sys
import csv
import time


def tp_rides():

	time_start = time.time()
	trip_total = 0

	#Q1. Iterative summing trip_total

	for row in data:
		if row[13] != str():
			trip_total += float(row[13])

	print("The sum of the total payments for all taxi rides: ", trip_total)
	print("Time for query 1: ",time.time()-time_start)

def tp_company():

	time_start = time.time()
	trip_total = defaultdict(list)

	#Q2. Append list of trip_total to dafaultdict, where each company is the key

	for i, row in enumerate(data):
		if row[13] != str():
			trip_total[row[15]].append(float(row[13]))

	for k,v in trip_total.items():
		print("The sum of the total payments for " + k + " is ", sum(v))

	print("Time for query 2: ",time.time()-time_start)

def tp_cash():

	time_start = time.time()
	trip_total = 0

	#Q3. Iterative summing trip_total for Cash method

	for row in data:
		if row[14] == "Cash" and row[13] != "":
			trip_total += float(row[13])


	print("The sum of the total payments that were paid in cash: ", trip_total)
	print("Time for query 3: ",time.time()-time_start)

def names11():

	time_start = time.time()
	taxi_id = defaultdict(list)

	#Q4. Append list of names to defaultdict, where each unique taxi id is the key

	for row in data:
		taxi_id[row[15]].append(row[0])

	for row in read:
		if row[0] in taxi_id["11"]:
			print("The names of the taxi drivers who work for company 11: ", row[1])

	print("Time for query 4: ",time.time()-time_start)

if __name__ == "__main__":

	total_time_start = time.time()
	read_file_start = time.time()

	#Loading and preprocessing data, arg1 = the path for the 12 CSV files, arg2 = taxi_drivers names csv

	files = [file for file in os.listdir(sys.argv[1]) if file.endswith(".csv")]
	data = []

	for file in files:
		with open(sys.argv[1] + file, "r") as f:
			reader = csv.reader(f)
			next(reader)
			data.extend(list(reader))
	
	f = open(sys.argv[2], "r")
	read = csv.reader(f)

	read_file_end = time.time() - total_time_start
	print("File time: ", read_file_end)

	#Calling the functions answering the four queries

	tp_rides()
	tp_company()
	tp_cash()
	names11()

	f.close()

	total_time_end = time.time() - total_time_start
	print("Total time end: ", total_time_end)