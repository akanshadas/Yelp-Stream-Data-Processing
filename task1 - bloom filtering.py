from pyspark import SparkConf, SparkContext
import sys
import json
import csv
import itertools
import time
import math
import random
import operator
import os
import glob
import re
import binascii

# variables
first_json_path = sys.argv[1]
second_json_path = sys.argv[2]
out_file_path = sys.argv[3]

print("first_json_path:",first_json_path )
print("second_json_path:",second_json_path )
print("out_file_path:",out_file_path )

def BF_hashing(city_integer):
    hashes = set()
    for index in range (1, k):
        current_hash_code = ((65 * index) + (index * city_integer)) % n
        hashes.add(current_hash_code)
    return hashes

def apply_bloom_filter(city_string):
    global bit_array
    city_int = int(binascii.hexlify(city_string.encode('utf8')),16)

    hashes = BF_hashing(city_int) 

    for hash_val in hashes:
        bit_array[hash_val] = 1
    return 0

def test_bloom_filter(city_string):
    global bit_array

    city_int = int(binascii.hexlify(city_string.encode('utf8')),16)

    hashes = BF_hashing(city_int)     
    presentInTrain = 1
    for hash_val in hashes:
        if bit_array[hash_val] == 0:
            presentInTrain = 0
            break

    if presentInTrain == 1:
        return 1
    else:
        return 0

# ======================================================== START ========================================================
start = time.time()

SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[*]', 'task1')
sc.setLogLevel("ERROR")

# n = length of bit array | m = #objects in set | k = #hash functions
trainRDD = sc.textFile(first_json_path).map(json.loads).filter(lambda x: 'city' in x).map(lambda x: x['city']).distinct().persist()

#PAREMETERS
m = trainRDD.count()
k = 5
n = math.ceil((k * m)/0.69314718056)
print ("m (elements in the train data):", m)
print ("n (size of bit array):", n)
print ("k (#hash functions):", k)
bit_array = []
for j in range(n):
    bit_array.append(0)

#Step 1: Training. Finding the final bit array after hashing cities in the business_first.
train_cities = trainRDD.collect()
for c in train_cities:
    if c != "":
        apply_bloom_filter(c)
		
		
#Step 2: Testing. Go through entries in business_second one by one and check whether it is present in the bit-array or not.
result = []
res = []
testRDD = sc.textFile(second_json_path).map(json.loads).collect()

for record in testRDD:
    if "city" in record:
        if record["city"] == "":
            res.append(0)
        else:
            ans = test_bloom_filter(record["city"])
            res.append(ans)
    else:
        res.append(0)

f = open(out_file_path, "w")
line = ""
for num in res:
    line += str(num) + " "
f.write(line)
f.close()
