# -*- coding: utf-8 -*-
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
from datetime import datetime
from pyspark.streaming import StreamingContext
import statistics

# variables
port_num = int(sys.argv[1])
out_file_path = sys.argv[2]
def hashing(city_string):
    return int(binascii.hexlify(city_string.encode('utf8')),16)


def FM_alogrithm(present_stream):

    timeStamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    global batch_number
    print ("\n\nBATCH NUMBER:", batch_number)
    stream_citz = present_stream.collect()   

    max_trailing0s = 0
    max_0s_hash = []
    for i in range (1, n + 1):
        max_trailing0s = 0
        for city in stream_citz:
            city_hash = hashing(city)
            hcode = ((J[i-1] * city_hash) + B[i-1]) % (45*10)
            bz = bin(hcode)
            trailing0s = len(bz[2:]) - len(bz[2:].rstrip('0'))
            if max_trailing0s < trailing0s:
                max_trailing0s = trailing0s
        if max_trailing0s != 0:
            max_0s_hash.append(math.pow(2,max_trailing0s))
        else:
            max_0s_hash.append(0)


    #combining estimates
    group_avgs = []
    for i in range (0, n, 5):
        sumVal = 0
        for j in range (i, i + 5):
            sumVal += max_0s_hash[j]
        group_avg = round(sumVal/5.0)
        group_avgs.append(round(group_avg))

    group_avgs.sort()
    final_R = int(group_avgs[int(9 / 2)])
    batch_number += 1

    print("cities:", len(set(stream_citz)), " | timeStamp", timeStamp)
    f = open(out_file_path, "a")
    line = "\n"
    line += str(timeStamp)+","+str(len(set(stream_citz)))+","+str(final_R)
    f.write(line)
    f.close()
    return 0
# ======================================================== START ========================================================
start = time.time()

SparkContext.setSystemProperty('spark.executor.memory', '4g')
SparkContext.setSystemProperty('spark.driver.memory', '4g')
sc = SparkContext('local[*]', 'task1')
sc.setLogLevel("ERROR")
ssc = StreamingContext(sc, 5)

n = 45                      #number of hashes
batch_number = 1
rowz_in_each_group = 9
J_B = 2
hash_functions= []								
J = []; B = [];
for i in range(0, n):
    for j in range(0, J_B):
        r= random.randint(1,100)
        if j == 0:  J.append(r)
        else:B.append(r)

f = open(out_file_path, "w")
f.write("Time,Ground Truth,Estimation")
f.close()

business_rdd = ssc.socketTextStream("localhost", port_num).map(json.loads).map(lambda x: x['city']).\
    window(30,10)\
    .foreachRDD(FM_alogrithm)

ssc.start()
ssc.awaitTermination()

end = time.time()
print("\n\nDuration:", end - start)