#!/usr/bin/env python
# coding: utf-8

# In[1]:

import argparse
parser = argparse.ArgumentParser()
parser.add_argument('input', type=str)
parser.add_argument('output', type=str)
parser.add_argument('--date_from', type=str)
parser.add_argument('--date_to', type=str)
parser.add_argument('--time_from', type=int)
parser.add_argument('--time_to', type=int)
parser.add_argument('--num_reducers', type=int)
parser.add_argument('--securities', type=str)
parser.add_argument('--widths', type=str)
parser.add_argument('--shifts', type=str)
parser.add_argument('--parquet_folder', type=str)
args = parser.parse_args()

import pyspark
conf = pyspark.SparkConf()
conf.set('spark.hadoop.validateOutputSpecs', 'false')
if args.num_reducers == None:
    reducers = 8
else:
    reducers = args.num_reducers
conf.set("spark.default.parallelism", reducers)
sc = pyspark.SparkContext(conf=conf)

import shutil
import os
if os.path.exists('parquet_result/parquet_out'):
	shutil.rmtree('parquet_result/parquet_out')

# In[2]:


inp = args.input
outp = args.output
if args.time_from == None:
    timefrom = 1000
else:
    timefrom = args.time_from
if args.time_to == None:
    timeto = 1800
else:
    timeto = args.time_to
if args.date_from == None:
    datefrom = 20110111
else:
    datefrom = args.date_from
if args.date_to == None:
    dateto = 20110112
else:
    dateto = args.date_to
if args.widths == None:
    widths = [1, 5, 10]
else:
    widths = [int(width) for width in args.widths.split(',')]
if args.shifts == None:
    shifts = [0, 1, 2, 3, 4, 5]
else:
    shifts = [int(shift) for shift in args.shifts.split(',')]
if args.securities == None:
    securities = 'SVH1|EDH1'
else:
    securities = args.securities

def mseconds(hours, minutes):
    return hours * 3600 * 1000 + minutes * 60 * 1000
timefrom = mseconds(timefrom // 100, timefrom % 100)
timeto = mseconds(timeto // 100, timeto % 100)


# In[3]:


def cnt_ms(mom):
    hhmmssfff = mom[8:]
    mseconds = 0
    mseconds += int(hhmmssfff[:2]) * 60 * 60 * 1000
    mmssfff = hhmmssfff[2:]
    mseconds += int(mmssfff[:2]) * 60 * 1000
    ssfff = mmssfff[2:]
    mseconds += int(ssfff[:2]) * 1000
    fff = ssfff[2:]
    mseconds += int(fff)
    return mseconds

def cnt_candle(mom, width):
    mseconds = cnt_ms(mom)
    w_mseconds = width * 1000

    return (mseconds - timefrom) // w_mseconds


# In[4]:


import re

def filter1(s):
    rec = s.split(',')
    if rec[0][0] == '#':
        return False
    match = re.match(securities, rec[0])
    if match == None:
        return False
    if match.span()[1] < len(rec[0]):
        return False

    mom = rec[2]
    date = mom[:8]
    if (date < datefrom) or (date > dateto):
        return False

    ms = cnt_ms(mom)
    if (ms < timefrom) or (ms > timeto):
        return False

    return True

lines = sc.textFile(inp)
lines = lines.filter(filter1)


# In[5]:


def map1(s):
    rec = s.split(',')
    mom = rec[2]
    id_deal = int(rec[3])
    res = []
    for width in widths:
        candle = cnt_candle(mom, width)
        price = float(rec[4])
        res.append((rec[0] + '-' + str(candle) + '-' + str(width), (price, mom, id_deal)))
    return res

key_vals = lines.map(map1).flatMap(lambda list: list)


# In[6]:


def reduce1(val1, val2):
    mom1 = val1[1]
    ms1 = cnt_ms(mom1)
    mom2 = val2[1]
    ms2 = cnt_ms(mom2)

    if ms1 > ms2:
        return val1
    if ms1 < ms2:
        return val2

    id_deal1 = val1[2]
    id_deal2 = val2[2]
    return val1 if id_deal1 > id_deal2 else val2

candles = key_vals.reduceByKey(reduce1)

def map2(tup):
    sym, candle, width = tup[0].split('-')
    price = tup[1][0]

    res = []
    flag = False
    for shift in shifts:
        if (shift == 0):
            flag = True
            continue
        res.append((candle + '-' + width + '-' + str(shift), (sym, price, 0)))
        if (int(candle) - shift) < 0:
            continue
        res.append((str(int(candle) - shift) + '-' + width + '-' + str(shift), (sym, price, shift)))
    if flag:
        res.append((candle + '-' + width + '-' + str(0), (sym, price, 0)))

    return res

candles_shifts = candles.map(map2).flatMap(lambda list:list)

def map3(vals_tup):
    vals = vals_tup[1]
    candle = int(vals_tup[0].split('-')[0])
    width = vals_tup[0].split('-')[1]
    key_shift = int(vals_tup[0].split('-')[2])
    res = []

    if (key_shift == 0):
        for i, val1 in enumerate(vals):
        	for val2 in vals[(i + 1):]:

        	    if val1[0] < val2[0]:
        	        sym1, sym2 = val1[0], val2[0]
        	        price1, price2 = val1[1], val2[1]
        	    else:
        	        sym1, sym2 = val2[0], val1[0]
        	        price1, price2 = val2[1], val1[1]

        	    res.append((sym1 + '-' + sym2 + '-' + width + '-0-0', (candle, price1, price2)))
    else:
        shifted = []
        unshifted = []
        for val in vals:
            if val[2] == 0:
                unshifted.append(val)
            else:
                shifted.append(val)

        for i, val1 in enumerate(unshifted):
            for i, val2 in enumerate(shifted):

                if val1[0] == val2[0]:
                    continue

                if val1[0] < val2[0]:
                    sym1, sym2 = val1[0], val2[0]
                    price1, price2 = val1[1], val2[1]
                    shift_str = '0' + '-' + str(val2[2])
                else:
                    sym1, sym2 = val2[0], val1[0]
                    price1, price2 = val2[1], val1[1]
                    shift_str = str(val2[2]) + '-' + '0'

                res.append((sym1 + '-' + sym2 + '-' + width + '-' + shift_str, (candle, price1, price2)))
    return res

candles_shifts = candles_shifts.groupByKey().mapValues(list).map(map3)
candles_shifts = candles_shifts.flatMap(lambda list:list)


from math import sqrt
def map4(vals_tup):
    pair = vals_tup[0]

    vals = sorted(vals_tup[1])
    if len(vals) == 1:
        return (pair, None)

    sum1 = 0
    sum2 = 0
    for i, val in enumerate(vals[:-1]):
        val_next = vals[i + 1]

        growth1 = (val_next[1] - val[1]) / val[1]
        growth2 = (val_next[2] - val[2]) / val[2]
        sum1 += growth1
        sum2 += growth2

        vals[i] = (growth1, growth2)

    mean1 = sum1 / (len(vals) - 1)
    mean2 = sum2 / (len(vals) - 1)

    sum1 = 0
    sum2 = 0
    sum3 = 0
    for val in vals[:-1]:
        tmp1 = val[0] - mean1
        tmp2 = val[1] - mean2

        sum1 += tmp1 * tmp2
        sum2 += tmp1 ** 2
        sum3 += tmp2 ** 2

    if (sum2 * sum3 == 0):
        return (pair, None)
    else:
        return (pair, sum1 / (sqrt(sum2 * sum3)))

candles_shifts = candles_shifts.groupByKey().mapValues(list).map(map4)
candles_shifts = candles_shifts.filter(lambda val: val[1] != None)

def map5(tup):
    pair = tup[0].split('-')[:2]
    width = int(tup[0].split('-')[2])
    shifts = tup[0].split('-')[3:]
    shift = max(int(shifts[0]), int(shifts[1]))
    return [pair[0], pair[1], width, shift, tup[1]]

candles_shifts = candles_shifts.map(map5)
candles = candles_shifts.sortBy(lambda val: -abs(val[4]))

filesystem = sc._jvm.org.apache.hadoop.fs.FileSystem
fs = filesystem.get(sc._jsc.hadoopConfiguration())

if args.parquet_folder != None:
    prefix = args.parquet_folder + '/'
else:
    prefix = 'parquet_result/'
parq_path_str = prefix + '/parquet_out'
parq_path = sc._jvm.org.apache.hadoop.fs.Path(parq_path_str)

if fs.exists(parq_path):
    fs.delete(parq_path, True)

from pyspark.sql import SparkSession
from pyspark.sql.types import *
spark = SparkSession \
    .builder \
    .getOrCreate()

columns = StructType([StructField('sec1',  StringType(),  True),
                      StructField('sec2',  StringType(),  True),
                      StructField('width', IntegerType(), True),
                      StructField('shift', IntegerType(), True),
                      StructField('corr',  DoubleType(),  True)])
if candles.isEmpty():
    df = spark.createDataFrame(sc.emptyRDD(), columns)
else:
    df = candles.toDF(columns)
df = df.repartition(1)
df.write.parquet(parq_path_str)

def map6(lst):
    return ' '.join([lst[0], lst[1], str(lst[2]), str(lst[3]), str(lst[4])])

candles = candles.map(map6)
candles.saveAsTextFile(outp)
