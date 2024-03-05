import findspark
findspark.init()

# Importing PYSPARK and the most used libraries
import pyspark
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
conf = pyspark.SparkConf().setMaster("local[*]").setAppName("CountingSheep")
spark_context = SparkSession.builder.config(conf=conf).getOrCreate()
from pyspark.sql.types import StructType, StructField, FloatType, BooleanType
from pyspark.sql.types import DoubleType, IntegerType, StringType
from pyspark import SQLContext
from pyspark.accumulators import AccumulatorParam

from sklearn import preprocessing
import numpy as np
# matplotlib inline
import matplotlib
matplotlib.use('Agg')
import matplotlib.pyplot as plt
import csv
import time
import copy
from math import sqrt, atan, floor, ceil
spark.sparkContext._conf.getAll()
spark.sparkContext.setSystemProperty('spark.executor.memory', '8g')
import sys
spark.sparkContext.setSystemProperty('spark.executor.heartbeatInterval', '30s')
spark.sparkContext.setSystemProperty('spark.network.timeout ', '360s')

from skyline import *
from util import *
from data_gen import *


## Parallel implementations of SKYLINE QUEREIS without grid or angular partitioning
print('Parallel implementations of SKYLINE QUERIES with fair partitioning methods.\nSuch as random partitioning of poitns or sorting and then partitioning points')
def execute_sfs(input_list, dimensions):
    i = 0
    allTuplesList = []
    newTupleAsList = []
    nd  = []
    # creation of tuples could be parallelized by glom, although we do not know the length of the list
    for el_list in input_list:
        for el in el_list:
            if i == dimensions:
                i = 0
                allTuplesList.append( tuple(newTupleAsList) )
                newTupleAsList = []
            newTupleAsList.append(el)
            i = i+1
    allTuplesList.append( tuple(newTupleAsList) )
    localSky = sfs_query_serial(allTuplesList)
    return localSky

def parallel_sfs(dataAsList, numberOfSlices = -1, show = True, sort = True):
    if numberOfSlices <= 0:
        numberOfSlices = min(max(8,  ceil((sys.getsizeof(dataAsList)/1024/1000) * 0.4 ) ), 24)
    print('NumSlices: ' + str(numberOfSlices))
    numColumns = len(dataAsList[0])
    start = time.time()
    initialResult = spark.sparkContext.parallelize(dataAsList, numberOfSlices).sortBy(lambda x: x[0]).mapPartitions(sfs_query)
    initialResult = initialResult.collect()
    end = time.time()-start
    
    print('Length of first pass of parallel sfs is: ' + str(len(initialResult)))
    print('Time taken by the parallel section of SFS: ' + str(end))
    initialResult = [list(x) for x in initialResult ]
    start_serial = time.time()
    finSky = sfs_query_serial(initialResult)
    end_serial = time.time() - start_serial
    print('Length of the skyline: ' + str(len(finSky)))
    print('Time taken by the serial section of sorted filtered Skyline : ' + str(end_serial))
    print('Time taken in total for sorted filtered Skyline execution: ' + str(end_serial+end))
    if show :
        helperVisualFunct(dataAsList)
        helperVisualFunct(finSky,'Parallel sfs skyline')
        plt.show()
        if numColumns > 2 :
            show3D(dataAsList)
            show3D(finSky)
    res = Result(finSky, end+end_serial)
    return res

def parallel_sfs_random(dataAsList, numberOfSlices = -1, show = True, sort = True):
    if numberOfSlices <= 0:
        numberOfSlices = min(max(8,  ceil((sys.getsizeof(dataAsList)/1024/1000) * 0.4 ) ), 24)
    print('NumSlices: ' + str(numberOfSlices))
    numColumns = len(dataAsList[0])
    start = time.time()
    initialResult = spark.sparkContext.parallelize(dataAsList, numberOfSlices).mapPartitions(sfs_query_serial)
    initialResult = initialResult.collect()
    end = time.time()-start
    
    print('Length of first pass of parallel sfs random is: ' + str(len(initialResult)))
    print('Time taken by the parallel section of SFS random: ' + str(end))
    initialResult = [list(x) for x in initialResult ]
    start_serial = time.time()
    finSky = sfs_query_serial(initialResult)
    end_serial = time.time() - start_serial
    print('Length of the skyline: ' + str(len(finSky)))
    print('Time taken by the serial section of SFS random: ' + str(end_serial))
    print('Time taken in total for SFS execution random: ' + str(end_serial+end))
    if show :
        helperVisualFunct(dataAsList)
        helperVisualFunct(finSky,'Parallel sfs skyline')
        plt.show()
        if numColumns > 2 :
            show3D(dataAsList)
            show3D(finSky)
    res = Result(finSky, end+end_serial)
    return res


def parallel_bnl(dataAsList, numberOfSlices = 32, show = True):
    ddbb = dataAsList
    start = time.time()
    weights = [1,1,1,1,1,1,1,1,1]
    initialResult = spark.sparkContext.parallelize(ddbb, numberOfSlices).mapPartitions(bnl_skyline).collect()
    finSky = bnl_skyline(initialResult, weights)
    end = time.time()-start
    print('Length of first pass of parallel BNL is: ' + str(len(initialResult)))
    print('Time taken by the parallel implementation of BNL: ' + str(end))
    if show :
        helperVisualFunct(ddbb)
        helperVisualFunct(finSky,'Parallel sfs skyline')
        plt.show()
        if len(ddbb[0]) > 2 :
            show3D(ddbb)
            show3D(finSky)
    res = Result(finSky, end)
    return res

def execute_bnl(input_list, dimensions, weights):
    i = 0
    allTuplesList = []
    newTupleAsList = []
    # creation of tuples could be parallelized by glom, although we do not know the length of the list
    for el_list in input_list:
        for el in el_list:
            if i == dimensions:
                i = 0
                allTuplesList.append( tuple(newTupleAsList) )
                newTupleAsList = []
            newTupleAsList.append(el)
            i = i+1
    allTuplesList.append( tuple(newTupleAsList) )
    localSky = bnl_skyline(allTuplesList, weights)
    return localSky

# We use DataFrames to create the SQL Query of the skyline
# This query benefits from the catalyst optimizer, although RDDs do not
# This query performs many times better than RDD implementations for lower data cardinalities
def sqlSkyline(dataArray, debug = False):
    numColumns = len(dataArray[0])
    structFieldArray = []
    
    for i in range(numColumns):
        if type(dataArray[0][i]) == int :
            structFieldArray.append(StructField('x'+str(i), IntegerType(),True))
        if type(dataArray[0][i]) == float :
            structFieldArray.append(StructField('x'+str(i), FloatType(),True))
        if type(dataArray[0][i]) == str :
            continue
        
    schema = StructType(structFieldArray)
    p = spark.createDataFrame(dataArray, schema = schema)
    dataArray = []
    if debug == True:
        p.show()
    p.createOrReplaceTempView("Z1")
    condition = ''
    for i in range(numColumns):
        condition = condition + 'p1.x' + str(i) + '<=p.x' + str(i) + ' AND '
    condition = condition + '('
    for i in range(numColumns):
        if i != numColumns -1 :
            condition =condition + 'p1.x' + str(i) + '<p.x' + str(i) + ' OR '
        else : 
            condition = condition + 'p1.x' + str(i) + '<p.x' + str(i) + ') )'
    
    sqlQuery = 'SELECT * FROM Z1 p WHERE NOT EXISTS(SELECT * FROM Z1 p1 WHERE ' + condition
    # The query here is : 
    # SELECT * FROM Z1 p WHERE NOT EXISTS
    #                                    (SELECT * FROM Z1 p1 WHERE p1.x1 <= p.x1 AND p1.x2 <= p.x2 AND etc.. AND
    #                                                     (p1.x1 < p.x1 OR p1.x2 < p.x2 OR etc.. ) ) 
    start = time.time()
    partialRes = spark.sql(sqlQuery)
    points = partialRes.collect()
    end = time.time() - start
    if debug == True:
        print(sqlQuery)
        partialRes.show()
    print('Time taken by the SQL Query: ' + str(end))
    
    p.unpersist()
    spark.catalog.dropTempView("Z1")
    return Result(points, end)

# Angular partitioning
print('Naive angular partitioning')
# calculate index
def getPartitionIndex(datapoint, dimensions, numSlices = 32):
    angleSQ = 0
    for i in range(dimensions):
        angleSQ = angleSQ + datapoint[i]**2
        
    anglesArray = []
    ## first is radius then all angles
    for i in range(dimensions):
        if i == 0:
            # radius
            continue
        else:
            angleSQ = angleSQ - (datapoint[i-1]**2) 
            angleSQ = max(0,angleSQ)
            if datapoint[i-1] == 0:
                value = sqrt(angleSQ) / (0.0001)
            else:
                value = sqrt(angleSQ) /  datapoint[i-1]
        anglesArray.append(value)
    radToDeg = 57.2957795
    piHalf = 1.57
    twoByPi = 0.636619772
    index = 0
    twoByPiNumSlices = twoByPi*numSlices
    for i in range(len(anglesArray)):
        index = index + floor(atan(anglesArray[i])*twoByPiNumSlices) * (numSlices**i)
    return index

#unpack each data by it's first index
def execute_sfs_indexed(input_list, dimensions, weights):
    i = 0
    newList = []
    nd = []
    isFirst = True
    for el_list in input_list:
        for el in el_list:
            if i % 2 == 1:
                ps = el
                nd.append(ps)
            i = i + 1
    nd = sfs_query_serial(nd,[1,1,1,1,1,1,1,1])
    return nd

def execute_sfs_serial(input_list, dimensions, weights):
    i = 0
    allTuplesList = []
    newTupleAsList = []
    nd  = []
    for el_list in input_list:
        for el in el_list:
            if i == dimensions:
                i = 0
                allTuplesList.append( tuple(newTupleAsList) )
                newTupleAsList = []
            newTupleAsList.append(el)
            i = i+1
    allTuplesList.append( tuple(newTupleAsList) )
    localSky = sfs_query_serial(allTuplesList, weights)
    return localSky
    
def parallel_angled_partitioning(dataArray, numSlices = 32):
    print('Angle partitioning: Number of slices : ' + str(numSlices))
    dimensions = len(dataArray[0])
    weights = [1,1,1,1,1,1]
    start = time.time()
    # Partition By divides the dataset by the primary key of each tuple
    m2 = spark.sparkContext.parallelize(dataArray, numSlices) \
                    .map(lambda x : ( getPartitionIndex(x,dimensions,numSlices)  , x) )  \
                    .partitionBy(numSlices**(dimensions-1)) \
                    .mapPartitions(lambda x : execute_sfs_indexed(x, dimensions, weights), preservesPartitioning=True)  \
                    .collect()
    end = time.time()- start
    print('Time taken for parallel section in Angle partitioning: ' + str(end))
    print('Angle partitioning: Length of initial Pass is :' + str(len(m2)))
    seq_time = time.time()
    finRes = sfs_query_serial(m2, weights)
    end_seq = time.time() - seq_time

    print('Angle partitioning: Length of the skyline is :' + str(len(finRes)))
    print('Angle partitioning: Sequential time taken: ' + str(end_seq))
    print('Angle partitioning: Total Time: ' + str(end+end_seq))
    return Result(finRes,end+end_seq)


def sfs_query_with_memory(data, memory, weights = [1,1,1,1,1,1,1,1]):
    values = []
    nd = memory
    stopFirst = min(len(weights), len(memory[0]))
    stopFirstArray = range(stopFirst)
    for ps in data:
        # ps : potentially non dominated point
        add = True
        for other in nd:
            dominated = False
            for k in stopFirstArray:
                if ps[k] < other[k] :
                    dominated = True # other point is dominated
                    break
            if dominated == True:
                continue
            add = False # default case is when
            break
        if add == True:
            nd.append(ps)
    return nd

# Grid partitioning
print('Grid partitioning with a serial grid filtering phase')
class Container:
    def __init__(self, pointFound = [], dataContained = []):
        self.pointFound = pointFound
        self.dataContained = dataContained
        
    def addPoint(self, dataPoint):
        if len(dataPoint) != len(pointFound):
            raise Exception('Datapoint dimension not consistent with container point`s dimensions: ' \
                            + str(len(dataPoint)) + \
                            ' ' + len(pointFound))
        self.dataContained.add(dataPoint)


def normalize_data(data):
    return ( 0.999999*(data - np.min(data))) / (np.max(data) - np.min(data)).tolist()
        
def sfs_query_for_containers(containerList, weights=[1,1,1,1,1,1,1]):
    stopFirst = min(len(weights),len(containerList[0].pointFound))
    nd = []
    stopFirstArray = range(stopFirst)
    for i in range(len(containerList)):
        if not containerList[i].dataContained: #if dataContained array is not empty
            continue
        # ps : potentially non dominated point
        ps = containerList[i].pointFound
        add = True
        for other in nd:
            dominated = False
            for k in stopFirstArray:
                if ps[k] <= other.pointFound[k] :
                    dominated = True
                    break
            if dominated == True:
                continue
            add = False
        if add == True:
            ps = containerList[i]
            nd.append(ps)
    return nd        

# Finds the skyline of grid containers based on its representative point
def query_containers(datapoints, dimensions, numSlicesPerDimension = 32, initialStepLen = -1):
    if initialStepLen == -1:
        initialStepLen = len(datapoints)
    limit = 1 / (numSlicesPerDimension)
    print('iterating - limit: ' +str(limit))
    num_slices_in_space = numSlicesPerDimension**dimensions
    containerList = []
    # create N square containers with each container having the datapoints contained and an index
    range_dim = range(dimensions)
    for i in range(num_slices_in_space): 
        point = []
        for j in range_dim:
            index = floor( i / (numSlicesPerDimension**j) ) % numSlicesPerDimension
            point.insert(j, index * limit + limit)
        containerList.insert(i+1, Container(point, []))
        
    for dp in datapoints:
        index = 0
        for i in range(len(dp)):
            if dp[i] >= 1:
                index = index + floor(0.99999 / limit) * (numSlicesPerDimension**i)
            else:
                index = index + floor(dp[i] / limit) * (numSlicesPerDimension**i)
        (containerList[index].dataContained).append(dp)
    resultingContainers = sfs_query_for_containers(containerList)
    input_list = []
    for container in resultingContainers:
        input_list = input_list + container.dataContained
    if len(input_list) <= initialStepLen * 0.80 :
        # if we discarded 20% of the previous step list's length then we do another step
        input_list = query_containers(normalize_data(input_list), dimensions, numSlicesPerDimension, len(input_list))
    return input_list

# Works for normalized data [0,1) only 
# Grid partitioning but with a serial filtering phase 
def sfs_index_squares(datapoints, dimensions = -1, numSlicesPerDimension = 32):
    if dimensions == -1:
        dimensions = len(datapoints[0])
    if numSlicesPerDimension <= 1:
        if dimensions <= 4:
            numSlicesPerDimension = 12
        else:
            numSlicesPerDimension = 5
    start = time.time()
    input_list = query_containers(datapoints, dimensions, numSlicesPerDimension)
    end = time.time() - start
    print(str(end) + ' for the container serial query')
    print('After first pass list length: ' + str(len(input_list)))
    print('After first pass list Size: ' + str(sys.getsizeof(input_list)/1024) + 'KB')
    numSlices = 12
    print('Num slices has been decided to be: ' + str(numSlices))
    start_parallel = time.time()
    finalResult = parallel_sfs(input_list, numSlices)
    end_parallel = time.time() - start_parallel
    print(str(end_parallel+end) + 's for grid indexed skyline query')
    return Result(finalResult.resultList, end_parallel+end)

# Radius Partitioning
print('Radius partitioning with succesive angular partitioning skyline query')
def sfs_query_serial_with_memory(data, memory, weights = [1,1,1,1,1,1,1,1]):
    values = []
    mergeSort(data)
    nd = memory
    stopFirst = min(len(weights), len(memory[0]))
    stopFirstArray = range(stopFirst)
    for ps in data:
        # ps : potentially non dominated point
        add = True
        for other in nd:
            dominated = False
            for k in stopFirstArray:
                if ps[k] < other[k] :
                    dominated = True # other point is dominated
                    break
            if dominated == True:
                continue
            add = False # default case is when
            break
        if add == True:
            nd.append(ps)
    return nd

def execute_sfs_indexed_with_memory(input_list, memory_list):
    i = 0
    newList = []
    allTuples = []
    isFirst = True
    indexKey = 0
    for el_list in input_list:
        for el in el_list:
            if i % 2 == 1:
                ps = el
                allTuples.append(ps)
            i = i + 1
    nd = sfs_query_serial_with_memory(allTuples, memory_list,[1,1,1,1,1,1,1,1])
    return nd
    
def find_in_radius(input_list, dimensions, beginning = 0.0 ,radius = 0.2):
    angleSQ = 0
    i = 0
    point_radius = 0
    squared_radius = radius * radius
    squared_beginning = beginning * beginning
    newTupleAsList = []
    allTuplesList = []
    for el_list in input_list:
        for el in el_list:
            newTupleAsList.append(el)
            point_radius = point_radius + (el * el)
            i = i+1
            if i == dimensions:
                i = 0
                if point_radius <= squared_radius and squared_beginning <= point_radius: 
                    allTuplesList.append( tuple(newTupleAsList) )
                newTupleAsList = []
                point_radius = 0
    if point_radius <= squared_radius and squared_beginning <= point_radius: 
        allTuplesList.append( tuple(newTupleAsList) )
    allTuplesList = [x for x in allTuplesList if x]
    return allTuplesList
    
def parallel_radius_partitioning(dataArray, radius = 0.5, numSlices = 32):
    dimensions = len(dataArray[0])
    weights = [1,1,1,1,1,1,1]
    start_1 = time.time()
    dataArray = NormalizeData(dataArray).tolist()
    # we get the first few datapoints that have a lot of weight, being inside the unit circle
    m1 = spark.sparkContext.parallelize(dataArray, numSlices) \
                    .mapPartitions(lambda x : find_in_radius(x, dimensions, beginning = 0, radius = radius)) \
                    .collect()
    m1 = spark.sparkContext.parallelize(m1, numSlices) \
                    .sortBy(lambda x: x[0])\
                    .mapPartitions(sfs_query) \
                    .collect()
    # we pass these datapoints to every parallel logic unit so that they can filter skyline points better
    end_1 = time.time() - start_1
    start = time.time()
    m2 = spark.sparkContext.parallelize(dataArray, numSlices) \
                    .mapPartitions(lambda x : find_in_radius(x, dimensions, beginning = radius,radius = 2))  \
                    .map(lambda x : (getPartitionIndex(x,len(x),numSlices),x))  \
                    .partitionBy(numSlices) \
                    .mapPartitions(lambda x : \
                                   execute_sfs_indexed_with_memory(x, m1), preservesPartitioning=True)  \
                    .collect()
    end = time.time() - start
    seq_time = time.time()
    finRes = sfs_query_serial(m2, weights)
    end_seq = time.time() - seq_time

    print('Length of 1st Pass is :' + str(len(m1)))
    print('Length of 2nd Pass is :' + str(len(m2)))
    print('Length of the skyline :' + str(len(finRes)))
    print('Time taken in parallel section 1st pass: ' + str(end_1))
    print('Time taken in parallel section 2nd pass: ' + str(end))
    print('Sequential time taken: ' + str(end_seq))
    print('Total Time: ' + str(end+end_seq+end_1))
    return Result(finRes,end+end_seq+end_1)

print('Helper methods for Representative filtering.')

def execute_sfs_no_sort(input_list, dimensions):
    i = 0
    allTuplesList = []
    newTupleAsList = []
    nd  = []
    # creation of tuples could be parallelized by glom, although we do not know the length of the list
    for el_list in input_list:
        for el in el_list:
            if i == dimensions:
                i = 0
                allTuplesList.append( tuple(newTupleAsList) )
                newTupleAsList = []
            newTupleAsList.append(el)
            i = i+1
    allTuplesList.append( tuple(newTupleAsList) )
    allTuplesList.sort(key=lambda x: x[0])
    localSky = sfs_query(allTuplesList)
    return localSky

# PARALLEL SFS With Representative filtering
print('SFS Query with Representative filtering.')
def parallel_sfs_with_representatives(dataAsList, slicesForSorting = -1, onlyFilter = False, givenReps = [], numberReps = 10):
    if slicesForSorting < 0:
        slicesForSorting = 10
    print('Slices for representative skyline ' + str(slicesForSorting))
    print('Data as list[0] ' + str(len(dataAsList[0])))
    start_indexing = time.time()
    if givenReps == []:
        representatives = spark.sparkContext.parallelize(dataAsList, len(dataAsList[0]) ) \
                                    .mapPartitionsWithIndex(lambda index, y: get_best_representatives(index, y, n = numberReps)) \
                                    .collect()
    else:
        representatives = givenReps
    end_indexing = time.time() - start_indexing
    print('Time taken to find best reps ' + str(end_indexing))
    print('Length of representatives: ' + str(len(representatives)))
    representatives = sfs_query_serial(representatives)
    print('Length of representatives after skyline query: ' + str(len(representatives)))
    start_parallel = time.time()
    parallel_skyline = []
    
    parallel_skyline = spark.sparkContext.parallelize(dataAsList, slicesForSorting)\
                                .mapPartitions(lambda x : filter_with_memory(x, representatives, onlyFilter = True)) \
                                .collect()
    end_parallel = time.time() - start_parallel
    
    print('Time taken to filter: ' +str(end_parallel))
    print('Length of the after filter: ' + str(len(parallel_skyline)))
    start = time.time()
    parallel_skyline.sort(key=lambda x: x[0])
    parallel_skyline = spark.sparkContext.parallelize(parallel_skyline, slicesForSorting)\
                            .mapPartitions(lambda x : execute_sfs_no_sort(x, len(parallel_skyline[0]) )) \
                            .collect()
    
    end = time.time() - start
    
    print('Time taken to find the local skylines: ' +str(end))
    print('Length of parallel skyline: ' + str(len(parallel_skyline)))
    print('Final Serial stage:')
    start_serial = time.time()
    finRes = sfs_query_serial(parallel_skyline)
    end_serial = time.time() - start_serial
    print('~~~~~ Time taken to find the global skyline : ' +str(end_serial))
    print('~~~~~ Length of the skyline: ' + str(len(finRes)))
    print('~~~~~ Total time taken with representatives: ' + str(end_serial+end_parallel+end_indexing+end))
    return Result(finRes,end_serial+end_parallel+end_indexing+end)

# SFS Query with representatives
print('SFS Query with Representative filtering and/or parallel global skyline computation')
def get_best_representatives(index, dataset, n = 10):
    best_n_points = []
    limit = 1 / n
    for i in range(n):
        best_n_points.append((0, []))
    counter = 0
    for point in dataset:
        if index >= len(point):  # Changed '>' to '>=' to include equal to handle valid indices
            return []
        counter += 1
        area_covered = 1
        for element in point:
            area_covered *= (1 - element)
        rep_index = min(floor(point[index] / limit), n - 1)  # Ensure rep_index is within range
        if best_n_points[rep_index][0] < area_covered:
            best_n_points[rep_index] = (area_covered, point)
    best_n_points = [x[1] for x in best_n_points if x[1]]
    return best_n_points

def filter_with_memory(datapoints, reps, onlyFilter = False):
    values = []
    nd = []
    stopFirst = len(reps[0])
    stopFirstArray = range(stopFirst)
    reps = sfs_query_serial(reps)
    for ps in datapoints:
        # ps : potentially non dominated point
        add = True
        for rep in reps:
            dominated = False
            eq_dim = 0
            for k in stopFirstArray:
                if ps[k] < rep[k] :
                    dominated = True # other point is dominated
                    break
                elif ps[k] == rep[k]:
                    eq_dim = eq_dim + 1
            if dominated == True or eq_dim == stopFirst:
                continue
            add = False # default case is when
            break
        if add == True:
            nd.append(ps)
    if onlyFilter:
        return nd
    return sfs_query(nd)


def sfs_multithread(datapoints, global_set):
    values = []
    nd = []
    islist = True
    stopFirst = len(global_set[0])
    #useless 
    stopFirstArray = range(stopFirst)
    datapoints = list(datapoints)
    for ps in datapoints:
        # ps : potentially non dominated point
        add = True
        for other in global_set:
            # dominated other
            dominates = False
            # num of dimensions in which the points are equal
            dimEqual = 0
            for k in stopFirstArray:
                if ps[k] < other[k] :
                    dominates = True
                    break
                elif other[k] == ps[k]:
                    dimEqual = dimEqual + 1
            if dominates == True:
                continue
            # We suppose that the global_set is ordered. 
            # Keeping in mind that the global_set is a superset of our datapoints, if we find our point in the global_set
            # then all other points can not dominated this current point.
            if dimEqual == len(global_set[0]):
                nd.append(ps)
                break
            add = False
            break
    return nd


dataset = get_CSV_Many_SkyPoints(num_col = 2, debug = True)
datapoints = normalize_data(dataset)

# # The filtering phase here finds a few points with a big domination area (usually at most 100 points work well)
# # which then test for domination against all other tuples in the dataset
parallel_sfs_with_representatives(dataset)
# We can use one of the following commented algorithms
# Slice on the first dimension and equally partition all data
parallel_sfs(dataset)
# Execute BNL on all partitions (dataset is randomly partitioned)
parallel_bnl(dataset)

# Naive angle partitioning 
parallel_angled_partitioning(dataset)

# Naive angle partitioning but first we find the local skyline within a radius and passes these data to the angled partitioning method
# with the goal of increasing the accuracy of the dataset
parallel_radius_partitioning(dataset)

####### Slice on the first dimension like parallel_sfs with an added filtering phase before all other computations
# # The filtering phase here groups points into a squares in a grid and all squares that are dominated by a point within another grid, are removed.
# # As such, for lower dimensionalities or for independent datasets, the number of test dominations is lowered substantially.
sfs_index_squares(dataset)