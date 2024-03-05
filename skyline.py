from util import *
import time
from math import sqrt, atan, floor, ceil

### SERIAL FUNCTIONS OF THE SKYLINE QUERIES
def bnl_skyline(data,weights = [1,1,1,1,1,1]):
    result = []
    islist = True
    if isinstance(data,list):
        True    
    else:
        data = list(data)
    stopFirst = min(len(weights), len(data[0]))
    stopFirstArray = range(stopFirst)
    
    data_copy = list(data)
    for ps in data:
        #potential skyline
        insert = True
        for subset in [result,data_copy]:
            for other in subset:
                # if it is dominated but does not dominate the datapoint then it is not a skyline point
                # if it is not dominated ever it is a skyline point
                # if it is dominated and also dominates then it is a tie and it can be a skyline point
                isDominated = False
                dominates = False
                for i in stopFirstArray:
                    if ps[i] < other[i] :
                        dominates = True
                    if ps[i] > other[i] :
                        isDominated = True
                if isDominated and not dominates :
                    insert = False
                    break
            if not insert:
                break
        data_copy.remove(ps)
        if insert:
                result.append(ps)  
    return result 
def nl_skyline(data,weights = [1,1,1,1,1,1]):
    result = []
    islist = True
    if isinstance(data,list):
        True    
    else:
        data = list(data)
    stopFirst = min(len(weights), len(data[0]))
    stopFirstArray = range(stopFirst)
    for ps in data:
        #potential skyline
        insert = True
        for other in data:
            # if it is dominated but does not dominate the datapoint then it is not a skyline point
            # if it is not dominated ever it is a skyline point
            # if it is dominated and also dominates then it is a tie and it can be a skyline point
            isDominated = False
            dominates = False
            for i in stopFirstArray:
                if ps[i] < other[i] :
                    dominates = True
                if ps[i] > other[i] :
                    isDominated = True
            if isDominated and not dominates :
                insert = False
                break
        if insert :
                result.append(ps)
                
    return result 

# SFS QUERY WITH SORTING
def sfs_query_serial(data, weights = [1,1,1,1,1,1,1,1]):
    values = []
    if not isinstance(data,list):
        data = list(data)
    # if data is empty
    if not data:
        return []
    stopFirst = min(len(weights), len(data[0]))
    data.sort()
    nd = []
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

# SFS QUERY WITHOUT SORTING
def sfs_query(data, weights = [1,1,1,1,1,1,1,1]):
    values = []
    nd = []
    islist = True
    stopFirst = 1
    if isinstance(data,map):
        mapValue = next(data)
        islist = False
        stopFirst = min(len(weights), len(mapValue))
        nd.append(mapValue)
    elif data:
        stopFirst = min(len(weights), len(data[0]))

    stopFirstArray = range(stopFirst)
    for ps in data:
        # ps : potentially non dominated point
        # ps = data[i]
        add = True
        for other in nd:
            dominated = False
            for k in stopFirstArray:
                if ps[k] < other[k] :
                    dominated = True
                    break
            if dominated == True:
                continue
            add = False
            break
        if add == True:
            nd.append(ps)
    return nd

# Container Object
class Result:
    def __init__(self, resultList, timeTaken, otherObject = []):
        self.resultList = resultList
        self.timeTaken = timeTaken
        self.otherObject = otherObject

