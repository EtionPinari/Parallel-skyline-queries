import matplotlib.pyplot as plt

# Utility functions
def helperVisualFunct(data, title = None, xlabel = None, ylabel = None, marker = '.', label = '', reverseOrder = False, linestyle='Empty', linewidth= 'Empty'):
    plt.axes(projection=None)
    if title != None:
        plt.title(title)
    if xlabel != None:
        plt.xlabel(xlabel)
    if ylabel != None:
        plt.xlabel(ylabel)
    splt = splitXAndY(data)
    if linestyle == 'Empty':
        if not reverseOrder:
            plt.plot(splt[0],splt[1], marker = marker, label = label)
        else: 
            plt.plot(splt[1],splt[0], marker = marker, label = label)
    else:
        if not reverseOrder:
            plt.plot(splt[0],splt[1], marker = marker, label = label, linestyle = linestyle, linewidth = linewidth)
        else: 
            plt.plot(splt[1],splt[0], marker = marker, label = label, linestyle = linestyle, linewidth = linewidth)
    plt.show()

def show3D(data):
    randData = data
    fig = plt.figure()
    ax = plt.axes(projection='3d')
    multiDimensionalData = splitXAndY(randData)
    plt.scatter(multiDimensionalData[0], multiDimensionalData[1], multiDimensionalData[2],marker='x');

def show3DList(dataAsList):
    fig = plt.figure(figsize=(12,10))
    ax = plt.axes(projection='3d')
    first = True
    color_map = plt.get_cmap('Reds')
    for data in dataAsList:
        multiDimensionalData = splitXAndY(data[0:400000])
        color_array = []
        size_array = []
        for i in range(len(multiDimensionalData[0])):
            color_array.append(multiDimensionalData[0][i] + multiDimensionalData[1][i] + multiDimensionalData[2][i]) 

        scatter_plot = plt.scatter(multiDimensionalData[0], multiDimensionalData[1], multiDimensionalData[2],\
                    marker='o',c=color_array,cmap = color_map)

    plt.colorbar(scatter_plot)

    

def mydatabase(dp = 10, randomize = False ):
    
    if (randomize == False and dp <= 10): return [(350,190), (400, 250), (320, 500), (430, 140), (550, 130),
                           (360, 420), (580, 500), (410, 600), (330, 210), (210, 400)]
    # cost and distance
    minimumCost = 350 # [$/week]
    minimumDistance = 147 # [m]    xopt = (minimumCost, minimumDistance) # Optimal choice
    skyline = [(350, 190), (330, 210), (430, 140), (210, 400), (550, 130)]
    allPoints = [(350,190), (400, 250), (320, 500), (430, 140), (550, 130),
                           (360, 420), (580, 500), (410, 600), (330, 210), (210, 400)]
    choice = 0
    for i in range(dp-10):
        if choice == 0:
            x = skyline[i%5][0] + i + 1
            y = skyline[i%5][1] + 1
            choice = 1
        elif choice == 1:
            x = skyline[i%5][0] + i + 1
            y = skyline[i%5][1] + i + 1 
            choice = 2
        else :
            x = skyline[i%5][0] + 1
            y = skyline[i%5][1] + i + 1
            choice = 0
        no = (x,y)
        allPoints.append(no)
    return allPoints

def splitXAndY(db):
    x = [[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[],[]]
    if isinstance(db, list):
        el = len(db)
        for i in range(len(db[0])) :
            for j in range(el) :
                try:
                    x[i].append(db[j][i])
                except:
                    print('Element is: ' + str(el) + ', j is: ' + str(j) + ', i is:' + str(i))
#                 x[i].append(db[i][i])
    else :
        el = int(db.size/db[0].size)
        for i in range(db[0].size) :
            for j in range(el) :
                x[i].append(db[j][i])
    return x  

import numpy as np

def NormalizeData(data):
    return (data - np.min(data)) / (np.max(data) - np.min(data))

def mergeSort(myList):
    if len(myList) > 1:
        mid = len(myList) // 2
        left = myList[:mid]
        right = myList[mid:]

        # Recursive call on each half
        mergeSort(left)
        mergeSort(right)

        # Two iterators for traversing the two halves
        i = 0
        j = 0
        
        # Iterator for the main list
        k = 0
        
        while i < len(left) and j < len(right):
            if left[i] <= right[j]:
                # The value from the left half has been used
                myList[k] = left[i]
                # Move the iterator forward
                i += 1
            else:
                myList[k] = right[j]
                j += 1
            # Move to the next slot
            k += 1

        # For all the remaining values
        while i < len(left):
            myList[k] = left[i]
            i += 1
            k += 1

        while j < len(right):
            myList[k]=right[j]
            j += 1
            k += 1