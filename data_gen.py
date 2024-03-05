### Data generation configuration
import random
import enum

# Type of dataset generation
class DataGenEnum(enum.Enum):
    antiCorrelated = 1
    anticorrelated = 1
    Anticorrelated = 1
    AntiCorrelated = 1
    correlated = 2
    Correlated = 2
    Independent = 3
    independent = 3
    
    
class DataGenConfig():
    def __init__(self, typeOfCorrelation = DataGenEnum.independent, 
                 dataRange = [0,1], avg = 0.5, skylinePercentage = 1,
                 numberOfData = 10**6, numberOfDimensions = 4,
                 spreadPercentage = 10): 
        self.typeOfCorrelation = typeOfCorrelation
        self.dataRange = dataRange
        # UNUSED Variable
        self.avg = avg
        self.skylinePercentage = skylinePercentage
        self.numberOfData = numberOfData
        self.numberOfDimensions = numberOfDimensions
        self.spreadPercentage = spreadPercentage
        
    def setCorrelated(self):
            self.typeOfCorrelation = DataGenEnum.correlated
    def setAntiCorrelated(self):
            self.typeOfCorrelation = DataGenEnum.antiCorrelated
    def setIndependent(self):
            self.typeOfCorrelation = DataGenEnum.independent 
            
    def setNumberOfData(self, numData):
        self.numberOfData = numData
    
# Method that  creates the different types of datasets based on the distribution   
def dataGenerator(dataConfiguration = None):
    if dataConfiguration == None :
        dataConfiguration = DataGenConfig()
        
    typeOfCorrelation = dataConfiguration.typeOfCorrelation
    dataRange = dataConfiguration.dataRange
    avg = dataConfiguration.avg
    skylinePercentage = dataConfiguration.skylinePercentage
    numberOfData = dataConfiguration.numberOfData
    numberOfDimensions = dataConfiguration.numberOfDimensions
    spreadPercentage = dataConfiguration.spreadPercentage
    
    minDataValue = dataRange[0]
    maxDataValue = dataRange[1]
    data = []
    if typeOfCorrelation == DataGenEnum.independent:
        for i in range(numberOfData):
            datum = []
            for i in range(numberOfDimensions):
                datum.append(random.random()*(maxDataValue-minDataValue)+minDataValue)
            data.append(datum)
    elif typeOfCorrelation == DataGenEnum.correlated:
        for i in range(numberOfData):
            datum = []
            datum.append(random.random()*(maxDataValue-minDataValue)+minDataValue)
            relatedValue = datum[0]
            spread = spreadPercentage * 0.01
            for i in range(1, numberOfDimensions):
                datum.append(relatedValue + ((random.random()-0.5)*spread) )
            data.append(datum)
    else: #typeOfCorrelation = antiCorrelated
        for i in range(numberOfData):
            datum = []
            datum.append(random.random()*(maxDataValue-minDataValue)+minDataValue)
            relatedValue = maxDataValue-datum[0]
            spread = spreadPercentage * 0.01
            for i in range(1, numberOfDimensions):
                datum.append(relatedValue + (relatedValue*(random.random()-0.5)*spread) )
            data.append(datum)
    return data


def get_CSV_Many_SkyPoints(num_col = 4, debug = False):
    import pandas as pd
    data = pd.read_csv("HT_Sensor_dataset.dat", delimiter= ';')
    # column 0 and 1 are date and time
    col_to_drop = [0, 1, 12, 2,3,4,5,6,7,8,9,10,11]
    if num_col > 10:
        print('Warning: Dataset only contains 10 columns')
        num_col = 10
    if num_col < 2:
        print('Warning: Minimum number of columns in the dataset is 2')
        num_col = 2
    num_col_to_drop = 3 + (10-num_col) 
    ### No duplicates in the data
    data_drop_column = data.drop(data.columns[ col_to_drop[:num_col_to_drop] ], axis=1)
    data_drop_column = data_drop_column.drop_duplicates()
    # Only .92M unique values
    data_drop_column = data_drop_column.apply(pd.to_numeric, errors='coerce')
    data_drop_column = data_drop_column.dropna(axis=0, how='any')
    for column in data_drop_column.columns:
        data_drop_column[column] = pd.to_numeric(data_drop_column[column])
    data_as_list = data_drop_column.values.tolist()
    data_as_list[0:10]
    if debug: 
        print( type(data))
        print(data[:10])
        print(len(data))
        print('data after drop')
        print(data_drop_column[:10])
        type(data_as_list[0][0])
    return data_as_list