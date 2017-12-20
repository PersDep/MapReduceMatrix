#!/usr/bin/python

import sys
import pandas
import mincemeat

def mappping(c, arr):
    if arr[0] == 'a':
        for elem in range(arr[5]):
            yield (arr[1], elem), (arr[0], arr[2], arr[3])
    else:
        for elem in range(arr[4]):
            yield (elem, arr[2]), (arr[0], arr[1], arr[3])

def reduce(c, arr):
    data = {}
    max = 0
    for elem in arr:
        if elem[1] > max:
            max = elem[1]
        data[tuple([elem[0], elem[1]])] = elem[2]
    summ = 0
    for elem in range(max + 1):
        summ += data[('a', elem)] * data[('b', elem)] % 97
        summ %= 97
    return summ

dataPath = "./data/matrix.csv"  
if len(sys.argv) == 2:
    dataPath = sys.argv[1]

data = pandas.read_csv(dataPath)
matrixA = data[data["matrixName"]=="a"]
sizeA = matrixA['row'][len(matrixA) - 1] + 1
matrixB = data[data["matrixName"]=="b"]
sizeB = matrixB['col'][len(matrixA) + len(matrixB) - 1] + 1

pairs = []
for value in data.values:
    pairs.append(list(value) + [sizeA, sizeB])

server = mincemeat.Server()
server.mapfn = mappping
server.reducefn = reduce
server.datasource = dict(enumerate(pairs))
results = server.run_server(password="changeme")
with open('MultiplicationResult.csv', 'w') as csvFile:
    csvFile.write('matrixName,row,col,val\n')
    for i in range(sizeA):
        for j in range(sizeB):
            csvFile.write('result' + str(i) + ',' + str(j) + ',' + str(results[(i, j)]) + '\n')
