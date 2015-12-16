# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 10:10:02 2015

@author: USER
"""

import matplotlib.pyplot as plt
import datetime

#add pyspark file pathes

from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import PowerIterationClustering, PowerIterationClusteringModel

app_name = "PIC_Amazon_20030601"
master = "spark_url"
source_path = "D:\dataset\Amazon0601.txt"

conf = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=conf)

#load edge
start_time = datetime.datetime.now();
edge_list = []
with open(source_path, "r") as source_file:
    for n,line in enumerate( source_file.readlines() ):
        if line[0] == "#":
            continue
        edge_list.append( tuple( line.strip().split("\t") ) )
        if n>10000:
            break

parse_done_time = datetime.datetime.now();
print( "spend " + str( parse_done_time.timestamp() - start_time.timestamp() ) + " sec. on parsing source file.\n" )

"""
def make_edge_tuple(line):
    edge_list = [float(x) for x in line.strip().split("\t")]
    edge_list.append( float(1) )
    edge_tuple = tuple(edge_list)
    return edge_tuple
amazon_data = sc.textFile("dataset/Amazon0601.txt")
amazon_similarities = amazon_data.map(make_edge_tuple)
"""

#use pyspark PIC clustering on vertex

# Load and parse the data
data = sc.textFile("data/mllib/pic_data.txt")
similarities = data.map(lambda line: tuple([float(x) for x in line.split(' ')]))

# Cluster the data into two classes using PowerIterationClustering
model = PowerIterationClustering.train(similarities, 2, 10)

model.assignments().foreach(lambda x: print(str(x.id) + " -> " + str(x.cluster)))

# Save and load model
model.save(sc, "myModelPath")
sameModel = PowerIterationClusteringModel.load(sc, "myModelPath")