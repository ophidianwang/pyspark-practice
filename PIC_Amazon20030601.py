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
        edge_proto = line.strip().split("\t")
        edge_proto.append(1.0)
        edge_list.append( tuple( edge_proto ) )
        if n>10000:
            break
        
"""
should I weight vertex on how many edge does it have, and weight edges by the two vertex?
maybe use the concept of pagerank? total node weight set to 1, give each edge weight (1/edge_num_of_node)
If so:
1. parse all edge record and accumulate vertex weight
2. assign edge weight by vertex weight
3. assign to networkx/spark link analysis model
"""

rdd = sc.parallelize(edge_list, 2)  #parallelize data set
parse_done_time = datetime.datetime.now();
print( "spend " + str( parse_done_time.timestamp() - start_time.timestamp() ) + " sec. on parsing and parallelize source file.\n" )

"""
def make_edge_tuple(line):
    edge_proto = [float(x) for x in line.strip().split("\t")]
    edge_proto.append( 1.0 )
    edge_tuple = tuple(edge_proto)
    return edge_tuple
amazon_data = sc.textFile("dataset/Amazon0601.txt")
amazon_similarities = amazon_data.map(make_edge_tuple)
"""

#use pyspark PIC clustering on vertex
"""
# Load and parse the data
data = sc.textFile("data/mllib/pic_data.txt")
similarities = data.map(lambda line: tuple([float(x) for x in line.split(' ')]))
# Cluster the data into two classes using PowerIterationClustering
model = PowerIterationClustering.train(similarities, 2, 10)
"""

model = PowerIterationClustering.train(rdd, 2, 100)

model.assignments().foreach(lambda x: print(str(x.id) + " -> " + str(x.cluster)))

# Save and load model
model.save(sc, "myModelPath")
sameModel = PowerIterationClusteringModel.load(sc, "myModelPath")