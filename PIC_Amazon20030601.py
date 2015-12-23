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




#start working with spark
app_name = "PIC_Amazon_20030601"
master = "spark_url"
source_path = "D:\dataset\Amazon0601.txt"
conf = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=conf)

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