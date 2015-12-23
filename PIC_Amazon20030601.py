# -*- coding: utf-8 -*-
"""
Created on Wed Dec 16 10:10:02 2015

@author: USER
"""

#add pyspark file pathes if not using spark-submit

from pyspark import SparkContext, SparkConf
from pyspark.mllib.clustering import PowerIterationClustering, PowerIterationClusteringModel

#start working with spark
app_name = "PIC_Amazon_20030601"
master = "spark_url"
source_path = "/home/ophidian/dataset/Amazon20030601_transform.txt"
my_model_path = "/home/ophidian/pyspark_models"

conf = SparkConf().setAppName(app_name).setMaster(master)
sc = SparkContext(conf=conf)

"""
# example in document
data = sc.textFile("data/mllib/pic_data.txt")
similarities = data.map(lambda line: tuple([float(x) for x in line.split(' ')]))
model = PowerIterationClustering.train(similarities, 2, 10)
"""

#use pyspark PIC clustering on vertex
# Load data
data = sc.textFile(source_path)
weighted_edges = data.map(lambda line: tuple([float(x) for x in line.split(' ')]))
# Cluster the data into 10 classes using PowerIterationClustering
model = PowerIterationClustering.train(weighted_edges, 10, 100)

model.assignments().foreach(lambda x: print(str(x.id) + " -> " + str(x.cluster)))

# Save and load model
model.save(sc, my_model_path)
#sameModel = PowerIterationClusteringModel.load(sc, my_model_path)