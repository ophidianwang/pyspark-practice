# -*- coding: utf-8 -*-
"""
Created on Mon Dec 21 14:02:50 2015

@author: USER
"""

import sys
from pyspark import SparkContext
from pyspark.mllib.fpm import FPGrowth

if __name__ == "__main__":
    """
        PFP practice
    """
    sc = SparkContext(appName="PFP_practice")
    partitions = int(sys.argv[1]) if len(sys.argv) > 1 else 2
    
    """
    data = [["a", "b", "c"], ["a", "b", "d", "e"], ["a", "c", "e"], ["a", "c", "f"]]
    rdd = sc.parallelize(data, 2)
    model = FPGrowth.train(rdd, 0.6, 2)
    sorted(model.freqItemsets().collect())
    [FreqItemset(items=[u'a'], freq=4), FreqItemset(items=[u'c'], freq=3), ...
    """

    data = sc.textFile("data/mllib/sample_fpgrowth.txt")
    transactions = data.map(lambda line: line.strip().split(' '))
    print(type(transactions))
    print(transactions)
    
    model = FPGrowth.train(transactions, minSupport=0.2, numPartitions=10)
    
    result = model.freqItemsets().collect()
    for fi in result:
        print(fi)