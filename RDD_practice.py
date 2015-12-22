# -*- coding: utf-8 -*-
"""
Created on Mon Dec 21 14:41:34 2015

@author: USER
"""

from pyspark import SparkContext

if __name__ == "__main__":
    """
        RDD practice
    """
    sc = SparkContext(appName="RDD_practice")

    seqOp = (lambda x, y: (x[0] + y, x[1] + 1))
    combOp = (lambda x, y: (x[0] + y[0], x[1] + y[1]))
    first_result = sc.parallelize([1, 2, 3, 4]).aggregate((0, 0), seqOp, combOp)
    #(10, 4)
    second_result = sc.parallelize([]).aggregate((0, 0), seqOp, combOp)
    #(0, 0)