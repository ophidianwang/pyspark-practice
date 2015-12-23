# -*- coding: utf-8 -*-
"""
Created on Wed Dec 23 09:41:39 2015

@author: USER
"""

import datetime


#Load edge
start_time = datetime.datetime.now();

edge_list = []
vertex_accumulate = {}
edge_limit = 3000

source_path = "/home/ophidian/dataset/Amazon0601.txt"
with open(source_path, "r") as source_file:
    for n,line in enumerate( source_file.readlines() ):
        if line[0] == "#":
            continue
        two_vertex = line.strip().split("\t")
        if two_vertex[0] in vertex_accumulate:
            vertex_accumulate[ two_vertex[0] ] += 1
        else:
            vertex_accumulate[ two_vertex[0] ] = 1
        if two_vertex[1] in vertex_accumulate:
            vertex_accumulate[ two_vertex[1] ] += 1
        else:
            vertex_accumulate[ two_vertex[1] ] = 1
        edge_list.append( tuple( two_vertex ) )
        if (n%1000)==0:
            print("working on " + str(n) + "th edge.")
        if n>edge_limit:
            break
    else:
        print("edge parsing done.")
        
"""
should I weight vertex on how many edge does it have, and weight edges by the two vertex?
maybe use the concept of pagerank? total node weight set to 1, give each edge weight (1/edge_num_of_node)
If so:
1. parse all edge record and accumulate vertex weight
2. assign edge weight by vertex weight
3. assign to networkx/spark link analysis model
"""
vertex_weight = {}
for n,vertex_id in enumerate(vertex_accumulate):
    vertex_weight[ vertex_id ] = 1.0/vertex_accumulate[ vertex_id ]
    if (n%1000)==0:
        print("weighting on " + str(n) + "th vertex.")
else:
    print("all vertexes are weighted")
    #print(vertex_accumulate)
    #print(vertex_weight)

edge_weight_list = []
edge_weight_pool = []
for n,(pro,copro) in enumerate(edge_list):
    edge_weight = (vertex_weight[pro] + vertex_weight[copro])
    edge_weight_list.append( [pro, copro, edge_weight] )
    edge_weight_pool.append( edge_weight )
    if (n%1000)==0:
        print("weighting on " + str(n) + "th edge.")
else:
    print("all edges are weighted")
    #print(edge_weight_list)

parse_done_time = datetime.datetime.now();
print( "spend " + str( parse_done_time.timestamp() - start_time.timestamp() ) + \
    " sec. on parsing source file and weighting edges.\n" )

#start writing to file
print("output start")
transformed_path = "/home/ophidian/dataset/Amazon20030601_transform.txt"
with open(transformed_path,"w") as transformed_file:
    for i, edge in enumerate( edge_weight_list ):
        transformed_file.write( " ".join( map(str,edge) ) + "\n" )
        if (i%1000)==0:
            print("working on " + str(i) + "th edge.")
    else:
        print("all weighted edge output done.")
        