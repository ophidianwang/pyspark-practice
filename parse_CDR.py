# -*- coding: utf-8 -*-
"""
Created on Mon Dec 21 15:15:29 2015

@author: USER
"""

import os
import json
import pandas as pd

def sortedCsvList(dir_path):
    """
    sort file by time, not the original name with Router ID
    example: CGW11_pgw_processed_01_20151105020743.cdr.gz
    [23:37] are %Y%m%d%H%M%S
    """
    csv_list = []
    ori_csv_list = os.listdir( dir_path )    
    """
    add extra name for testing
    for file_name in ori_csv_list:
        if file_name.find("CGW")==-1:
            break
        clone = file_name
        ori_csv_list.append( clone.replace("CGW11","CGX12") )
    """    
    csv_list = sorted(ori_csv_list, key=lambda file_name: file_name[23:37] )
    return csv_list

def checkSameMsisdn(csv_path_1, csv_path_2):
    """
    scan different Router's record
    exam if one msisdn will be served by more than one router
    example: CGW11_pgw_processed_01_20151105020743.cdr.gz & CGX11_pgw_processed_01_20151105020745.cdr.gz
    """
    data_frame_1 = pd.read_csv(csv_path_1)
    data_frame_2 = pd.read_csv(csv_path_2)
    
    #print( str(len( data_frame_1[:3]) ) )
    
    for row in data_frame_1.iterrows():
        #print(type(row))
        #print(row[1])
        #print(type(row[1]))
        msisdn = row[1]['SERVED_MSISDN']        
        match_rows = data_frame_2[data_frame_2["SERVED_MSISDN"]==msisdn].head(1)
        
        if len(match_rows)>0:
            print(msisdn)
            print("same_msisdn found")
            break
    else:
        print("no dulplicated msisdn between at all")

def intraSequence(data_frame):
    """
    find intra-seq. return map of msisdn to pgw_records
    """
    target_fields = ['SERVED_MSISDN', 'RECORD_OPENING_TIME', 'UPLINK',
                     'DOWNLINK', 'DURATION', 'LAC', 'CI', 'SAC', 'TAC', 'ECI',
                     'SITE_TYPE']
    seq_map = {}    
    grouped = data_frame.loc[:,target_fields].groupby('SERVED_MSISDN')
    for name, group in grouped:
        #print(name)
        #print(group)
        if group.shape[0] == 1:
            continue
        else:
            print(list(group.RECORD_OPENING_TIME))
            continue
    
        record_list = []
        site_name_list = []  #list(group.SITE_NAME)
        county_list = []    #list(group.COUNTY)
        district_list = []  #list(group.DISTRICT)
        record_opening_time_list = []  #list(group.RECORD_OPENING_TIME)
        duration_list = []  #list(group.DURATION)
        for row in group.iterrows():
            #order = row[0]
            """
            msisdn = row[1]['SERVED_MSISDN']
            record_opening_time = row[1]['RECORD_OPENING_TIME']
            uplink = row[1]['UPLINK']
            downlink = row[1]['DOWNLINK']
            duration = row[1]['DURATION']
            one_record = (order, msisdn, record_opening_time, uplink, downlink, duration)
            """
            one_record = tuple(map(lambda field_name:row[1][field_name], target_fields))
            #or make a new data_frame, and return, for inter-seq. group by msisdn?
            record_list.append(one_record)
        seq_map[name] = record_list
    #print(seq_map)
    return seq_map

def interSequence(seq_maps):
    """
    merge intra-seq.s to inter-seq.s on msisdn(map key)
    """
    seq_map = {}
    
    return seq_map
    
def makeSequence(df_list):
    """
    1. find intra-seq. in one source
    2. group intra-seq. to inter-seq.  
    """
    
    seq_map = {}   
    
    return seq_map

if __name__ == "__main__":
    """
    parse CDR data
    make json of msisdn->records
    """
    target_dir = "D:\\Bluetech\\PoCdata\\pgw\\20151105"
    sorted_csv_list = sortedCsvList(target_dir)
    print( sorted_csv_list )
    
    #checkSameMsisdn( target_dir+"\\"+sorted_csv_list[0], target_dir+"\\"+sorted_csv_list[1] )
    
    data_frame = pd.read_csv(target_dir+"\\"+sorted_csv_list[0])
    #print(data_frame[:3])
    intra_seq = intraSequence( data_frame )
