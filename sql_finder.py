import json
import re

# Find the start index of substring in SQL string
def find_str(s, char):
    index = 0

    if char in s:
        c = char[0]
        for ch in s:
            if ch == c:
                if s[index:index+len(char)] == char:
                    return index

            index += 1

    return -1

# Cleanup or resolve the filter condition
def cleanup_cond(filter):
    return filter.replace("(", "").replace(")", "").replace("::text", "")

def process_seq_scan(qepJSON, query):
    print("Processing seq scan")
   
    # Search attributes
    start_index = -1
    end_index = -1

    if "Filter" in qepJSON.keys():
        filter_cond = qepJSON["Filter"]
        filter_cond = cleanup_cond(filter_cond)
        print(query.replace("\n", " "))
        print(filter_cond)

        # filter_cond is the SQL fragment predicted. For improvement, filter_cond
        # should be a list of SQL fragments that are compared with the query.
        #
        # Need to predict SQL (that's the algorithm)
        start_index = find_str(query, filter_cond)
        
        if start_index is not -1:
            end_index = start_index + len(filter_cond) - 1

        print("Start index is " + str(start_index) + " and end index is " + str(end_index))
    
    if "Relation Name" in qepJSON.keys():
        relation_name = qepJSON["Relation Name"]

def process_ind_scan(qepJSON, query):
    print("Processing index scan")

    # Search attributes
    start_index = -1
    end_index = -1

    if "Index Cond" in qepJSON.keys():
        filter_cond = qepJSON["Index Cond"]
        filter_cond = cleanup_cond(filter_cond)
        print(query.replace("\n", " "))
        print(filter_cond)

        # filter_cond is the SQL fragment predicted. For improvement, filter_cond
        # should be a list of SQL fragments that are compared with the query.
        #
        # Need to predict SQL (that's the algorithm)
        start_index = find_str(query, filter_cond)
        
        if start_index is not -1:
            end_index = start_index + len(filter_cond) - 1

        print("Start index is " + str(start_index) + " and end index is " + str(end_index))
    
    if "Relation Name" in qepJSON.keys():
        relation_name = qepJSON["Relation Name"]

def process_nested_loop(qepJSON, query):
    print("Processing nested loop")