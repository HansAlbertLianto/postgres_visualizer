import json
import sqlparse
import sql_finder
import re
from pprint import pprint

def traverseJSON(qepJSON, query):

    # Declare node types
    options = {
        "Seq Scan": sql_finder.process_seq_scan,
        "Index Scan": sql_finder.process_ind_scan,
        "Nested Loop": sql_finder.process_nested_loop,
    }

    # Terminal node
    if 'Plans' not in qepJSON.keys():

        if qepJSON['Node Type'] in options.keys():
            # Process node
            options[qepJSON['Node Type']](qepJSON, query)

        if 'Relation Name' in qepJSON.keys():
            if 'Filter' in qepJSON.keys():
                print("Perform " + qepJSON['Node Type'] + " on " 
                + qepJSON['Relation Name'] + " with filter: " + qepJSON['Filter'] + ".")
            else:
                print("Perform " + qepJSON['Node Type'] + " on " 
                + qepJSON['Relation Name'] + ".")
        else:
            print("Perform " + qepJSON['Node Type'] + ".")
        return

    # Recursive part
    for subplan_data in qepJSON['Plans']:
        traverseJSON(subplan_data, query)

    # Process current node
    if qepJSON['Node Type'] in options.keys():
        # Process node
        options[qepJSON['Node Type']](qepJSON, query)
    
    # Traverse through current node
    if 'Relation Name' in qepJSON.keys():
        if 'Filter' in qepJSON.keys():
            print("Perform " + qepJSON['Node Type'] + " on " 
            + qepJSON['Relation Name'] + " with filter: " + qepJSON['Filter'] + ".")
        else:
            print("Perform " + qepJSON['Node Type'] + " on " 
            + qepJSON['Relation Name'] + ".")
    else:
        print("Perform " + qepJSON['Node Type'] + ".")

def convert(sql_string):
    if (type(sql_string) is not str):
        return
    
    parse_tree = sqlparse.parse(sql_string)

    return parse_tree

with open('testjson.json') as f:
    data = json.load(f)

with open('SQLTestQuery.sql') as g:
    query = g.read()
    g.close()
    query = re.sub(' +', ' ', query.replace("\n", " ").replace("\t", ""))

plan_data = data[0]['Plan']

traverseJSON(plan_data, query)