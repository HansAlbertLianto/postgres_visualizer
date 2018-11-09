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
        "Bitmap Index Scan": sql_finder.process_ind_scan,
        "Bitmap Heap Scan": sql_finder.process_bitmap_heap_scan,
    }

    # Terminal node
    if 'Plans' not in qepJSON.keys():

        print("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

        if qepJSON['Node Type'] in options.keys():
            # Process node
            options[qepJSON['Node Type']](qepJSON, query)

        if 'Relation Name' in qepJSON.keys():
            if 'Filter' in qepJSON.keys():
                print("Performed " + qepJSON['Node Type'] + " on " 
                + qepJSON['Relation Name'] + " with filter: " + qepJSON['Filter'] + ".")
            else:
                print("Performed " + qepJSON['Node Type'] + " on " 
                + qepJSON['Relation Name'] + ".")
        else:
            print("Performed " + qepJSON['Node Type'] + ".")

        return

    # Recursive part
    for subplan_data in qepJSON['Plans']:
        traverseJSON(subplan_data, query)

    # Process current node
    print("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    if qepJSON['Node Type'] in options.keys():
        # Process node
        options[qepJSON['Node Type']](qepJSON, query)
    
    # Traverse through current node
    if 'Relation Name' in qepJSON.keys():
        if 'Filter' in qepJSON.keys():
            print("Performed " + qepJSON['Node Type'] + " on " 
            + qepJSON['Relation Name'] + " with filter: " + qepJSON['Filter'] + ".")
        else:
            print("Performed " + qepJSON['Node Type'] + " on " 
            + qepJSON['Relation Name'] + ".")
    else:
        print("Performed " + qepJSON['Node Type'] + ".")


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

    # Clean query
    query = re.sub(' +', ' ', query.replace("\n", " ").replace("\t", ""))

    parsed_query = convert(query)

plan_data = data[0]['Plan']

traverseJSON(plan_data, query)
