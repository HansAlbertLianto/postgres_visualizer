import json
import sqlparse
import sql_finder
import re
from pprint import pprint

def traverseJSON(qepJSON, query):

    # assign JSON to be modified to a new JSON variable
    modifiedJSON = qepJSON

    # Declare node types
    options = {
        "Seq Scan": sql_finder.process_seq_scan,
        "Index Scan": sql_finder.process_ind_scan,
        "Nested Loop": sql_finder.process_nested_loop,
        "Bitmap Index Scan": sql_finder.process_ind_scan,
        "Bitmap Heap Scan": sql_finder.process_bitmap_heap_scan,
        "Merge Join": sql_finder.process_merge_join,
        "Aggregate": sql_finder.process_aggregate,
        "Hash Join": sql_finder.process_hash_join,
        "Sort": sql_finder.process_sort,
        "Index Only Scan": sql_finder.process_index_only_scan,
        "Hash": sql_finder.process_hash,
        "Gather": sql_finder.process_gather,
        "Unique": sql_finder.process_unique,
        "Limit": sql_finder.process_limit,
        "Subquery Scan": sql_finder.process_subquery_scan,
    }

    # Terminal node
    if 'Plans' not in qepJSON.keys():

        print("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")

        if qepJSON['Node Type'] in options.keys():
            # Process node
            modifiedJSON = options[qepJSON['Node Type']](qepJSON, query)

        # For debugging purposes
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
        modifiedJSON = traverseJSON(subplan_data, query)

    # Process current node
    print("--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------")
    if qepJSON['Node Type'] in options.keys():
        # Process node
        modifiedJSON = options[qepJSON['Node Type']](qepJSON, query)
    
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

    return modifiedJSON

def connect_query(json_dir, sql_dir, output_json_dir):
    with open(json_dir) as f:
        data = json.load(f)

    with open(sql_dir) as g:
        query = g.read()
        g.close()

        # Clean query
        query = re.sub(' +', ' ', query.replace("\n", " ").replace("\t", ""))

    plan_data = data[0]['Plan']

    # modified JSON will be put here
    resultJSON = traverseJSON(plan_data, query)
    
    # Encapsulate again in a dictionary and in a list
    finalJSON = [{"Plan": resultJSON}]

    # write to output JSON file
    with open(output_json_dir, 'w') as outfile:
        json.dump(finalJSON, outfile, indent=2)

def main():
    # Set input and output directory of postGreSQL JSON file
    json_dir = 'testjson.json'
    output_json_dir = 'finalJSON.json'

    # Set directory of SQL file
    sql_dir = 'SQLTestQuery.sql'

    connect_query(json_dir, sql_dir, output_json_dir)

if __name__ == '__main__':
    main()