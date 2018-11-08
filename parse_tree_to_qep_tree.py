from pprint import pprint
import sqlparse
import json

# setup connection and get query

def convert(sql_string):
    if (type(sql_string) is not str):
        return
    
    parse_tree = sqlparse.parse(sql_string)

    return parse_tree

def traverseJSON(qepJSON):
    # Terminal node
    if 'Plans' not in qepJSON.keys():
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
        traverseJSON(subplan_data)
    
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

def connect(sql_string, qep_json):
    #TO-DO: connect from QEP to SQL string
    plan_data = json.loads(qep_json)[0]


