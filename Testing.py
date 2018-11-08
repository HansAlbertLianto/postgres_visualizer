import json
import sqlparse
from pprint import pprint

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

def convert(sql_string):
    if (type(sql_string) is not str):
        return
    
    parse_tree = sqlparse.parse(sql_string)

    return parse_tree

def traverse_parse_tree(parse_tree):
    # Parse tree
    queue = [parse_tree[0]]
    visited = list()
    visited.append(parse_tree[0].value.replace('\n', ' ').replace('\t', ''))
    print(visited)
    print()

    while queue:
        node = queue.pop(0)

        if hasattr(node, 'tokens'):
            for child in node.tokens:
                if child not in visited:
                    queue.append(child)
                    visited.append(child.value.replace('\n', ' ').replace('\t', ''))
                    print(visited)
                    print()
    
    return visited

with open('testjson.json') as f:
    data = json.load(f)

with open('SQLTestQuery.sql') as g:
    query = g.read()
    g.close()

plan_data = data[0]['Plan']

traverseJSON(plan_data)

parsingResult = convert(query)

print("\n" + query + "\n")

print(traverse_parse_tree(parsingResult))