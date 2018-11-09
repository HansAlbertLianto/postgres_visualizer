import json
import re
from itertools import islice

# Find index of nth time a value is found in a list.
def nth_index(iterable, value, n):
    matches = (idx for idx, val in enumerate(iterable) if val == value)
    return next(islice(matches, n-1, n), None)

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
    filtered_result = filter.replace("::text", "")
    while ('(' in filtered_result) or (')' in filtered_result):
        filtered_result = re.sub(r'\((.*?)\)', r'\1', filtered_result)

    for aggr in ['avg', 'count', 'min', 'max', 'sum']:
        if aggr in filtered_result:
            filtered_result = filtered_result.replace(aggr, '')

    return filtered_result

def process_seq_scan(qepJSON, query):
    print("Processing seq scan")
   
    # Search attributes
    start_index = -1
    end_index = -1

    sqlfragments = list()

    if "Filter" in qepJSON.keys():
        filter_cond = qepJSON["Filter"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)
        
        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
    
    if "Relation Name" in qepJSON.keys():
        if "Filter" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)

        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    # Find matching SQL
    search_in_sql(sqlfragments, query)

def process_ind_scan(qepJSON, query):
    print("Processing index scan")

    # Search attributes
    start_index = -1
    end_index = -1

    sqlfragments = list()

    if "Index Cond" in qepJSON.keys():
        filter_cond = qepJSON["Index Cond"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' in filter_word:
                if filter_words.index('=') > 0:
                    filter_words[filter_words.index('=') - 1], filter_words[filter_words.index('=') + 1] = filter_words[filter_words.index('=') + 1], filter_words[filter_words.index('=') - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.insert(0, new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
    
    if "Filter" in qepJSON.keys():
        filter_cond = qepJSON["Filter"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")

    if "Relation Name" in qepJSON.keys():
        if "Index Cond" in qepJSON.keys() or "Filter" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)
        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    # Find matching SQL
    search_in_sql(sqlfragments, query)

def process_bitmap_heap_scan(qepJSON, query):
    print("Processing bitmap heap scan")

    # Search attributes
    start_index = -1
    end_index = -1

    sqlfragments = list()

    if "Recheck Cond" in qepJSON.keys():
        filter_cond = qepJSON["Recheck Cond"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
    
    if "Filter" in qepJSON.keys():
        filter_cond = qepJSON["Filter"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)

        # Account for all subqueries
        for subquery_result in re.findall("\$\d+", filter_cond):
            filter_words = filter_cond.split()
            
            for filter_word in filter_words:
                if filter_word == subquery_result:
                    if filter_words.index(filter_word) > 1:
                        sqlfragments.append("WHERE " + filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")
                        sqlfragments.append(filter_words[filter_words.index(filter_word) - 2] + " " + filter_words[filter_words.index(filter_word) - 1] + " (")

    if "Relation Name" in qepJSON.keys():
        if "Recheck Cond" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)
        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    # Find matching SQL
    search_in_sql(sqlfragments, query)

# Process nested loop
def process_nested_loop(qepJSON, query):
    print("Processing nested loop")

    # Search attributes
    start_index = -1
    end_index = -1

    sqlfragments = list()

    if "Join Filter" in qepJSON.keys():
        filter_cond = qepJSON["Join Filter"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)
    
    if "Relation Name" in qepJSON.keys():
        if "Join Filter" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)
        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    if "Plans" in qepJSON.keys():
        all_names_present = True

        for plan in qepJSON["Plans"]:
            if "Relation Name" not in plan.keys():
                all_names_present = False
        
        if all_names_present:
            attributes = list()
            
            for plan in qepJSON["Plans"]:
                attributes.append(plan["Relation Name"])

            sqlfragment = ', '.join(attributes)
            sqlfragments.append(sqlfragment)

        sqlfragments_temp = sqlfragments.copy()

        for sqlfragment in sqlfragments_temp:
            for plan in qepJSON["Plans"]:
                if "Alias" in plan.keys():
                    sqlfragments.append(sqlfragment.replace(plan["Alias"], plan["Relation Name"]))

    # Find matching SQL
    search_in_sql(sqlfragments, query)

# Process merge join
def process_merge_join(qepJSON, query):
    print("Processing merge join")

    # Search attributes
    start_index = -1
    end_index = -1

    sqlfragments = list()

    if "Merge Cond" in qepJSON.keys():
        filter_cond = qepJSON["Merge Cond"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)
    
    if "Relation Name" in qepJSON.keys():
        if "Join Filter" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)
        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    if "Plans" in qepJSON.keys():
        all_names_present = True

        for plan in qepJSON["Plans"]:
            if "Relation Name" not in plan.keys():
                all_names_present = False
        
        if all_names_present:
            attributes = list()
            
            for plan in qepJSON["Plans"]:
                attributes.append(plan["Relation Name"])

            sqlfragment = ', '.join(attributes)
            sqlfragments.append(sqlfragment)

        sqlfragments_temp = sqlfragments.copy()

        for sqlfragment in sqlfragments_temp:
            for plan in qepJSON["Plans"]:
                if "Alias" in plan.keys():
                    sqlfragments.append(sqlfragment.replace(plan["Alias"], plan["Relation Name"]))

    # Find matching SQL
    search_in_sql(sqlfragments, query)

# Process aggregate node
def process_aggregate(qepJSON, query):
    print("Processing aggregate")

    sqlfragments = list()

    if "Group Key" in qepJSON.keys():
        group_key = qepJSON["Group Key"]
        cleaned_key_list = list()
        
        for key in group_key:
            sqlfragments.append(cleanup_cond(key))
    
    # Find matching SQL
    search_in_sql(sqlfragments, query)

# Process hash join
def process_hash_join(qepJSON, query):
    print("Processing hash join")

    # Search attributes
    start_index = -1
    end_index = -1

    sqlfragments = list()

    if "Hash Cond" in qepJSON.keys():
        filter_cond = qepJSON["Hash Cond"]
        filter_cond = cleanup_cond(filter_cond)

        sqlfragments.append(filter_cond)

        filter_words = filter_cond.split()

        for filter_word in filter_words:
            if '=' == filter_word:
                if filter_words.index(filter_word) > 0:
                    filter_words[filter_words.index(filter_word) - 1], filter_words[filter_words.index(filter_word) + 1] = filter_words[filter_words.index(filter_word) + 1], filter_words[filter_words.index(filter_word) - 1]

        new_filter_cond = ' '.join(filter_words)

        sqlfragments.append(new_filter_cond)

        sqlfragments = subquery_block_add(sqlfragments, filter_cond)
    
    if "Relation Name" in qepJSON.keys():
        if "Join Filter" in qepJSON.keys():
            sqlfragments = resolve_relation(sqlfragments, qepJSON)
        relation_name = qepJSON["Relation Name"]
        sqlfragments.append("FROM " + relation_name)

    if "Plans" in qepJSON.keys():
        all_names_present = True

        for plan in qepJSON["Plans"]:
            if "Relation Name" not in plan.keys():
                all_names_present = False
        
        if all_names_present:
            attributes = list()
            
            for plan in qepJSON["Plans"]:
                attributes.append(plan["Relation Name"])

            sqlfragment = ', '.join(attributes)
            sqlfragments.append(sqlfragment)

        sqlfragments_temp = sqlfragments.copy()

        for sqlfragment in sqlfragments_temp:
            for plan in qepJSON["Plans"]:
                if "Alias" in plan.keys():
                    sqlfragments.append(sqlfragment.replace(plan["Alias"], plan["Relation Name"]))

    # Find matching SQL
    search_in_sql(sqlfragments, query)

# Search for corresponding SQL based on SQL fragments.
# Function stops once a match is found
def search_in_sql(sqlfragments, query):

    # Search attributes
    start_index = -1
    end_index = -1

    print("\nSQL Fragments: " + str(sqlfragments) + "\n")
    # print("\n" + query + "\n")

    # search for matching SQL
    for sqlfragment in sqlfragments:
        start_index = find_str(query.lower(), sqlfragment.lower())

        if start_index is not -1:
            end_index = start_index + len(sqlfragment)
            print("Start index is " + str(start_index) + " and end index is " + str(end_index))
            print("Matching SQL is: " + query[start_index : end_index] + "\n")
            break

    if start_index is -1:
        print("Start index is " + str(start_index) + " and end index is " + str(end_index))

# Append relation name to front of attribute in JSON
def resolve_relation(sqlfragments, qepJSON):
    sqlfragments_temp = reversed(sqlfragments.copy())

    for sqlfragment in sqlfragments_temp:
        sqlwords = sqlfragment.split()

        for operator in ['=', '!=', '<', '>', '<>', '>=', '<=', '!<', '!>', 'IS', 'NOT', 'IN', 'LIKE']:

            n = 0

            # Try to append to attributes
            for sqlword in sqlwords:
                if operator == sqlword:

                    n += 1

                    if nth_index(sqlwords, operator, n) > 0 and nth_index(sqlwords, operator, n) < len(sqlwords) - 1:
                        if '.' not in sqlwords[nth_index(sqlwords, operator, n) - 1]:
                            sqlwords[nth_index(sqlwords, operator, n) - 1] = qepJSON["Relation Name"] + "." + sqlwords[nth_index(sqlwords, operator, n) - 1]
                        if '.' not in sqlwords[nth_index(sqlwords, operator, n) + 1] and sqlwords[nth_index(sqlwords, operator, n) + 1].isidentifier():
                            sqlwords[nth_index(sqlwords, operator, n) + 1] = qepJSON["Relation Name"] + "." + sqlwords[nth_index(sqlwords, operator, n) + 1]

        sqlfragment_new = ' '.join(sqlwords)

        # print(sqlfragment_new)

        sqlfragments.insert(0, sqlfragment_new)

    for sqlfragment in sqlfragments:
        if "Alias" in qepJSON.keys():
            sqlfragment = sqlfragment.replace(qepJSON["Alias"], qepJSON["Relation Name"])
        

    
    return sqlfragments

# Taking into account IN and NOT IN tokens
def subquery_block_add(sqlfragments, filter_cond):

    # Parse filter condition to words
    filter_words = filter_cond.split()

    for operator in ['=', '!=', '<>']:

        n = 0

        # Try to append to attributes
        for filter_word in filter_words:
            if operator == filter_word:
                if filter_words.index(filter_word) > 0:
                    if (operator == '='):
                        sqlfragments.insert(0, "WHERE " + filter_words[filter_words.index(filter_word) - 1] + " IN (")
                        sqlfragments.insert(1, filter_words[filter_words.index(filter_word) - 1] + " IN (")
                    elif (operator == '!='):
                        sqlfragments.insert(0, "WHERE " + filter_words[filter_words.index(filter_word) - 1] + " NOT IN (")
                        sqlfragments.insert(1, filter_words[filter_words.index(filter_word) - 1] + " NOT IN (")

    return sqlfragments

# Currently unused
def traverse_parse_tree(parse_tree):
    # Parse tree
    queue = [parse_tree[0]]
    visited = list()
    visited.append(parse_tree[0].value.replace('\n', ' ').replace('\t', ''))
    # print(visited)
    # print()
    while queue:
        node = queue.pop(0)
        if hasattr(node, 'tokens'):
            for child in node.tokens:
                if child not in visited:
                    queue.append(child)
                    visited.append(child.value.replace('\n', ' ').replace('\t', ''))
                    # print(visited)
                    # print()
    
    return visited