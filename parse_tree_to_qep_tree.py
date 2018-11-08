import sqlparse

# setup connection and get query

def convert(sql):
    if (type(sql) is not str):
        return
    
    parse_tree = sqlparse.parse(sql)

    return parse_tree