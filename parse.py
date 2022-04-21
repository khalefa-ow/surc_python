#pip3 install anytree
#pip3 install pglast
#pip3 install pprint
from pglast import parse_sql
from pprint import pprint
from anytree import AnyNode, RenderTree

def parse(sql):
    stms=[]
    root = parse_sql(sql)
    for stmt in root:
        #print(stmt)
        stm = stmt.stmt
        #pprint(stm( skip_none=True))
        #print(RenderTree(stmt))
        stms.append(stm)
    return stms    
    