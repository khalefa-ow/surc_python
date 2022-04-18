from cmath import exp
import re
from sqlite3 import paramstyle
from anytree import NodeMixin, RenderTree

from enum import Enum


class NodeType(Enum): 
    SCAN=1
    FUNCTION=2
    PROJECT=3
    FILTER=4
    JOIN=5
    ORDER=6
    GROUP=7
    LIMIT=8
    CROSSPRODUCT=9
    RESULT=0
    NONE=10

class Op:
    def __init__(self, nd):
        self.type= nd
    def __init__(self):
        self.type= NodeType.NONE  
    def emitcode(self):
        return ""
# and a function to emit code    




class Operator(Op, NodeMixin):  # Add Node feature
    nodeid =0
    def __init__(self, type, params, parent=None, children=None):
        super(Operator, self).__init__()
        self.id = Operator.nodeid
        Operator.nodeid += 1
        self.type=type
        self.params=params
        self.parent = parent
       
        if children:
             self.children = children

def get_rel_cond(relname, context):
    conds=[]
    if context != None:
            w=context.whereClause()
            if w != None:
                args=w['args']
                conds=[]
                for arg in args:
                    lexpr=arg['lexpr']
                    rexpr=arg['rexpr']
                    lrelname=None
                    rrelname=None
                    if len(lexpr['fields'])>1 : 
                        lrelname=(lexpr['fields'])[0]['val']
                    if len(rexpr['fields'])>1 : 
                        rrelname=(rexpr['fields'])[0]['val']
                    lcond= lexpr['@']=='ColumnRef' and rexpr['@']=='Const' and lrelname==relname
                    rcond=rexpr['@']=='ColumnRef' and lexpr['@']=='Const' and rrelname==relname
                    if lcond:
                        conds.append(arg)
                    elif rcond:
                        arg['lexpr'],arg['rexpr']=arg['rexpr'],arg['lexpr']
                        expr=arg['name'][0]['val']
                        if expr=='>':
                            arg['name'][0]['val']='<'
                        elif expr=='<':
                            arg['name'][0]['val']='>'   
                        conds.append(arg)
    return conds

def get_join_cond(l,r,context):
    #l can be a list
    conds=[]
    if context != None:
            w=context.whereClause
            if w != None:
                args=w['args']
                conds=[]
                for arg in args:
                    lexpr=arg['lexpr']
                    rexpr=arg['rexpr']
                    lrelname=None
                    rrelname=None
                    if len(lexpr['fields'])>1 : 
                        lrelname=(lexpr['fields'])[0]['val']
                    if len(rexpr['fields'])>1 : 
                        rrelname=(rexpr['fields'])[0]['val']
                    cond= lexpr['@']=='ColumnRef' and rexpr['@']=='ColumnRef' and ((lrelname in l and rrelname in r) or ((lrelname in r and rrelname in l))) 
                    
                    if cond:
                        conds.append(arg)

    return conds

def get_relname(rel):
    relname=""
    if type(rel).__name__=='RangeVar':
        relname=rel.relname
    return relname    
             
def process_scan(rel, context):
    relname=""
    filter=None
    if type(rel).__name__=='RangeVar':
        relname=rel.relname
        #=rel.relpersistence
        params=dict()
        params['relname']=relname
        scan=Operator(NodeType.SCAN,params)
    #find any  where 
        conds=get_rel_cond(relname,context)
        params['conds']=conds
        filter=Operator(NodeType.FILTER,params,scan)

    if filter==None:
        return scan
    else: 
        return filter

def process_from(fromClause, context):
    #to do:
    # sort relation by the condition
    length=len(fromClause)
    if length==1:
        return process_scan(fromClause[0],context)
    elif length>=2:
        prev=process_scan(fromClause[0],context)
        rel1=get_relname(fromClause[0])
        l=[].append(rel1)
        
        for i in range(1,length):
            scan2=process_scan(fromClause[i],context)
            rel2=get_relname(fromClause[i])
            r=[].append(rel2)
            conds=get_join_cond(l,r,context)
            if len(conds)>0:
                params=[]
                params['conds']=conds
                join=Operator(NodeType.JOIN,params,prev,scan2)
                prev=join
                l.append(rel2)
            else:
                crossproduct=Operator(NodeType.CROSSPRODUCT,None,prev,scan2)
                prev=crossproduct
    return prev       



    

def buildtree(s):
    #convert from
    fclause=s.fromClause
    f=process_from(fclause,s)
    result=Operator(NodeType.RESULT)
    f.parent=result
    #whereClause
    
    #joincondition
    #filtercondition
    #groupClause
    #havingClause
    #sortClause
    #limitOption
    #at each node schema extraction
    return result