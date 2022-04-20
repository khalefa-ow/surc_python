from cmath import exp

from sqlite3 import paramstyle
from anytree import NodeMixin, RenderTree, PreOrderIter

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
    
    def __str__(self):
        return str(self.name)+"= "+str(self.value)
 

class Op:
    def __init__(self):
        self.type= NodeType.NONE  
   

class Operator(Op, NodeMixin):  # Add Node feature
    nodeid =0
    def __init__(self, dtype, params, parent=None):
        super(Operator, self).__init__()
        self.id = Operator.nodeid
        Operator.nodeid += 1
        self.optype=dtype
        self.params=params
        self.parent = parent
        self.child=[]

    def __str__(self):
        return "id "+ str(self.id) + "optype "+ str(self.optype) + " "+str(self.params) +  " "+str(self.child)   

    def emitcode(self, n, context):
        func="operator_"+ str(self.id)
        indent='\t'*n
        print(indent +"#" +str(self.optype)+"\n")
        print(indent+"def "+func+':'+'\n')
        
        if self.params != None:
            for x,v in self.params.items():
                print(indent+ " "+ str(x) + " "+ str(v)+ "\n")
        if self.optype==NodeType.SCAN :
            # assume a scan csv file
            lines=[]
            self.params["filename"]=context.Relations[self.params["relname"]]
            lines.append("with open('{filename}', newline='') as csv_file:")

            lines.append("    csv_reader = csv.DictReader(csv_file, delimiter=',')")
            lines.append("    for row in csv_reader:")
            lines.append("        yield row")
	
            #lines.append("\t\tprint(f'Column names are \{\", \".join(row)}\')")
            
            for line in lines:
                print(indent+ line.format(**self.params)+"")

            #assume a scan json file 

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
                    if lexpr['@']=='ColumnRef' and lexpr['fields'] !=None and len(lexpr['fields'])>1 : 
                        lrelname=(lexpr['fields'])[0]['val']
                    if  rexpr['@']=='ColumnRef' and rexpr['fields'] !=None and len(rexpr['fields'])>1 : 
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
                    if lexpr['@']=='ColumnRef' and lexpr['fields'] !=None and  len(lexpr['fields'])>1 : 
                        lrelname=(lexpr['fields'])[0]['val']
                    if rexpr['@']=='ColumnRef' and rexpr['fields'] !=None and  len(rexpr['fields'])>1 : 
                        rrelname=(rexpr['fields'])[0]['val']
                    cond= lexpr['@']=='ColumnRef' and rexpr['@']=='ColumnRef' and ((lrelname in l and rrelname in r) or ((lrelname in r and rrelname in l))) 
                    
                    if cond:
                        conds.append(arg)
                        

    return conds

def get_relname(rel):
    relname=""
    if type(rel).__name__=='RangeVar':
        relname=rel.relname
        if rel.alias != None:
            relname=rel.alias.aliasname
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
        filter=Operator(NodeType.FILTER,params)
        filter.child.append(scan)
        scan.parent=filter

    if filter==None:
        return scan
    else: 
        return filter

def process_from(fromClause, context):
    #to do:
    # sort relation by the condition
    l=[]
    r=[]
    length=len(fromClause)
    if length==1:
        return process_scan(fromClause[0],context)
    elif length>=2:
        prev=process_scan(fromClause[0],context)
        rel1=get_relname(fromClause[0])
        l.append(rel1)
        
        for i in range(1,length):
            scan2=process_scan(fromClause[i],context)
            rel2=get_relname(fromClause[i])
            r.append(rel2)
            conds=get_join_cond(l,r,context)
            if len(conds)>0:
                params=dict()
                params['conds']=conds
                join=Operator(NodeType.JOIN,params)
                join.child.append( prev)
                join.child.append( scan2)
                prev.parent=join
                scan2.parent=join
                prev=join
                l.append(rel2)
            else:
                crossproduct=Operator(NodeType.CROSSPRODUCT,None)
                prev.parent=crossproduct
                scan2.parent=crossproduct
                crossproduct.child.append(prev)
                crossproduct.child.append( scan2)

                prev=crossproduct
    return prev       




    

def buildtree(s):
    #convert from
    fclause=s.fromClause
    f=process_from(fclause,s)
    result=Operator(NodeType.RESULT,None)
    f.parent=result
    result.child.append(f)
    #whereClause
    
    #joincondition
    #filtercondition
    #groupClause
    #havingClause
    #sortClause
    #limitOption
    #at each node schema extraction
    return result