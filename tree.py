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
        self.optype= NodeType.NONE  
   

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
        print(indent +"#" +str(self.optype)+"")
        print(indent+"def "+func+':'+'')
        lines=[]
        if self.params != None:
            for x,v in self.params.items():
                print(indent+ "# "+ str(x) + " "+ str(v)+ "")
        if self.optype==NodeType.SCAN :
            # assume a scan csv file
            
            self.params["filename"]=context.Relations[self.params["relname"]]
            lines.append("with open('{filename}', newline='') as csv_file:")
            lines.append("    csv_reader = csv.DictReader(csv_file, delimiter=',')")
            lines.append("    for row in csv_reader:")
            lines.append("        yield row")
            lines.append("")
        elif self.optype==NodeType.FILTER:
            # assume a function
            print(indent+"def "+func+':'+'\n')
            print(indent+"    return "+str(self.params["func"])+"\n")

        for line in lines:
                print(indent+ line.format(**self.params)+"")    


def get_cond(arg):
#    print(type(arg))
#    print(arg)
#    print(arg['name'])
    cond=dict()
    if arg['@']=='A_Expr':
        cond['op']=arg['name'][0]['val']
        f=arg['lexpr']
        if(f['@']=='ColumnRef'):
            cond['r1']=f['fields'][0]['val']
            cond['f1']=f['fields'][1]['val']
        elif (f['@']=='A_Const'):
            cond['v1']=f['val']['val']

        f=arg['rexpr']
        if(f['@']=='ColumnRef'):
            cond['r2']=f['fields'][0]['val']
            cond['f2']=f['fields'][1]['val']
        elif (f['@']=='A_Const'):
            cond['v2']=f['val']['val']    

        
    return cond

def get_rel_cond(relname, context):
#    print("\n\n\n\n\n "+ relname +" \n\n\n\n")
    conds=[]
    if context != None:
            w=context.whereClause()
            if w != None:
                args=w['args']
                conds=[]
                for arg in args:
                    #print(arg)
                    #lexpr=arg['lexpr']
                    #rexpr=arg['rexpr']
                    #lrelname=None
                    #rrelname=None
                    #if lexpr['@']=='ColumnRef' and lexpr['fields'] !=None and len(lexpr['fields'])>1 : 
                    #    lrelname=(lexpr['fields'])[0]['val']
                    #if  rexpr['@']=='ColumnRef' and rexpr['fields'] !=None and len(rexpr['fields'])>1 : 
                    #    rrelname=(rexpr['fields'])[0]['val']
                    #lcond= lexpr['@']=='ColumnRef' and 'Const' in rexpr['@'] and lrelname==relname
                    #rcond=rexpr['@']=='ColumnRef' and 'Const' in lexpr['@'] and rrelname==relname
                    cond=get_cond(arg)
                    lcond= 'r1' in cond and 'r2' not in cond and cond['r1']==relname
                    rcond= 'r2' in cond and 'r1' not in cond and cond['r2']==relname
                    if lcond:
                        conds.append(cond)
                        #print(arg)
                    elif rcond:
                        
                        cond['r1'],cond['r2'],cond['f1'],cond['f2']=cond['r2'],cond['r1'],cond['f2'],cond['f1']
                        expr=cond['op']
                        if expr=='>':
                            cond['op']='<'
                        elif expr=='<':
                            cond['op']='>'   
                        conds.append(cond)
    return conds

def get_join_cond(l,r,context):
    conds=[]
    if context != None:
            w=context.whereClause()
            if w != None:
                args=w['args']
                conds=[]
                for arg in args:
                    #lexpr=arg['lexpr']
                    #rexpr=arg['rexpr']
                    #lrelname=None
                    #rrelname=None
                    #if lexpr['@']=='ColumnRef' and lexpr['fields'] !=None and  len(lexpr['fields'])>1 : 
                    #    lrelname=(lexpr['fields'])[0]['val']
                    #if rexpr['@']=='ColumnRef' and rexpr['fields'] !=None and  len(rexpr['fields'])>1 : 
                    #    rrelname=(rexpr['fields'])[0]['val']
                    #cond= lexpr['@']=='ColumnRef' and rexpr['@']=='ColumnRef' and ((lrelname in l and rrelname in r) or ((lrelname in r and rrelname in l))) 
                    condition=get_cond(arg)
                    cond1='r1' in condition and 'r2' in condition and condition['r1'] in l and condition['r2'] in r
                    cond2='r2' in condition and 'r1' in condition and condition['r2'] in l and condition['r1'] in r
                    if cond1 or cond2:
                        conds.append(condition)
                        

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
        relname=get_relname(rel)
    #find any  where 
        conds=get_rel_cond(relname,context)
        if conds != None and len(conds) > 0:
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