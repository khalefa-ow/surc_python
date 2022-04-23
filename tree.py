
import re
from anytree import NodeMixin, RenderTree, PreOrderIter
from enum import Enum

class NodeType(Enum): 
    SCAN=1
    API_endpoint=2
    CACHE=3
    RETRY=4
    PROJECT=5
    SELECTION=6
    JOIN=7
    ORDER=8
    GROUP=9
    LIMIT=10
    CROSSPRODUCT=11
    RESULT=0
    NONE=12
    
    def __str__(self):
        return str(self.name)
 

class Op:
    def __init__(self):
        self.optype= NodeType.NONE  

def isfloat(element):
    try:
        float(element)
        return True
    except ValueError:
        return False
def isint(element):
    try:
        int(element)
        return True
    except ValueError:
        return False        

def is_str_const(s):
    if (isint(s) or isfloat(s)) : 
        return False
    return True    


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
  
    def gen_cond(self, cond,relname):    
        left=False
        right=False

        if 'r1' in cond:
            r1=cond['r1']
            f1=cond['f1']
            left=True
        else:
            v1=cond['v1']   
            if(is_str_const(v1)):
                v1="'"+v1+"'"
        
        if 'r2' in cond:
            r2=cond['r2']
            f2=cond['f2']
            right=True
        else:
           v2=cond['v2']
           if(is_str_const(v2)):
            v2="'"+v2+"'"
        
        op=cond['op']
        if(op=='='):
            op='=='

        txt=""    
        if left and right:
            txt="'"+f1+"' in "+relname +" and " + "'"+f2+"' in "+relname +" and "+ relname+"['"+f1+"']" +op+relname+"['"+f2+"']  "
        elif left:
            txt="'"+f1+"' in "+relname +" and "  +relname+"['"+f1+"']" +op+str(v2)+"  "
        elif right:
            txt="'"+f2+"' in "+relname +" and "  +str(v1)+op+relname+"['"+f2+"']  "        
        
        txt="("+txt+")"
        return txt  
     
    def emitcode(self, n, context, outfile):
        func="operator_"+ str(self.id)
        indent='\t'*n
        lines=[]
        #print("#" +str(self.optype)+"")
        #print("def "+func+'():'+'')

        lines.append("def "+func+'():'+'')

        #if self.params != None:
            #print("self.params="+ str(self.params))
            #for x,v in self.params.items():
                #print( "# "+ str(x) + " "+ str(v)+ "")
        if self.optype==NodeType.SCAN :
            # assume a scan csv file
            tmp=context.schema[self.params["relname"]]
            scan_type=tmp[0]
            if(scan_type=="csvfile"):
                self.params["filename"]=tmp[1]
                lines.append("\twith open('{filename}', newline='') as csv_file:")
                lines.append("\t\tcsv_reader = csv.DictReader(csv_file, delimiter=',')")
                lines.append("\t\tfor row in csv_reader:")
                lines.append("\t\t\tyield row")
                lines.append("")
            elif (scan_type=="jsonfile"):
                self.params["filename"]=tmp[1]
                if len(tmp)>=2:
                    self.params["objects"]=tmp[2]
                else:
                    self.params["objects"]=None
                if len(tmp)>=3:
                    self.params["flatten"]=tmp[3]
                else:
                     self.params["flatten"]=False

                lines.append("\twith open('{filename}', newline='') as json_file:")
                lines.append("\t\tjson_data = json.load(json_file)")
                if self.params["objects"]!=None:
                    lines.append("\t\t\tfor x in row['{objects}']:")   
                else:
                    lines.append("\t\tfor row in json_data:")
                if self.params["flatten"]:
                    lines.append("\t\t\tyield flatten(x)")
                else:
                    lines.append("\t\t\t\tyield row")
         
                lines.append("")
          ## add json scan here  
        elif self.optype==NodeType.SELECTION:
            scan_func="operator_"+str(self.children[0].id)+"()"
            #relname=self.params["relname"]
            lines.append("\tfor row" +   " in "+ scan_func+":")
            lines.append ("\t\t"+"condition = True")
            for cond in self.params["conds"]:
                if cond == None: 
                        continue
                lines.append("\t\tcondition= condition and " +self.gen_cond(cond,"row"))
            lines.append("\t\tif  condition :" )
            lines.append("\t\t\tyield " + "row")
            lines.append("")
        elif self.optype==NodeType.CROSSPRODUCT:
            #Read one input entiry to a list
            #for each input fomr the seoncd input, retiterate throught the list
            i1="operator_"+str(self.children[0].id)+"()"
            i2="operator_"+str(self.children[1].id)+"()"
            lines.append("\tinput1=[]")
            lines.append("\tfor row in "+i1+":")
            lines.append("\t\tinput1.append(row)")
            lines.append("")
            lines.append("\tfor row2 in "+i2+":")
            lines.append("\t\tfor row1 in input1:")
            lines.append("\t\t\tr=row2.copy()")
            lines.append("\t\t\tr.update(row1)")
            lines.append("\t\t\tyield r")
            lines.append("")
        elif self.optype==NodeType.JOIN:
            lines.append("\t#build a hash table")
            i1="operator_"+str(self.children[0].id)+"()"
            i2="operator_"+str(self.children[1].id)+"()"
            lines.append("\tinput1=dict()")
            lines.append("\tfor row in "+i1+":")
            cond=self.params["conds"][0]
            lines.append("\t\tv1=get_value(row['"+cond["f1"]+"'])")
            lines.append("\t\tif v1 not in input1:")
            lines.append("\t\t\tinput1[v1]=[]")
            lines.append("\t\tinput1[v1].append(row)")
            lines.append("")
            lines.append("\tfor row2 in "+i2+":")
            lines.append("\t\tv2=get_value(row2['"+cond["f2"]+"'])")
            lines.append("\t\tif v2 in input1:")
            lines.append("\t\t\tl1=input1[v2]")
            lines.append("\t\t\tfor v in l1:")
            lines.append("\t\t\t\tr=row2.copy()")
            lines.append("\t\t\t\tr.update(v)")
            lines.append("\t\t\t\tyield r")
            lines.append("")
        elif self.optype==NodeType.RESULT:
            lines.append("\ti1=operator_"+str(self.children[0].id)+"()")
            lines.append("\tfor row in i1:")
            lines.append("\t\tyield(row)")
            lines.append("")

        if self.params == None:
            for line in lines:
                outfile.write(indent+ line+"\n")    
        else:
            for line in lines:
               outfile.write(indent+ line.format(**self.params)+"\n")    


def get_cond(arg):
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

def process_arg(arg, relname):
    cond=get_cond(arg)
    lcond= 'r1' in cond and 'r2' not in cond and cond['r1']==relname
    rcond= 'r2' in cond and 'r1' not in cond and cond['r2']==relname
    if lcond:
        return cond
    elif rcond:
        cond['r1'],cond['r2'],cond['f1'],cond['f2']=cond['r2'],cond['r1'],cond['f2'],cond['f1']
        expr=cond['op']
        if expr=='>':
            cond['op']='<'
        elif expr=='<':
            cond['op']='>'   
        return cond

def get_rel_cond(relname, context):
    conds=[]
    if context != None:
            w=context.whereClause
            if w != None:
                if 'args' not in w():
                    conds.append(process_arg(w(),relname))
                else:
                    for arg in w()['args']:
                        conds.append(process_arg(arg,relname))
    return conds

def get_conds(context):
    conds=[]
    if context != None:
        if 'whereClause' in context:
            w=context.whereClause

            if w != None:
                if 'args' not in w():
                    conds.append(get_cond(w()))
                else:
                    for arg in w()['args']:
                        conds.append(get_cond(arg))
    
    return conds


def process_arg_2(arg,l,r):
    condition=get_cond(arg)
    #print("#condition"+str(condition))
    cond1='r1' in condition and 'r2' in condition and condition['r1'] in l and condition['r2'] in r
    if cond1:
        return condition
    cond2='r2' in condition and 'r1' in condition and condition['r2'] in l and condition['r1'] in r
    if  cond2:
        condition['r1'],condition['r2'],condition['f1'],condition['f2']=condition['r2'],condition['r1'],condition['f2'],condition['f1']
        return condition
    return None    

def get_join_cond(l,r,context):
    conds=[]
    if context != None:
            w=context.whereClause
            if w != None:
                if 'args' not in w:
                    tmp=process_arg_2(w(),l,r)
                    if tmp != None:
                        conds.append(tmp)
                else:
                    for arg in w()['args']:
                        tmp=process_arg_2(arg,l,r)
                        if tmp != None:
                            conds.append(tmp)
                        
    return conds

def get_relname(rel):
    relname=""
    if type(rel).__name__=='RangeVar':
        relname=rel.relname
        if rel.alias != None:
            relname=rel.alias.aliasname
    return relname 
             
def process_scan(rel, qcontext,dcontext, optimize=True):
    relname=""
    filter=None
    if type(rel).__name__=='RangeVar':
        relname=rel.relname
        tmp=dcontext.schema[relname]
        if tmp[0]=='csvfile' or tmp[0]=='jsonfile':
            params=dict()
            params['relname']=relname
            scan=Operator(NodeType.SCAN,params)
            relname=get_relname(rel) #for alias
        if tmp[0]=="api":
            params=dict()
            params['api']=relname
            api=Operator(NodeType.API_endpoint ,params)
            scan=api
            relname=get_relname(rel)    #for alias 
            if tmp[2]=='retry':
                retry=Operator(NodeType.RETRY,params)
                scan.parent=retry
                retry.child.append(scan)
                scan=retry
            if tmp[1]=='cache':
                cache=Operator(NodeType.CACHE,params)
                scan.parent=cache
                cache.child.append(scan)
                scan=cache
               

    #find any  where 
    if optimize == True:
        conds=get_rel_cond(relname,qcontext)
        if conds != None and len(conds) > 0:
            fparams=dict()
            fparams['conds']=conds
            fparams['relname']=relname # this is needed to get the correct table name with alias
            filter=Operator(NodeType.SELECTION,fparams)
            filter.child.append(scan)
            scan.parent=filter

    if filter==None:
        return scan
    else: 
        return filter

def process_from(fromClause, qcontext,dcontext):
    #to do:
    # sort relation by the condition
    l=[]
    r=[]
    length=len(fromClause)
    if length==1:
        return process_scan(fromClause[0],qcontext,dcontext,True)
    elif length>=2:
        prev=process_scan(fromClause[0],qcontext,dcontext,True)
        rel1=get_relname(fromClause[0])
        l.append(rel1)
        
        for i in range(1,length):
            scan2=process_scan(fromClause[i],qcontext,dcontext,True)
            rel2=get_relname(fromClause[i])
            r=[rel2]
            #r.append(rel2)
            conds=get_join_cond(l,r,qcontext)
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

    conds=get_conds(qcontext)
    if conds != None and len(conds) > 0:
        fparams=dict()
        fparams['conds']=conds
        #fparams['relname']=relname # this is needed to get the correct table name with alias
        filter=Operator(NodeType.SELECTION,fparams)
        filter.child.append(prev)
        prev.parent=filter
        prev=filter

    return prev       


def process_from_nooptimize(fromClause, qcontext,dcontext):
    #to do:
    # sort relation by the condition
    prev=None
    length=len(fromClause)
    if length==1:
        prev= process_scan(fromClause[0],qcontext,dcontext,False)
    elif length>=2:
        prev=process_scan(fromClause[0],qcontext,dcontext,False)
        for i in range(1,length):
            scan2=process_scan(fromClause[i],qcontext,dcontext,False)
            crossproduct=Operator(NodeType.CROSSPRODUCT,None)
            prev.parent=crossproduct
            scan2.parent=crossproduct
            crossproduct.child.append(prev)
            crossproduct.child.append(scan2)

            prev=crossproduct
     
    conds=get_conds(qcontext)
    if conds != None and len(conds) > 0:
        fparams=dict()
        fparams['conds']=conds
        #fparams['relname']=relname # this is needed to get the correct table name with alias
        filter=Operator(NodeType.SELECTION,fparams)
        filter.child.append(prev)
        prev.parent=filter
        prev=filter
    return prev       


def buildtree(qcontext, dcontext, optimize=True):
    #convert from
    fclause=qcontext.fromClause
    if optimize==True:
        f=process_from(fclause,qcontext, dcontext)
    else:
        f=process_from_nooptimize(fclause,qcontext, dcontext)
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