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
    def __init__(self, id, type, parent=None, children=None):
        super(Operator, self).__init__()
        self.id = id
        self.type=type
        self.parent = parent
       
        if children:
             self.children = children
        else:
            self.children = []

def buildtree(s):
    id=1
    #convert from
    fclause=s.fromClause
    n=len(fclause)
    if(n>1):
        f=Operator(id,NodeType.CROSSPRODUCT)
        id=id+1
        for x in fclause:
            tmp=Operator(id,NodeType.SCAN,f)
            id=id+1
    else:
         f=Operator(id,NodeType.SCAN)  
         id=id+1  

    result=Operator(id,NodeType.RESULT)
    f.parent=result
    #at each node schema extraction
    return result