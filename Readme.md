this is a simple code generateion

handle big table 
and memory
condition and and or


#adjust condition handling
#fix handling logical expression



from pglast.visitors import Visitor

nodes=[]

class CollectorVisitor(Visitor):
    def visit(self, ancestors, node):
        print(node.parent)
        #print(ancestors, ':', node(depth=0, skip_none=True))
        nodes.append(node)

visitor = CollectorVisitor()
visitor(r)


for node in nodes:
   
    print(node)
    print("\n\n")


for pre, fill, node in RenderTree(r):
    print("%s%s" % (pre, node.id))   