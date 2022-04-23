

def scan():
    yield 1
    yield 2
    yield 3
    yield None



def filter():
    for i in scan():
        if i is None:
            return
        if i % 2 == 1:
            yield i


for m in filter():
    print(m)




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