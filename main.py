from doctest import Example
import parse as p
import tree as t
import example as e

from anytree import NodeMixin, RenderTree
from pglast.visitors import Visitor
from anytree import NodeMixin, RenderTree, PreOrderIter


sql="select * from f a, h where a.bar = 'baz' and a.x=h.y and a.m>3 group by a having count(*)>100 order by x;"
#sql="select * from f a, h where a.bar = 'baz' "
#" select * from (select * from b)a;"
v=p.parse(sql)

for s in v:
    r=t.buildtree(s)
    for pre, fill, node in RenderTree(r):
     print("%s%s %s" % (pre, node.id, node.optype))
    for x in PreOrderIter(r):
        print (x.emitcode(1,e.example))