
import parse as p
import tree as t
import example as e
import codegen

from anytree import NodeMixin, RenderTree
from pglast.visitors import Visitor
from anytree import NodeMixin, RenderTree, PreOrderIter


sql="select * from f a, h where a.bar = 'baz' and a.x=h.y and a.m>3 group by a having count(*)>100 order by x;"
#sql="select * from f a, h where a.bar = 'baz' "
#" select * from (select * from b)a;"
v=p.parse(sql)


outfile = open('genereated.py', 'w')
codegen.emit_header(outfile)

for s in v:
    r=t.buildtree(s)
    for pre, fill, node in RenderTree(r):
     print("%s%s %s" % (pre, node.id, node.optype))
    for x in PreOrderIter(r):
         x.emitcode(0,e.example, outfile)
    codegen.emit_footer(outfile,r)
