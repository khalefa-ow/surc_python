
import parse as p
import tree as t
import example as e
import codegen

from anytree import NodeMixin, RenderTree
from pglast.visitors import Visitor
from anytree import NodeMixin, RenderTree, PreOrderIter


sql="select * from f a, h b where a.a = 'baz' and a.x=b.l  group by a having count(*)>100 order by x;"
#sql="select * from f a, h where a.bar = 'baz' "
#" select * from (select * from b)a;"
v=p.parse(sql)
opt=False

outfile = open('genereated.py', 'w')
codegen.emit_header(outfile)

for s in v:
    r=t.buildtree(s, opt)
    for pre, fill, node in RenderTree(r):
     print("%s%s %s" % (pre, node.id, node.optype))
    for x in PreOrderIter(r):
         x.emitcode(0,e.example, outfile)
    codegen.emit_footer(outfile,r)
