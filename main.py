import parse as p
import tree as t
import example as e
import codegen
import argparse
from anytree import NodeMixin, RenderTree,PreOrderIter
from pglast.visitors import Visitor

#sqlq="select * from a ,  b where a.a = 'baz' and a.x=b.l  group by a having count(*)>100 order by x;"
sql1="select * from course c, student s, enrolled e where c.cid = e.ecid and s.id = e.eid ;"
sql2="select * from course c, enrolled e,  student s where c.cid = e.ecid and s.id = e.eid ;"

parser = argparse.ArgumentParser()
parser.add_argument("--opt",action="store_true",help="optimize", default="True")
parser.add_argument("--sql",action="store",help="sql query", default=sql2)
args = parser.parse_args()

v=p.parse(args.sql)
opt=False#False#args.opt

outfile = open('genereated.py', 'w')
codegen.emit_header(outfile)

for s in v:
    r=t.buildtree(s,e.example, opt)
    for pre, fill, node in RenderTree(r):
     print("%s%s %s" % (pre, node.id, node.optype))
    for x in PreOrderIter(r):
         x.emitcode(0,e.example, outfile)
    codegen.emit_footer(outfile,r)
