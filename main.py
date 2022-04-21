import parse as p
import tree as t
import example as e
import codegen
import argparse
from anytree import NodeMixin, RenderTree,PreOrderIter
from pglast.visitors import Visitor

sqlq="select * from f a, h b where a.a = 'baz' and a.x=b.l  group by a having count(*)>100 order by x;"

parser = argparse.ArgumentParser()
parser.add_argument("--opt",action="store_true",help="optimize", default="True")
parser.add_argument("--sql",action="store",help="sql query", default=sqlq)
args = parser.parse_args()
#sql="select * from f a, h where a.bar = 'baz' "
#" select * from (select * from b)a;"

v=p.parse(args.sql)
opt=args.opt

outfile = open('genereated.py', 'w')
codegen.emit_header(outfile)

for s in v:
    r=t.buildtree(s, opt)
    for pre, fill, node in RenderTree(r):
     print("%s%s %s" % (pre, node.id, node.optype))
    for x in PreOrderIter(r):
         x.emitcode(0,e.example, outfile)
    codegen.emit_footer(outfile,r)
