import parse as p
import tree as t

from anytree import NodeMixin, RenderTree


sql="select * from f, h where f.bar = 'baz' and f.x=h.y;"
#" select * from (select * from b)a;"
v=p.parse(sql)

for s in v:
    r=t.buildtree(s)
    for pre, fill, node in RenderTree(r):
     print("%s%s %s" % (pre, node.id, node.type)) 