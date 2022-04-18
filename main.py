import parse as p
import tree as t

from anytree import NodeMixin, RenderTree


sql="select * from f a, h where f.bar = 'baz' and f.x=h.y and f.m>3 group by a having count(*)>100 order by x;"
#" select * from (select * from b)a;"
v=p.parse(sql)

for s in v:
    r=t.buildtree(s)
    for pre, fill, node in RenderTree(r):
     print("%s%s %s" % (pre, node.id, node.type)) 