

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
