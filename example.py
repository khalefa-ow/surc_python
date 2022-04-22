

class Example(object):
    pass



example= Example()
example.schema=dict()
example.schema['f']=("csvfile","f.csv")
example.schema['h']=("csvfile","h.csv")

example.schema['items']=("jsonfile","h.json","Items", True) #last parameter is for flatten json
example.schema['tweets']=("api","cache", "noretry","tweets.json","items")


