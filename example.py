class Example(object):
    pass
example= Example()

example.schema=dict()
example.schema['course']=("csvfile","data/course.csv")
example.schema['student']=("csvfile","data/student.csv")
example.schema['enrolled']=("csvfile","data/enrolled.csv")

example.schema['items']=("jsonfile","h.json","Items", True) #last parameter is for flatten json
example.schema['tweets']=("api","cache", "noretry","tweets.json","items")


