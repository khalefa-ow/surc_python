import json

def func():
    filename="/Users/user/Desktop/items-0.json"
    objects="Items"
    with open(filename, newline='') as json_file:
        json_data = json.load(json_file)
        for x in json_data[objects]:
            yield x
        
    

for x in func():
    print(x)

