def get_value(v):
	if v.isdigit():
		return int(v)
		elif v.isfloat():
			return float(v)
		else:
			return str(v)
#condition{'op': '=', 'r1': 'a', 'f1': 'bar', 'v2': 'baz'}
#condition{'op': '=', 'r1': 'a', 'f1': 'x', 'r2': 'h', 'f2': 'y'}
#condition{'op': '>', 'r1': 'a', 'f1': 'm', 'v2': 3}
5 RESULT= 0
└── 4 JOIN= 5
    ├── 1 FILTER= 4
    │   └── 0 SCAN= 1
    └── 3 FILTER= 4
        └── 2 SCAN= 1
def operator_5():
i1=operator_4()
for row in i1:
	print(row)

def operator_4():
	#build a hash table
	input1=dict()
	for row in operator_1():
		v1=get_value(row['x']):
		if v1 not in input1:
			input1[v1]]=[]
		input1[v1].append(row)

	for row2 in operator_3():
		v2=get_value(row2['y']))
		if v2 in input1:
			l1=input1[v2]
			for v in l1:
				r=row2.copy()
				r.update(v)
			yield r

def operator_1():
	for a in operator_0():
		condition = True
		condition= condition and a['bar']=baz 
		condition= condition and a['m']>3 
		if  condition :
			yield a

def operator_0():
	with open('f.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
			yield row

def operator_3():
	for h in operator_2():
		condition = True
		if  condition :
			yield h

def operator_2():
	with open('h.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
			yield row

if __name__ == "__main__":
for r in operator_id 5optype RESULT= 0 None [<tree.Operator object at 0x10ac06be0>]():
	print (r)
