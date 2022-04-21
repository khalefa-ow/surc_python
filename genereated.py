import csv

def isfloat(element):
	try:
		float(element)
		return True
	except ValueError:
		return False
def isint(element):
	try:
		int(element)
		return True
	except ValueError:
		return False    
def get_value(v):
	if isint(v):
		return int(v)
	elif isfloat(v):
		return float(v)
	else:
		return str(v)
def operator_5():
	i1=operator_4()
	for row in i1:
		yield(row)

def operator_4():
	#build a hash table
	input1=dict()
	for row in operator_1():
		v1=get_value(row['x'])
		if v1 not in input1:
			input1[v1]=[]
		input1[v1].append(row)

	for row2 in operator_3():
		v2=get_value(row2['l'])
		if v2 in input1:
			l1=input1[v2]
			for v in l1:
				r=row2.copy()
				r.update(v)
			yield r

def operator_1():
	for a in operator_0():
		condition = True
		condition= condition and a['a']=='baz'  
		if  condition :
			yield a

def operator_0():
	with open('a.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
			yield row

def operator_3():
	for b in operator_2():
		condition = True
		if  condition :
			yield b

def operator_2():
	with open('b.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
			yield row

if __name__ == "__main__":
	for r in operator_5():
		print (r)
