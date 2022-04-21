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
def operator_4():
	i1=operator_3()
	for row in i1:
		yield(row)

def operator_3():
	for row in operator_2():
		condition = True
		condition= condition and ('a' in row and row['a']=='baz'  )
		condition= condition and ('x' in row and 'l' in row and row['x']==row['l']  )
		if  condition :
			yield row

def operator_2():
	input1=[]
	for row in operator_0():
		input1.append(row)

	for row2 in operator_1():
		for row1 in input1:
			r=row2.copy()
			r.update(row1)
			yield r

def operator_0():
	with open('a.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
			yield row

def operator_1():
	with open('b.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
			yield row

if __name__ == "__main__":
	for r in operator_4():
		print (r)
