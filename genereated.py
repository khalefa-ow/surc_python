import csv
import json
from flatten_json import flatten

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
def operator_6():
	i1=operator_5()
	for row in i1:
		yield(row)

def operator_5():
	#build a hash table
	input1=dict()
	for row in operator_1():
		v1=get_value(row['x'])
		if v1 not in input1:
			input1[v1]=[]
		input1[v1].append(row)

	for row2 in operator_4():
		v2=get_value(row2['l'])
		if v2 in input1:
			l1=input1[v2]
			for v in l1:
				r=row2.copy()
				r.update(v)
			yield r

def operator_1():
	for row in operator_0():
		condition = True
		condition= condition and ('a' in row and row['a']=='baz'  )
		if  condition :
			yield row

def operator_0():
	with open('f.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
			yield row

def operator_4():
	for row in operator_3():
		condition = True
		if  condition :
			yield row

def operator_3():
def operator_2():
if __name__ == "__main__":
	for r in operator_6():
		print (r)
