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
	for row in operator_4():
		condition = True
		condition= condition and ('cid' in row and 'ecid' in row and row['cid']==row['ecid']  )
		condition= condition and ('id' in row and 'eid' in row and row['id']==row['eid']  )
		if  condition :
			yield row

def operator_4():
	input1=[]
	for row in operator_2():
		input1.append(row)

	for row2 in operator_3():
		for row1 in input1:
			r=row2.copy()
			r.update(row1)
			yield r

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
	with open('data/course.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
			yield row

def operator_1():
	with open('data/enrolled.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
			yield row

def operator_3():
	with open('data/student.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
			yield row

if __name__ == "__main__":
	for r in operator_6():
		print (r)
