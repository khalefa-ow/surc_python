import csv

from numpy import integer
def operator_4():
	#build a hash table
	input1=dict()
	for row in operator_1():
		v=get_value(row['a'])
		if v not in  input1 :
			input1[v]=[]
		input1[v].append(row)

	for row2 in operator_3():
		v=get_value(row2['m'])
		if v in  input1:
			l=input1[v]
			for v in l:
				r=row2.copy()
				r.update(v)
				yield r

def get_value(v):
	if v.isdigit():
		return int(v)	
	elif v.isfloat():
			return float(v)
	else:
			return str(v)




def operator_1():
	for a in operator_0():
		condition = True
		condition= condition and get_value(a['a'])>1 
		condition= condition and get_value(a['b'])<10 
		if  condition :
			yield a

def operator_0():
	with open('a.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
#			print(row)
			yield row

def operator_3():
	for h in operator_2():
		condition = True
		if  condition :
			yield h

def operator_2():
	with open('b.csv', newline='') as csv_file:
		csv_reader = csv.DictReader(csv_file, delimiter=',')
		for row in csv_reader:
#			print(row)
			yield row

if __name__ == "__main__":
	for a in operator_4():
		print(a)
		print("\n\n\n ")