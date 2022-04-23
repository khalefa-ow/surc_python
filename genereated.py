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
def operator_1():
	i1=operator_0()
	for row in i1:
		yield(row)

def operator_0():
	with open('data/items-0.json', newline='') as json_file:
		json_data = json.load(json_file)
		for row in json_data['Items']:
			yield flatten(row)

if __name__ == "__main__":
	for r in operator_1():
		print (r)
