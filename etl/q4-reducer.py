#!/usr/bin/python 
import sys
import re
import ast
import happybase


def main(argv):
	current_dtime = None
	id_text_dict = {}
	dtime = None

	connection = happybase.Connection('ec2-54-86-77-90.compute-1.amazonaws.com')
	table = connection.table('q4')
	# input comes from STDIN
	for line in sys.stdin:
		# remove leading and trailing whitespace
		line = line.strip()

		# parse the input we got from mapper.py
		dtime, id_text = line.split('\t', 1)
		id_str, text = id_text.split(':', 1)
		text = ast.literal_eval(text)

		if dtime == current_dtime:
			id_text_dict[id_str] = text;
		else:
			if current_dtime:
				# remove duplicate and alphabetically sort by str length
				k = id_text_dict.keys()
				k.sort(key=int)
				print_res = ""
				for key in k:
					print_res = print_res + key + ":" + id_text_dict[key] + "\n"
				range_key = 't'
				table.put(current_dtime,{range_key:print_res})


			current_dtime = dtime
			id_text_dict = {}
			id_text_dict[id_str] = text;

	# the last one
	if current_dtime == dtime:
		# remove duplicate and alphabetically sort by str length
		k = id_text_dict.keys()
		k.sort(key=int)
		print_res = ""
		for key in k:
			print_res = print_res + key + ":" + id_text_dict[key] + "\n"
		range_key = 't'
		table.put(current_dtime,{range_key:print_res})

	connection.close()

		
if __name__ == "__main__":
	main(sys.argv)