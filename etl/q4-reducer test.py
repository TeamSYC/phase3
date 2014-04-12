#!/usr/bin/python 
import sys
import re
import collections

def main(argv):
	current_dtime = None
	id_text_dict = {}
	dtime = None

	# input comes from STDIN
	for line in sys.stdin:
		# remove leading and trailing whitespace
		line = line.strip()

		# parse the input we got from mapper.py
		dtime, id_text = line.split('\t', 1)
		id_str, text = id_text.split(':', 1)

		if dtime == current_dtime:
			id_text_dict[id_str] = text;
		else:
			if current_dtime:
				# remove duplicate and alphabetically sort by str length
				order_dict = collections.OrderedDict(sorted(id_text_dict.items()))
				res = ""
				print_res = ""
				for key in order_dict:
					res = res + key + ":" + id_text_dict[key] + "\n"
					print_res = print_res + key + ":" + id_text_dict[key] + "_"
				print current_dtime + "," + print_res
				range_key = 't' 

			current_dtime = dtime
			id_text_dict = {}
			id_text_dict[id_str] = text;

	# the last one
	if current_dtime == dtime:
		# remove duplicate and alphabetically sort by str length
		order_dict = collections.OrderedDict(sorted(id_text_dict.items()))
		res = ""
		print_res = ""
		for key in order_dict:
			res = res + key + ":" + id_text_dict[key] + "\n"
			print_res = print_res + key + ":" + id_text_dict[key] + "_"
		print current_dtime + "," + print_res
		range_key = 't' 
		
if __name__ == "__main__":
	main(sys.argv)