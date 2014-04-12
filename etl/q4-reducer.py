#!/usr/bin/python 
import sys
import re
import collections
import happybase

def main(argv):
	current_dtime = None
	id_text_dict = {}
	dtime = None

	connection = happybase.Connection('ec2-54-85-185-35.compute-1.amazonaws.com')
	table = connection.table('tweets_q4')

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
				for key in order_dict:
					res = res + key + ":" + id_text_dict[key] + "\n"
				range_key = 't' 
				table.put(dtime,{range_key:res})

			current_dtime = dtime
			id_text_dict = {}
			id_text_dict[id_str] = text;

	# the last one
	if current_dtime == dtime:
		# remove duplicate and alphabetically sort by str length
		order_dict = collections.OrderedDict(sorted(id_text_dict.items()))
		res = ""
		for key in order_dict:
			res = res + key + ":" + id_text_dict[key] + "\n"
		range_key = 't' 
		table.put(dtime,{range_key:res})

	connection.close()
		
if __name__ == "__main__":
	main(sys.argv)