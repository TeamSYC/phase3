#!/usr/bin/python 
import sys
import re
import happybase

def main(argv):
	#connection to hbase
	connection = happybase.Connection('172.31.47.117')
	table = connection.table('q2')
	# input comes from STDIN
	for line in sys.stdin:
		# remove leading and trailing whitespace
		line = line.strip()

		# parse the input we got from mapper.py
		user_time, res = line.split(',', 1)
		res.replace('_','\n')
		table.put(user_time,{'t':res})

	connection.close()
		
if __name__ == "__main__":
	main(sys.argv)