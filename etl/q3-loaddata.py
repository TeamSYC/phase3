#!/usr/bin/python 
import sys
import re
import happybase

def main(argv):
	#connection to hbase
	connection = happybase.Connection('ec2-54-85-60-99.compute-1.amazonaws.com')
	table = connection.table('tweets_q3')
	# input comes from STDIN
	for line in sys.stdin:
		# remove leading and trailing whitespace
		line = line.strip()

		# parse the input we got from mapper.py
		current_user, res = line.split(',', 1)
		res.replace = ('_','\n')

		table.put(current_user,{'r':res})

	connection.close()
		
if __name__ == "__main__":
	main(sys.argv)