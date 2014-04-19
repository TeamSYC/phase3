#!/usr/bin/python 
import sys
import re

def main(argv):
	user = None
	count = 0

	# input comes from STDIN
	for line in sys.stdin:
		# remove leading and trailing whitespace
		line = line.strip()

		# parse the input we got from mapper.py
		orig_user, number = line.split('\t', 1)

		if orig_user == user:
			count += 1
		else:
			if user:
				print user + "," + str(count)

			user = orig_user
			count = 1

	# the last one
	if user == orig_user:
		print user + "," + str(count)

if __name__ == "__main__":
	main(sys.argv)
