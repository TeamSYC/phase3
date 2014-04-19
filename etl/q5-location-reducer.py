#!/usr/bin/python 
import sys
import re

def main(argv):
	prev_place = None

	for line in sys.stdin:
		line = line.strip()
		place = line.split('\t', 1)[0]

		if prev_place != place:
			print place
			prev_place = place
		
if __name__ == "__main__":
	main(sys.argv)
