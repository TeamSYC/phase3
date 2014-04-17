#!/usr/bin/python 
import sys
import re

def main(argv):
	prev_place_time = None
	tweet_list = []

	for line in sys.stdin:
		# remove leading and trailing whitespace
		line = line.strip()

		# parse the input we got from mapper.py
		place_time, tweet_id = line.split('\t', 1)

		if place_time == prev_place_time:
			tweet_list.append(long(tweet_id))
		else:
			if prev_place_time:
				# remove duplicate and alphabetically sort by str length
				tweet_list = list(set(tweet_list))
				tweet_list.sort()
				res = ""
				for id in tweet_list:
					res = res + str(id) + "_"
				print prev_place_time + "," + res
			prev_place_time = place_time
			tweet_list = []
			tweet_list.append(long(tweet_id))

	if prev_place_time == place_time:
		tweet_list = list(set(tweet_list))
		tweet_list.sort()
		res = ""
		for id in tweet_list:
			res = res + str(id) + "_"
		print prev_place_time + "," + res
		
if __name__ == "__main__":
	main(sys.argv)
