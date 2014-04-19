#!/usr/bin/python 
import sys
import re

def main(argv):
	tweet_list = []
	current_user_time = None
	tweet_list = []
	user_time = None

	# input comes from STDIN
	for line in sys.stdin:
		# remove leading and trailing whitespace
		line = line.strip()

		# parse the input we got from mapper.py
		user_time, tweet = line.split('\t', 1)

		if current_user_time == user_time:
			tweet_list.append(tweet)
		else:
			if current_user_time:
				# remove duplicate and alphabetically sort by str length
				tweet_list = list(set(tweet_list))
				tweet_list.sort()
				tweet_list.sort(key=len)
				res = ""
				for tweet_id in tweet_list:
					res = res + tweet_id + "_"
				print current_user_time + "," + res


			current_user_time = user_time
			tweet_list = []
			tweet_list.append(tweet)

	# the last one
	if current_user_time == user_time:
		# remove duplicate and alphabetically sort by str length
		tweet_list = list(set(tweet_list))
		tweet_list.sort()
		tweet_list.sort(key=len)
		
		res = ""
		for tweet_id in tweet_list:
			res = res + tweet_id + "_"
		print current_user_time + "," + res


if __name__ == "__main__":
	main(sys.argv)