#!/usr/bin/python
import sys
import os
import json
from datetime import datetime

def main(argv):
	for line in sys.stdin:
		#data_string = json.dumps(line)
		decoded = json.loads(line)
		if not 'retweeted_status' in decoded or not 'user' in decoded['retweeted_status']:
			continue
		orig_user = decoded['retweeted_status']['user']['id_str']
		current_user = decoded['user']['id_str']
		print orig_user + '\t' + current_user 

if __name__ == "__main__":
	main(sys.argv)