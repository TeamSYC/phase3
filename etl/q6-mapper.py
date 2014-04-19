#!/usr/bin/python
import sys
import os
import json
from datetime import datetime

def main(argv):
	for line in sys.stdin:
		#data_string = json.dumps(line)
		decoded = json.loads(line)
		current_user = decoded['user']['id_str']
		print current_user + '\t' +  '1'

if __name__ == "__main__":
	main(sys.argv)