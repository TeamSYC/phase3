#!/usr/bin/python
import sys
import os
import json
from datetime import datetime

def main(argv):
	for line in sys.stdin:
		#data_string = json.dumps(line)
		decoded = json.loads(line)
		dtime = decoded["created_at"]
		dt_obj = datetime.strptime(dtime, '%a %b %d %H:%M:%S +%f %Y')
		dtime = dt_obj.strftime('%Y-%m-%d+%H:%M:%S')
		print decoded["user"]["id_str"] + "_" + dtime + "\t" + decoded["id_str"]

if __name__ == "__main__":
	main(sys.argv)
