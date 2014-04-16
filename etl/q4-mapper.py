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
		text = decoded['text']
		sys.stdout.write(dtime + "\t" + decoded["id_str"] + ":")
		print repr(decoded['text'].encode('utf-8', 'replace'))

if __name__ == "__main__":
	main(sys.argv)
