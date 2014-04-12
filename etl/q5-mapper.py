#!/usr/bin/python
import sys
import os
import json
from datetime import datetime

def main(argv):
	for line in sys.stdin:
		decoded = json.loads(line)
		if not 'place' in decoded or not 'name' in decoded['place']:
			continue
		place = decoded['place']['name'].strip()
		if len(place) < 2 or len(place.split()) > 3:
			continue
		place = place.replace(' ', '+')
		dtime = decoded['created_at']
		dt_obj = datetime.strptime(dtime, '%a %b %d %H:%M:%S +%f %Y')
		dtime = dt_obj.strftime('%Y-%m-%d+%H:%M:%S')
		print place + "_" + dtime + "\t" + decoded['id_str']

if __name__ == "__main__":
	main(sys.argv)