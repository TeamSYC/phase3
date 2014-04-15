#!/usr/bin/python
import sys
import os
import json
from datetime import datetime

def main(argv):
	for line in sys.stdin:
		decoded = json.loads(line)

		# Make sure that place and place-name are defined within JSON object.
		if not 'place' in decoded or not decoded['place'] or not 'name' in decoded['place'] or not decoded['place']['name']:
			continue

		# Remove all excess whitespace, and filter out places whose names are less than 2
		# characters long or are more than 3 words long.
		place = decoded['place']['name'].strip()
		if len(place) < 2 or len(place.split()) > 3:
			continue
		# For valid place, replace whitespace with +.
		place = place.replace(' ', '+')

		dtime = decoded['created_at']
		dt_obj = datetime.strptime(dtime, '%a %b %d %H:%M:%S +%f %Y')
		dtime = dt_obj.strftime('%Y-%m-%d+%H:%M:%S')

		try:
			print place + "_" + dtime + "\t" + decoded['id_str']
		except UnicodeEncodeError:
			continue

if __name__ == "__main__":
	main(sys.argv)