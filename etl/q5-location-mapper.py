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

		try:
			print place + "\t" + "1"
		except UnicodeEncodeError:
			continue

if __name__ == "__main__":
	main(sys.argv)