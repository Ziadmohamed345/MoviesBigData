import json
import csv

with open('mds.json') as json_file:
    data = json.load(json_file)

csv_file = open('mdsComma.csv', 'w', newline='')
csv_writer = csv.writer(csv_file)

# Write header
header = data[0].keys()
csv_writer.writerow(header)

# Write rows
for row in data:
    csv_writer.writerow(row.values())

csv_file.close()