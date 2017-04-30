# This script is compatible with Python 3.6.
# Using Python 2.7 may not work
# Written by Sergey Klimantovich
# Date: 30/04/2017


import json
import csv
from datetime import datetime
import argparse
import os

"""
This method opens a file with JSON object,
read it line by line and creates json_data object
Receives a file path and returns object
"""
def open_json_file(read_filepath):
    json_data = []
    with open(read_filepath) as json_file:
        for line in json_file:
            try:
                data = json.loads(line)
            except ValueError:
                print ("Can't read a line as a JSON object: " + line)
                continue
            json_data.append(data)
    return json_data

"""
This method opens a csv file,
writes a csv data line by line
Receives csv data and file path
"""
def write_csv_data(csv_data, write_fiepath):
    with open(write_fiepath, "w", newline='') as csv_file:
        writer = csv.writer(csv_file, delimiter=',', quotechar='"')
        for line in csv_data:
            writer.writerow(line)

"""
This method prints statistics in JSON format
Receives statistics
"""
def print_stats_json(stats, sort=False, indents=4):
    if type(stats) is str:
        print(json.dumps(json.loads(stats), sort_keys=sort, indent=indents))
    else:
        print(json.dumps(stats, sort_keys=sort, indent=indents))
    return None

"""
This method analize the data ,
read it line by line and creates metadata output and csv data
Receives the data and returns statistics and csv data
"""
def analize_data(data):
    ADD = ['createdDoc', 'addedText', 'changedText']
    REMOVE = ['deletedDoc', 'deletedText', 'archived']
    ACCESSED = ['viewedDoc']
    csvline = []
    csvlines = [['TIMESTAMP','ACTION','USER','FOLDER','FILENAME','IP']]

    metrics_output = {
        "lineReads": 0,
        "droppedEventsCounts": 0,
        "droppedEvents": {
            "No action mapping": 0,
            "Duplicates": 0
        },
        "uniqueUsers": 0,
        "uniqueFiles": 0,
        "startDate": 0,
        "endDate": 0,
        "actions": {
            "ADD": 0,
            "REMOVE": 0,
            "ACCESSED": 0
        }
    }

    event_id = []
    user = []
    file = []

    for line in data:
        metrics_output['lineReads'] += 1
        if line['eventId'] in event_id:
            metrics_output['droppedEvents']['Duplicates'] += 1
            metrics_output['droppedEventsCounts'] += 1
        else:
            if line['activity'] in ADD:
                metrics_output['actions']['ADD'] += 1
                line['activity'] = "ADD"
            elif line['activity'] in REMOVE:
                metrics_output['actions']['REMOVE'] += 1
                line['activity'] = "REMOVE"
            elif line['activity'] in ACCESSED:
                metrics_output['actions']['ACCESSED'] += 1
                line['activity'] = "ACCESSED"
            else:
                metrics_output['droppedEvents']['No action mapping'] += 1
                metrics_output['droppedEventsCounts'] += 1
                continue

            event_id.append(line['eventId'])

            if not line['user'] in user:
                metrics_output['uniqueUsers'] += 1
                user.append(line['user'])

            if not line['file'] in file:
                metrics_output['uniqueFiles'] += 1
                file.append(line['file'])

            if 'timeOffset' in line:
                time = line['timestamp'] + ''.join(line['timeOffset'].split(':'))
            else:
                time = line['timestamp'] + '+0000'
            dtime = datetime.strptime(time, '%m/%d/%Y %I:%M:%S%p%z')

            if metrics_output['startDate'] == 0 or metrics_output['startDate'] > dtime:
                metrics_output['startDate'] = dtime

            if metrics_output['endDate'] == 0 or metrics_output['endDate'] < dtime:
                metrics_output['endDate'] = dtime

            csvline.append(dtime.isoformat())
            csvline.append(line['activity'])
            csvline.append(line['user'].split('@')[0])
            csvline.append('/'.join(line['file'].split('/')[:-1]))
            csvline.append(line['file'].split('/')[-1])
            csvline.append(line['ipAddr'])

            csvlines.append(csvline)
            csvline = []

    metrics_output['startDate'] = metrics_output['startDate'].isoformat()
    metrics_output['endDate'] = metrics_output['endDate'].isoformat()

    return metrics_output, csvlines

def main():
    parser = argparse.ArgumentParser(description="This program reads and parses the JSON file"
                                                 "produces a filtered CSV file for our behaviour analytics team to analyze, and"
                                                 "aggregates and prints out statistics at the end.")
    parser.add_argument("json_path", type = str, help = "A path to a JSON file to read")
    parser.add_argument("csv_path", type = str, help = "A path for a CSV file to write")
    args = parser.parse_args()

    json_filepath = args.json_path
    csv_filepath = args.csv_path

    if not (os.path.exists(json_filepath) and os.access(os.path.dirname(json_filepath), os.R_OK)):
        print("Can not read: " + json_filepath)
        return

    if not os.access(os.path.dirname(csv_filepath), os.W_OK):
        print ("Can not write: " + csv_filepath)
        return

    data = open_json_file(json_filepath)
    statistics, csvdata = analize_data(data)
    write_csv_data(csvdata, csv_filepath)
    print_stats_json(statistics)

if __name__ == "__main__":
    main()




