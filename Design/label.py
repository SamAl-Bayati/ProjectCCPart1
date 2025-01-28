from google.cloud import pubsub_v1      # pip install google-cloud-pubsub  ##to install
import glob                             # for searching for json file
import json
import os
import random
import numpy as np                      # pip install numpy    ##to install
import time
import csv                              # ADDED for CSV reading

# Search the current directory for the JSON file (including the service account key) 
# to set the GOOGLE_APPLICATION_CREDENTIALS environment variable.
files = glob.glob("*.json")
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = files[0];

# Set the project_id with your project ID
project_id="project-449200";
topic_name = "label";   # change it for your topic name if needed

# create a publisher and get the topic path for the publisher
publisher = pubsub_v1.PublisherClient()
topic_path = publisher.topic_path(project_id, topic_name)
print(f"Published messages with ordering keys to {topic_path}.")

# NEW: Read from CSV, iterate over the rows, serialize each row as a dictionary, and publish.
csv_filename = "Labels.csv"

with open(csv_filename, "r") as csv_file:
    csv_reader = csv.DictReader(csv_file)
    for row in csv_reader:
        # row is already a dictionary with column names as keys and row values as strings
        record_value = json.dumps(row).encode("utf-8")  # serialize the dictionary

        try:
            future = publisher.publish(topic_path, record_value)
            # ensure that the publishing has been completed successfully
            future.result()
            print("The message {} has been published successfully".format(row))
        except:
            print("Failed to publish the message")

        time.sleep(0.5)  # wait for 0.5 second between each publish
