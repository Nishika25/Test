import csv
import time
import logging
import requests
from requests.auth import HTTPBasicAuth
import json
import os
import sys

file_path = "checkOffset.csv"
print(f"Trying to read from Update: {file_path}")

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
success_logger = logging.getLogger('success_logger')
error_logger = logging.getLogger('error_logger')

# Create a file handler
success_handler = logging.FileHandler('success_offset_log.txt')
error_handler = logging.FileHandler('error_offset_log.txt')

success_handler.setLevel(logging.INFO)
error_handler.setLevel(logging.ERROR)

# Create a formatter and add it to the handler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
success_handler.setFormatter(formatter)
error_handler.setFormatter(formatter)

# Add the handler to the logger
success_logger.addHandler(success_handler)
error_logger.addHandler(error_handler)

with open("appsettings.json", "r") as creds:
    credentials = json.load(creds)

def get_connector_status(URL, connector, username=None, password=None, token=None):
    try:
        if token:
            headers = {
                'Authorization': f'Bearer {token}'
            }
            response = requests.get(URL + "/status", headers=headers, verify=False)
            if response.status_code == 200:
                print(f"Successfully received status of connector {connector}")
                success_logger.info(f"Successfully received status of connector {connector}")
                return response.json()['connector']['state'], response.json()['tasks'][0]['state']
            else:
                print(f"Failed to get status of connector {connector}: {response.text}")
                error_logger.error(f"Failed to get status of connector {connector}: {response.text}")
                return None, None
        else:
            auth = HTTPBasicAuth(username, password)
            response = requests.get(URL + "/status", auth=auth, verify=False)
            if response.status_code == 200:
                print(f"Successfully received status of connector {connector}")
                success_logger.info(f"Successfully received status of connector {connector}")
                return response.json()['connector']['state'], response.json()['tasks'][0]['state']
            else:
                print(f"Failed to get status of connector {connector}: {response.text}")
                error_logger.error(f"Failed to get status of connector {connector}: {response.text}")
                return None, None
    except Exception as e:
        print(f"Failed to get status of connector {connector}: {e}")
        error_logger.error(f"Failed to get status of connector {connector}: {e}")
        return None, None

def getMethod(URL, username=None, password=None, token=None): 
    try:
        if token:
            headers = {
                'Authorization': f'Bearer {token}'
            }
            response = requests.get(URL, headers=headers, verify=False)
            if response.status_code == 200:
                print(f"Successfully received offset from {URL}")
                success_logger.info(f"Successfully received offset from {URL}")
                return response.json()
            else:
                print(f"Failed to get offset from {URL}: {response.text}")
                error_logger.error(f"Failed to get offset from {URL}: {response.text}")
                return None
        else:
            auth = HTTPBasicAuth(username, password)
            response = requests.get(URL, auth=auth, verify=False)
            if response.status_code == 200:
                print(f"Successfully received offset from {URL}")
                success_logger.info(f"Successfully received offset from {URL}")
                return response.json()
            else:
                print(f"Failed to get offset from {URL}: {response.text}")
                error_logger.error(f"Failed to get offset from {URL}: {response.text}")
                return None
    except Exception as e:
        print(f"Failed to get offset from {URL}: {e}")
        error_logger.error(f"Failed to get offset from {URL}: {e}")
        return None
 
def CheckOffset():
    with open(file_path, 'r') as file, open("success_Offset.csv", "w", newline="") as success_file, open("failed_Offset.csv", "w", newline="") as failed_file:
        try:
            failed_writer = csv.writer(failed_file)
            success_writer = csv.writer(success_file)
            reader = csv.reader(file)
            header = next(reader)
            header.append("Reason")
            failed_writer.writerow(["$".join(header)])
            success_writer.writerow(["$".join(header)])

            skip_list = []

            for row in reader:
                try:
                    line_split = row[0].split("$")
                    environment = line_split[0]
                    confluent_connector = line_split[1]
                    confluent_cluster = line_split[2]
                    env_id = "env-rgq57"
                    Conf_username = credentials[environment]['cloud_credentials']['confluent_Key']
                    Conf_password = credentials[environment]['cloud_credentials']['confluent_Secret']
                    confluent_URL = f"https://api.confluent.cloud/connect/v1/environments/{env_id}/clusters/{confluent_cluster}/connectors/{confluent_connector}"
                    # confluent_status, confluent_task_status = get_connector_status(confluent_URL, confluent_connector, Conf_username, Conf_password)
                    Conf_Url = f"{confluent_URL}/offsets"
                    conf_config = getMethod(Conf_Url, username=Conf_username, password=Conf_password)
                    row.append(str(conf_config))
                    success_writer.writerow(["$".join(row)])
                    print(f"Successfully captured offset for {confluent_connector}")
                    success_logger.info(f"Successfully captured offset for {confluent_connector}")

                except Exception as e:
                    print(f"Failed to update configuration for row :{row}, Error: {e}")
                    error_logger.error(f"Failed to update configuration for row :{row}, Error: {e}")

        except Exception as e:
            print(f"Failed to update configuration file: {e}")
            error_logger.error(f"Failed to update configuration file: {e}")

if __name__ == "__main__":
    CheckOffset()
