import logging
import sys
import json
import locale
from os import getenv, environ
import base64
from typing import Optional
import requests
import re
from googleapiclient import discovery, errors
from google.auth import default
from google.auth.transport.requests import Request as googleRequest
from google.auth.jwt import Credentials
from google.oauth2 import service_account
from datetime import datetime
from getGoogleProjectNumber import getGoogleProjectNumber

# (Optional) Customize the backup labels
my_daily_label = "daily"
my_weekly_label = "weekly"
my_monthly_label = "monthly"
my_yearly_label = "yearly"

# Currently it supports 32 backups per volume
max_backup_snapshots = 32

def get_token():

    from google.cloud import secretmanager
    
    # Setup the Secret manager Client
    client = secretmanager.SecretManagerServiceClient()

    # Get the sites environment credentials
    project_id = "cv-solution-architect-lab"

    # Get the secret
    secret_name = "cvs-backup-scheduler"

    resource_name = f"projects/{project_id}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(resource_name)
    service_account_credential = response.payload.data.decode('UTF-8')

    audience = 'https://cloudvolumesgcp-api.netapp.com'

    svc_creds = service_account.Credentials.from_service_account_info(json.loads(service_account_credential))

    # Create jwt
    jwt_creds = Credentials.from_signing_credentials(svc_creds, audience=audience)

    # Issue request to get auth token
    request = googleRequest()
    jwt_creds.refresh(request)

    # Extract token
    id_token = jwt_creds.token

    return id_token

class CloudVolumes:

    def __init__(self, project_number, region, volume_name, volume_id):

        self.project_number = project_number
        self.region = region
        self.volume_id = volume_id
        self.name = volume_name

    def delete_backup(self, backup_id, id_token):
        
        delete_url = "https://cloudvolumesgcp-api.netapp.com/v2/projects/" + str(self.project_number) + "/locations/" + self.region + "/Volumes/" + self.volume_id + "/Backups/" + backup_id
        print(json.dumps({'severity': "INFO", 'message': "Deleting the backup " + backup_id}))

        # Construct GET request
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + id_token.decode('utf-8'),
            "User-Agent": "cvs-backup-scheduler"
        }

        # Issue the request to the server
        try:
            response = requests.delete(delete_url, headers=headers)
        except:
            print(json.dumps({'severity': "ERROR", 'message': "There was an error in the DELETE call " + delete_url}))
            sys.exit(1)
        
        if (response.status_code != 202):
            print(json.dumps({'severity': "ERROR", 'message': "There was an error in the DELETE call " + delete_url}))
        else:
            # Load the json response into a dict
            r_dict = response.json()
            # Remove the below comment to detailed troubleshooting
            # print(r_dict)
            print(json.dumps({'severity': "DEBUG", 'message': "Correct response to GET request: " + delete_url}))

    def rotate_backup(self, backup_label, max_snapshots, id_token):
        
        # Construct GET request
        get_url = "https://cloudvolumesgcp-api.netapp.com/v2/projects/" + str(self.project_number) + "/locations/" + self.region + "/Volumes/" + self.volume_id + "/Backups"
        # print(backupsURL)
        payload = ""
        headers = {
            'Content-Type': "application/json",
            'Authorization': "Bearer " + id_token.decode('utf-8'),
            'cache-control': "no-cache",
            "User-Agent": "cvs-backup-scheduler"
        }
        try:
            response = requests.request("GET", get_url, data=payload, headers=headers)
        except:
            print(json.dumps({'severity': "ERROR", 'message': "There was an error in the GET call " + get_url}))
            sys.exit(1)
        
        if (response.status_code != 200):
            print(json.dumps({'severity': "ERROR", 'message': "There was an error in the GET call " + get_url}))
            sys.exit(1)
        else:
            # Load the json response into a dict
            r_dict = response.json()
            # Remove the below comment to detailed troubleshooting
            # print(r_dict)
            # Print out all service levels
            print(json.dumps({'severity': "DEBUG", 'message': "Correct response to GET request: " + get_url}))

            # Looking for the oldest existing backup
            num_sched_backups = 0
            oldest_backup_id = "null"
            oldest_time_str = "9999-12-31 00:00:00"
            oldest_time = datetime.strptime(oldest_time_str,"%Y-%m-%d %H:%M:%S")
            t1 = datetime.timestamp(oldest_time)

            for backup in r_dict:
                backup_id = backup['backupId']
                backup_name = backup['name']

                splitted_backup_name = backup_name.split("_")
                new_label_position = int(backup_name.count('_'))-1
                new_timestamp_position = int(backup_name.count('_'))
                
                if (splitted_backup_name[new_label_position] == backup_label) and (splitted_backup_name[new_timestamp_position].count('-') == 5):
                    num_sched_backups += 1

                    backup_time = datetime.strptime(splitted_backup_name[new_timestamp_position], "%m-%d-%Y-%H-%M-%S")
                    t2 = datetime.timestamp(backup_time)

                    if (t2 < t1):
                        oldest_backup_id = str(backup_id)
                        t1 = t2

            print(json.dumps({'severity': "DEBUG", 'message': "Currently there are " + str(num_sched_backups) + " existing backups"}))
            print(json.dumps({'severity': "DEBUG", 'message': "Maximum " + str(max_snapshots) + " snapshots should be keeped"}))

            if (num_sched_backups >= max_snapshots):
                self.delete_backup(oldest_backup_id, id_token)

    def create_backup(self, backup_label, id_token):
        
        current_time = datetime.now()
        current_time_str = current_time.strftime("%m-%d-%Y-%H-%M-%S")
        backup_name = self.name + "_" + backup_label + "_" + current_time_str
        print(json.dumps({'severity': "DEBUG", 'message': "Creating the backup " + backup_name}))
        data = {"name": backup_name}

        # Create backup post url
        post_url = "https://cloudvolumesgcp-api.netapp.com/v2/projects/" + str(self.project_number) + "/locations/" + self.region + "/Volumes/" + self.volume_id + "/Backups"

        # Construct post request
        headers = {
            "Content-Type": "application/json",
            "Authorization": "Bearer " + id_token.decode('utf-8'),
            "User-Agent": "cvs-backup-scheduler"
        }

        # Issue the request to the server
        try:
            response = requests.post(post_url, headers=headers, data=json.dumps(data))
        except:
            print(json.dumps({'severity': "ERROR", 'message': "There was an error in the POST call " + post_url}))
            sys.exit(1)
        
        if (response.status_code != 202):
            print(json.dumps({'severity': "ERROR", 'message': "There was an error in the POST call " + post_url}))
            sys.exit(1)
        else:
            # Load the json response into a dict
            r_dict = response.json()
            # Remove the below comment to detailed troubleshooting
            # print(r_dict)
            print(json.dumps({'severity': "DEBUG", 'message': "Correct response to POST request: " + post_url}))
            print(json.dumps({'severity': "INFO", 'message': "Backup " + backup_name + " was successfully created"}))

def cvs_backup_scheduler(event, context):

    print(json.dumps({'severity': "INFO", 'message': "--- START ---"}))

    my_project_number = getenv('PROJECT_NUMBER', None)
    my_volume_list = getenv('VOLUMES', None)
    my_max_daily_snapshots = getenv('DAILY_SNAPSHOTS_TO_KEEP', None)
    my_max_weekly_snapshots = getenv('WEEKLY_SNAPSHOTS_TO_KEEP', None)
    my_max_monthly_snapshots = getenv('MONTHLY_SNAPSHOTS_TO_KEEP', None)
    my_max_yearly_snapshots = getenv('YEARLY_SNAPSHOTS_TO_KEEP', None)

    if not my_project_number:
        print(json.dumps({'severity': "ERROR", 'message': "The required environment variable PROJECT_NUMBER is missing"}))
        return 'False'
    elif not my_volume_list:
        print(json.dumps({'severity': "ERROR", 'message': "The required environment variable VOLUMES is missing"}))
        return 'False'
    elif not my_max_daily_snapshots:
        print(json.dumps({'severity': "ERROR", 'message': "The required environment variable DAILY_SNAPSHOTS_TO_KEEP is missing"}))
        return 'False'
    elif not my_max_weekly_snapshots:
        print(json.dumps({'severity': "ERROR", 'message': "The required environment variable WEEKLY_SNAPSHOTS_TO_KEEP is missing"}))
        return 'False'
    elif not my_max_monthly_snapshots:
        print(json.dumps({'severity': "ERROR", 'message': "The required environment variable MONTHLY_SNAPSHOTS_TO_KEEP is missing"}))
        return 'False'
    elif not my_max_yearly_snapshots:
        print(json.dumps({'severity': "ERROR", 'message': "The required environment variable YEARLY_SNAPSHOTS_TO_KEEP is missing"}))
        return 'False'
    
    if re.match(r"[a-zA-z][a-zA-Z0-9-]+", my_project_number):
        my_project_number = getGoogleProjectNumber(my_project_number)
        if my_project_number == None:
            print(json.dumps({'severity': "ERROR", 'message': "Cannot resolve projectId to project number. Please specify project number"}))
            return 'False'
            
    if (int(my_max_daily_snapshots) + int(my_max_weekly_snapshots) + int(my_max_monthly_snapshots) + int(my_max_yearly_snapshots) > max_backup_snapshots):
        print(json.dumps({'severity': "ERROR", 'message': "The maximum number of backups is 32, please check the snapshots to keep in the runtime environment variables"}))
        return 'False'
    else:
        # Get the backup label based on the date
        my_max_snapshots = 0

        if (my_max_yearly_snapshots != 0 and datetime.today().month == 1 and datetime.today().day == 1):
            my_backup_label = my_yearly_label
            my_max_snapshots = int(my_max_yearly_snapshots)
        elif (my_max_monthly_snapshots != 0 and datetime.today().day == 1):
            my_backup_label = my_monthly_label
            my_max_snapshots = int(my_max_monthly_snapshots)
        elif (my_max_weekly_snapshots != 0 and datetime.today().weekday() == 6):
            my_backup_label = my_weekly_label
            my_max_snapshots = int(my_max_weekly_snapshots)
        elif (my_max_daily_snapshots != 0):
            my_backup_label = my_daily_label
            my_max_snapshots = int(my_max_daily_snapshots)

        if (my_max_snapshots == 0):
            print(json.dumps({'severity': "WARN", 'message': "A backup is not going to be invoked"}))
        else:
            my_id_token = get_token()

            # Get all volumes from all regions
            get_url = "https://cloudvolumesgcp-api.netapp.com/v2/projects/" + str(my_project_number) + "/locations/-/Volumes"

            # Construct GET request
            headers = {
                    "Content-Type": "application/json",
                    "Authorization": "Bearer " + my_id_token.decode('utf-8'),
                    "User-Agent": "cvs-backup-scheduler"
                    }

            # Issue the request to the server
            print(json.dumps({'severity': "DEBUG", 'message': "Getting all volumes from all regions"}))

            try:
                response = requests.get(get_url, headers=headers)
            except:
                print(json.dumps({'severity': "ERROR", 'message': "There was an error in the GET call " + get_url}))
                return 'False'

            if (response.status_code != 200):
                print(json.dumps({'severity': "ERROR", 'message': "There was an error in the GET call " + get_url}))
                return 'False'
            else:
                # Load the json response into a dict
                r_dict = response.json()
                # Remove the below comment to detailed troubleshooting
                # print(r_dict)
                print(json.dumps({'severity': "DEBUG", 'message': "Correct response to GET request: " + get_url}))

                for vol in r_dict:
                    # Get volume attributes
                    my_volume_id = vol["volumeId"]
                    my_volume_name = vol["name"]
                    my_volume_type = vol["storageClass"]
                    my_region = vol["region"]
                
                    if (my_volume_id in my_volume_list):

                        if (my_volume_type != "software"):
                            print(json.dumps({'severity': "ERROR", 'message': "Backing up the volume " + my_volume_name + " is not supported because it's not a CVS Software service type"}))
                        else:
                            print(json.dumps({'severity': "INFO", 'message': "Backing up the volume " + my_volume_name + " is in progress"}))
                            vol = CloudVolumes(my_project_number, my_region, my_volume_name, my_volume_id)
                            vol.rotate_backup(my_backup_label, my_max_snapshots, my_id_token)
                            vol.create_backup(my_backup_label, my_id_token)
                            print(json.dumps({'severity': "INFO", 'message': "---"}))

    print(json.dumps({'severity': "INFO", 'message': "--- END ---"}))

    return 'True'