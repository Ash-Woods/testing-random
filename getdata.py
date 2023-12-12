import requests
import json
import base64
import boto3
from datetime import datetime

def get_secret(secret_name, region_name='eu-west-1'):
    client = boto3.client(service_name='secretsmanager', region_name=region_name)
    try:
        get_secret_value_response = client.get_secret_value(SecretId=secret_name)
        secret = json.loads(get_secret_value_response['SecretString'])
        return secret
    except Exception as e:
        print(f"Error retrieving secret: {str(e)}")
        return None

def get_project_details(base_url, project_id, username, apikey):
    credentials = f"{username}:{apikey}"
    headers = {
        'Authorization': f'Basic {base64.b64encode(credentials.encode()).decode()}',
        'Content-Type': 'application/json'
    }

    url = f"{base_url}/get_project/{project_id}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching details for project {project_id}: {response.text}")
        return None

def get_testrail_runs(base_url, project_id, username, apikey):
    credentials = f"{username}:{apikey}"
    headers = {
        'Authorization': f'Basic {base64.b64encode(credentials.encode()).decode()}',
        'Content-Type': 'application/json'
    }

    url = f"{base_url}/get_runs/{project_id}"
    response = requests.get(url, headers=headers)
    if response.status_code == 200:
        return response.json()
    else:
        print(f"Error fetching data for project {project_id}: {response.text}")
        return None

def save_to_s3(bucket_name, data, path):
    s3 = boto3.client('s3', region_name='eu-west-1')
    try:
        s3.put_object(Bucket=bucket_name, Key=path, Body=json.dumps(data))
        print(f"Data successfully stored in {bucket_name}/{path}")
    except Exception as e:
        print(f"Error storing data in S3: {str(e)}")

def lambda_handler(event, context):
    secret_name = "dwh-testrail-credentials"
    base_url = "https://org.testrail.io/index.php?/api/v2"
    credentials = get_secret(secret_name)
    if not credentials:
        print('Failed to retrieve API credentials.')
        return {'statusCode': 500, 'body': json.dumps('Failed to retrieve API credentials.')}

    aggregated_data = {}  # Initialize an empty dictionary to store aggregated data

    for project_id in range(1, 101):  # Iterate through project IDs up to 100
        project_details = get_project_details(base_url, project_id, credentials['email'], credentials['apikey'])
        if project_details:
            runs_data = get_testrail_runs(base_url, project_id, credentials['email'], credentials['apikey'])
            if runs_data and runs_data.get('runs'):
                project_data = {
                    'project_name': project_details['name'],
                    'runs': runs_data
                }
                aggregated_data[f"project_{project_id}"] = project_data  # Store project name and runs data

    # Check if aggregated data is not empty
    if aggregated_data:
        current_date = datetime.now()
        date_path = current_date.strftime("%Y/%m/%d")
        file_name_date = current_date.strftime("%Y%m%d")
        path = f"raw-data/testrail/{date_path}/{file_name_date}_testrail_data.json"
        save_to_s3('dlx-datalake', aggregated_data, path)
    else:
        print("No data found for any projects.")

    return {'statusCode': 200, 'body': json.dumps('Data fetching and aggregation completed.')}

# End of the script
