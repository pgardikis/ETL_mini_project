import pandas as pd
import requests

from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.models import Variable
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(minutes=1)
}

@dag(dag_id='national_parks_etl_dag',
     default_args=default_args,
     start_date=datetime(2023, 5, 7),
     schedule_interval='@daily')

def national_parks_etl_dag():
    @task(task_id='extract_api_data')
    def extract_api_data():
        parks_endpoint = "https://developer.nps.gov/api/v1/parks"
        alerts_endpoint = "https://developer.nps.gov/api/v1/alerts" 
 

        params = {'limit': 1000} 
        headers = {'X-Api-Key': Variable.get('national_parks_api_key')}

        response = requests.get(url=parks_endpoint, params=params, headers=headers)
        parks_data = response.json()

        alert_response = requests.get(url=alerts_endpoint, headers=headers)
        alerts_data = alert_response.json()

        return {'parks_data': parks_data, 'alerts_data': alerts_data}

    @task(task_id='transform_api_data')
    def transform_api_data(data):
        parks_data = data['parks_data']
        alerts_data = data['alerts_data']
        parks = []
        for park in parks_data['data']:
            if 'addresses' in park and park['addresses'] and 'city' in park['addresses'][0]:
                city = park['addresses'][0]['city']
            else:
                city = None
            parks.append({
                'FullName': park['fullName'],
                'States': park['states'],
                'City': city,
                'ParkCode': park['parkCode']
            })
        parks_df = pd.DataFrame(parks)

        activities = []
        for park in parks_data['data']:
            park_activities = []
            if 'activities' in park and park['activities'] and 'name' in park['activities'][0]:
                for activity in park['activities']:
                    park_activities.append(activity['name'])
            else:
                park_activities.append(None)
            activities.append({
                'ParkCode': park['parkCode'],
                'Activity': park_activities
            })
        activities_df = pd.DataFrame(activities).explode('Activity').reset_index(drop=True)

        alerts = []
        for alert in alerts_data['data']:
            alerts.append({
                'ParkCode': alert['parkCode'],
                'Title': alert['title'],
                'Category': alert['category'],
                'LastIndexedDate': alert['lastIndexedDate']
            })
        alerts_df = pd.DataFrame(alerts)
        
        return {'parks_df': parks_df, 'activities_df': activities_df, 'alerts_df': alerts_df}
    
    @task(task_id='load_data_to_bigquery')
    def load_data_to_bigquery(data_df):
        parks_df = data_df['parks_df']
        activities_df = data_df['activities_df']
        alerts_df = data_df['alerts_df']

        project_id = 'national-parks-etl-test'
        dataset_id = 'national_parks'

        parks_table = 'parks'
        parks_df.to_gbq(destination_table=f'{project_id}.{dataset_id}.{parks_table}', project_id=project_id, if_exists='replace')

        activities_table = 'activities'
        activities_df.to_gbq(destination_table=f'{project_id}.{dataset_id}.{activities_table}', project_id=project_id, if_exists='replace')

        alerts_table = 'alerts'
        alerts_df.to_gbq(destination_table=f'{project_id}.{dataset_id}.{alerts_table}', project_id=project_id, if_exists='replace')

    data = extract_api_data()
    data_df = transform_api_data(data)
    load_data_to_bigquery = load_data_to_bigquery(data_df)

national_parks_dag = national_parks_etl_dag()