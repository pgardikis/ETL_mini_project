# ETL mini-project

This repo contains the code of a mini-project I did in order to become familiar with ETL process.

## Description
I used Apache Airflow for the ETL pipeline. I created a DAG with three tasks:
- Extract data from [National Parks API](https://www.nps.gov/subjects/developer/api-documentation.htm) containting information about parks, activities and alerts
- Transform the data and create three dataframes using Python
- Load transformed data in BigQuery where I had created a dataset with three tables(parks, activities, alerts)

I used BigQuery to query the data and extract useful insights from it.
