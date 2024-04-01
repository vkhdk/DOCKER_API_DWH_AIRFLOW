# DOCKER_API_DWH_AIRFLOW

# Airflow
## Default authorization
- User: airflow
- Password airflow

# PGAdmin
## Default authorization
- Master Password: pgadminpswrd
## Connect to DWH:
- Host: dwh_postgres_container
- Port: 5432
- User: dwh_postgres
- Password: dwh_postgres

# API
## Users for authorization
- User: ivan
- Password: ivan123
- User: masha
- Password: masha321

## Python code examples for working with API

```
import requests

# write a new value for the number of generated records
url = 'http://localhost:4000/update_param'
data = {"gen_records_params": {"launched": 'True', 'gen_records': 2}}
response = requests.post(url, json=data, auth=('ivan', 'ivan123'))
print(response.text)

# example string to write\delete visits
json_string = '''
[{'product_id':4864,'visit_date':'2022-11-01','line_size':39.08,'employer_id':438,'shop_id':30}, 
 {'product_id':466,'visit_date':'2022-03-22','line_size':114.01,'employer_id':42,'shop_id':10}, 
 {'product_id':5856,'visit_date':'2023-04-10','line_size':88.47,'employer_id':211,'shop_id':6}, 
 {'product_id':691,'visit_date':'2022-05-10','line_size':3.53,'employer_id':390,'shop_id':10}, 
 {'product_id':4178,'visit_date':'2022-06-30','line_size':104.97,'employer_id':226,'shop_id':37}, 
 {'product_id':5835,'visit_date':'2022-10-17','line_size':49.24,'employer_id':317,'shop_id':15}, 
 {'product_id':4333,'visit_date':'2022-07-14','line_size':137.06,'employer_id':13,'shop_id':20}, 
 {'product_id':667,'visit_date':'2023-02-18','line_size':133.87,'employer_id':101,'shop_id':6}, 
 {'product_id':9204,'visit_date':'2023-05-12','line_size':45.1,'employer_id':24,'shop_id':10}, 
 {'product_id':1349,'visit_date':'2023-05-06','line_size':89.88,'employer_id':163,'shop_id':38}]
 '''
# write visits from string
url = 'http://localhost:4000/write_visits_from_json'
data = {"visits_data": f"{json_string}"}
response = requests.post(url, json=data, auth=('ivan', 'ivan123'))
print(response.text)

# delete visits from string
url = 'http://localhost:4000/delete_visits_from_json'
data = {"visits_data": f"{json_string}"}
response = requests.post(url, json=data, auth=('ivan', 'ivan123'))
print(response.text)

# filter field example
products_text = 'Помидоры, Креветки'
start_date = '2026-12-30'
end_date = '2024-12-31'

# get data from dm by filters
url = 'http://localhost:4000/get_data_dm_revizion'
data = {"dm_revizion_data": {"products_text": products_text, 'start_date': start_date, 'end_date': end_date}}
response = requests.post(url, json=data, auth=('ivan', 'ivan123'))
print(response.text)
```