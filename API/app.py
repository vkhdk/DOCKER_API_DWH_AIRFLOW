from flask_sqlalchemy import SQLAlchemy
from flask import Flask, request, jsonify, make_response
from sqlalchemy import create_engine, Table, Column, Integer, String, MetaData, Float, Date, ForeignKey, inspect
from sqlalchemy import text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import schedule
import time
import datetime
import random
import os
import threading

from default_settings import (DWH_USER, 
                              DWH_PASSWORD, 
                              DWH_HOST, 
                              dwh_db_name,
                              api_db_name,
                              source_schema,
                              gen_store_schema,
                              day_gen_store,
                              external_day_gen_store,
                              day_gen_visits_config)

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False
engine_dwh = create_engine(f'postgresql://{DWH_USER}:{DWH_PASSWORD}@{DWH_HOST}:5432/{dwh_db_name}')
engine_api = create_engine(f'postgresql://{DWH_USER}:{DWH_PASSWORD}@{DWH_HOST}:5432/{api_db_name}')

default_params = {'source_tables': {"employers": {"id": {"type": "primary", "data_type": "Integer", "function": "all"}},
                                    "visits": {"line_size": {"type": "extra", "data_type": "Float", "function": "random_range"}},
                                    "product": {"id": {"type": "extra", "data_type": "Integer", "function": "all"}},
                                    "shops": {"id": {"type": "extra", "data_type": "Integer", "function": "all"}} },
                  'extra_columns_length_limit': 500,
                  'gen_records': 1}


#TEST

res_test = {}

def visit_gen():
  # Code to be executed every minute
  current_datetime = datetime.datetime.now()
  l1 = list(range(1, 55))
  l2 = list(range(1, 44))
  random.shuffle(l1)
  random.shuffle(l2)
  n1 = list(range(1, 6))
  for i,j,n in zip(l1, l2, n1):
    visit = str(f'{i} - {j}')
  res_test[current_datetime] = visit

# Schedule the visit_gen function to run every minute
schedule.every(1).minutes.do(visit_gen)

# Run the scheduled tasks in a separate thread
def run_schedule():
  while True:
    schedule.run_pending()
    time.sleep(1)

# Start the schedule thread
schedule_thread = threading.Thread(target=run_schedule)
schedule_thread.start()

@app.route('/visit', methods=['GET'])
def visit():
  return make_response(jsonify({'visit': f'{res_test}'}), 200)


#create a test route
@app.route('/test', methods=['GET'])
def test():
  return make_response(jsonify({'engine_dwh': f'{engine_dwh}', 'engine_api': f'{engine_api}', 'default_params': f'{default_params}'}), 200)