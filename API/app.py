from flask_sqlalchemy import SQLAlchemy
from flask import Flask, request, jsonify, make_response
from sqlalchemy import create_engine, Table, Column, Integer, Float, MetaData, Date, inspect, text
import schedule
import time
import threading
import json

from util import (write_default_params,
                  get_last_param,
                  create_generated_data_store_table,
                  check_filling_generated_data_store_table,
                  fill_generated_data_store_table,
                  fill_visits_table,
                  update_gen_records_param,
                  engine_dwh,
                  engine_api,
                  check_users_auth,
                  fill_visits_table_from_json,
                  delete_visits_table_from_json
                  )

default_params = {'source_tables': 
                  {"employers": {"id": {"type": "primary", "data_type": "Integer", "function": "all"}},
                   "visits": {"line_size": {"type": "extra", "data_type": "Float", "function": "random_range"}},
                   "product": {"id": {"type": "extra", "data_type": "Integer", "function": "all"}},
                   "shops": {"id": {"type": "extra", "data_type": "Integer", "function": "all"}} },
                  'extra_columns_length_limit': 500,
                  'launched': True,
                  'gen_records': 1}

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

# Authentication decorator
def authenticate(func):
    def wrapper(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_users_auth(auth.username, auth.password):
            return make_response("Unauthorized", 401, {"WWW-Authenticate": 'Basic realm="Login Required"'})
        return func(*args, **kwargs)
    # Renaming the function name:
    wrapper.__name__ = func.__name__
    return wrapper

@app.route('/update_param', methods=['POST'])
@authenticate
def get_load_gen_records_param():
    gen_records_params = request.json.get('gen_records_params')
    update_gen_records_param(gen_records_params)
    return make_response(jsonify({'message': f'Parameters updated. New parameters >{gen_records_params}<'}), 200)

@app.route('/write_visits_from_json', methods=['POST'])
@authenticate
def get_visits_data_write():
    visits_data = request.json.get('visits_data')
    rows_count = fill_visits_table_from_json(visits_data)
    return make_response(jsonify({'message': f'Successfully added {rows_count} rows'}), 200)

@app.route('/delete_visits_from_json', methods=['POST'])
@authenticate
def get_visits_data_delete():
    visits_data = request.json.get('visits_data')
    rows_counts = delete_visits_table_from_json(visits_data)
    rows_in = rows_counts[0]
    rows_deleted = rows_counts[1]
    return make_response(jsonify({'message': f'{rows_in} rows entered. Successfully deleted {rows_deleted} rows'}), 200)

#test routes

@app.route('/test', methods=['GET'])
def test():
  return make_response(jsonify({'engine_dwh': f'{engine_dwh}', 'engine_api': f'{engine_api}', 'default_params': f'{default_params}'}), 200)

def visits_generator():
  current_params = get_last_param()
  if current_params['launched'] == True:
    metadata = create_generated_data_store_table(**current_params)
    if check_filling_generated_data_store_table(metadata, **current_params) == True:
      fill_generated_data_store_table(metadata, **current_params)
    else:
      print('Table is full')
    fill_visits_table(**current_params)
  else:
    print('Generator is not launched')

#before the first run is processed
conn_test_res = ''
while conn_test_res!='OK':
  try :
    with engine_api.begin() as conn_api:
      result_conn_api = conn_api.execute(text('SELECT now();')) 
      conn_api.commit()
    with engine_dwh.begin() as conn_dwh:
      result_conn_dwh = conn_dwh.execute(text('SELECT now();')) 
      conn_dwh.commit()
    conn_test_res = 'OK'
  except Exception as e:
    res_test = f'Error: {e}'
    time.sleep(5)


# Set the default parameters
write_default_params(**default_params)
# First run of the visits_generator function
visits_generator()
# Schedule the visits_generator function to run every 5 minute
schedule.every(5).minutes.do(visits_generator)

# Run the scheduled tasks in a separate thread
def run_schedule():
  while True:
    schedule.run_pending()
    time.sleep(1)

# Start the schedule thread
schedule_thread = threading.Thread(target=run_schedule)
schedule_thread.start()