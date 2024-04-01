from flask_sqlalchemy import SQLAlchemy
from flask import Flask, request, jsonify, make_response
from sqlalchemy import create_engine, Table, Column, Integer, Float, MetaData, Date, inspect, text
import schedule
import time
import threading
import json
import datetime
import logging

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
                  delete_visits_table_from_json,
                  get_data_dm_revizion
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

# Create a logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a file handler
file_handler = logging.FileHandler('../GLOBAL_FILE_SHARE/app_log.log')
file_handler.setLevel(logging.INFO)

# Create a formatter and add it to the file handler
formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
file_handler.setFormatter(formatter)

# Add the file handler to the logger
logger.addHandler(file_handler)

# Authentication decorator
def authenticate(func):
    def wrapper(*args, **kwargs):
        auth = request.authorization
        if not auth or not check_users_auth(auth.username, auth.password):
            logger.warning(f'Unauthorized user entered username -> {auth.username}')
            return make_response("Unauthorized", 401, {"WWW-Authenticate": 'Basic realm="Login Required"'})
        return func(*args, **kwargs)
    # Renaming the function name:
    wrapper.__name__ = func.__name__
    return wrapper

@app.route('/update_param', methods=['POST'])
@authenticate
def get_load_gen_records_param():
    gen_records_params = request.json.get('gen_records_params')
    if 'launched' in gen_records_params and 'gen_records' not in gen_records_params:
        if gen_records_params['launched'] in ['True', 'False']:
            launched = f"{gen_records_params['launched']} as "
            gen_records = f""
            update_gen_records_param(launched, gen_records)
            result = (f'Parameters updated. New parameters >{gen_records_params}<')
        else:
            result = (f'Incorrect parameter "launched". Entered parameters -> {gen_records_params}')
    if 'gen_records' in gen_records_params and 'launched' not in gen_records_params:
        if isinstance(gen_records_params['gen_records'], int):
            gen_records = f"{(gen_records_params['gen_records'])} as " 
            launched = f""
            update_gen_records_param(launched, gen_records)
            result = (f'Parameters updated. New parameters >{gen_records_params}<')
        else:
            result = (f'Incorrect parameter "gen_records". Entered parameters -> {gen_records_params}')
    if ('launched' in gen_records_params) and ('gen_records' in gen_records_params):
        if gen_records_params['launched'] in ['True', 'False'] and isinstance(gen_records_params['gen_records'], int):
            launched = f"{gen_records_params['launched']} as " 
            gen_records = f"{gen_records_params['gen_records']} as " 
            update_gen_records_param(launched, gen_records)
            result = (f'Parameters updated. New parameters >{gen_records_params}<')
        else:
            result = (f'Incorrect parameters received. Entered parameters -> {gen_records_params}')

    logger.info(f'{result}')
    return make_response(jsonify({'{message': f'{result}'}), 200)

@app.route('/write_visits_from_json', methods=['POST'])
@authenticate
def get_visits_data_write():
    visits_data = request.json.get('visits_data')
    rows_count = fill_visits_table_from_json(visits_data)
    logger.info(f'Successfully added visits data from api. Added data -> {visits_data} rows')
    return make_response(jsonify({'message': f'Successfully added {rows_count} rows'}), 200)

@app.route('/delete_visits_from_json', methods=['POST'])
@authenticate
def get_visits_data_delete():
    visits_data = request.json.get('visits_data')
    rows_counts = delete_visits_table_from_json(visits_data)
    rows_in = rows_counts[0]
    rows_deleted = rows_counts[1]
    logger.info(f'Successfully deleted visits data from api. Deleted data -> {visits_data}')
    return make_response(jsonify({'message': f'{rows_in} rows entered. Successfully deleted {rows_deleted} rows'}), 200)

@app.route('/get_data_dm_revizion', methods=['POST'])
@authenticate
def get_dm_revizion():
  entered_data = request.json.get('dm_revizion_data')
  if 'products_text' in entered_data and 'start_date' in entered_data and 'end_date' in entered_data:
    try:
      start_date = datetime.datetime.strptime(entered_data['start_date'], "%Y-%m-%d")
      end_date = datetime.datetime.strptime(entered_data['end_date'], "%Y-%m-%d")
      if start_date > end_date:
        logger.info(f'get_data_dm_revizion problem. Start_date parameter must be less than end_date. Current parameters -> {entered_data}')
        result = f"Start_date parameter must be less than end_date. Current parameters -> {entered_data}"
      else:
        result = get_data_dm_revizion(entered_data['products_text'], entered_data['start_date'], entered_data['end_date'])
        if not result:
          logger.info(f'get_data_dm_revizion problem. No data found. Check inputed parameters. Current parameters -> {entered_data}')
          result = f"No data found. Check inputed parameters. Current parameters -> {entered_data}"
    except:
      logger.info(f'get_data_dm_revizion problem. The date format should be like "YYYY-MM-DD". Current parameters -> {entered_data}')
      result = f"The date format should be like 'YYYY-MM-DD'. Current parameters -> {entered_data}"
  else:
    logger.info(f'get_data_dm_revizion problem. All three parameters must be provided. Current parameters -> {entered_data}')
    result = f"All three parameters must be provided. Current parameters -> {entered_data}"
  return make_response(jsonify({'message': f'{result}'}), 200)

#######
# test route with out authentication
@app.route('/test', methods=['GET'])
def test():
  return make_response(jsonify({'default_params': f'{default_params}'}), 200)
#######

def visits_generator():
  logger.info('Visits_generator started')
  current_params = get_last_param()
  if current_params['launched'] == True:
    metadata = create_generated_data_store_table(**current_params)
    if check_filling_generated_data_store_table(metadata, **current_params) == True:
      fill_generated_data_store_table(metadata, **current_params)
    else:
      logger.info('generated_data_store_table is full')
      print('generated_data_store_table is full')
    fill_visits_table(**current_params)
  else:
    logger.info('Visits_generator is not launched')
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
logger.info('First run of the visits_generator after the API started')
# Schedule the visits_generator function to run every 60 minute
schedule.every(60).minutes.do(visits_generator)

# Run the scheduled tasks in a separate thread
def run_schedule():
  logger.info('API scheduler started')
  while True:
    schedule.run_pending()
    time.sleep(1)

# Start the schedule thread
schedule_thread = threading.Thread(target=run_schedule)
schedule_thread.start()