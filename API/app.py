from flask_sqlalchemy import SQLAlchemy
from flask import Flask, request, jsonify, make_response
import schedule
import time
import threading

from util import (write_default_params,
                  get_last_param,
                  create_generated_data_store_table,
                  check_filling_generated_data_store_table,
                  fill_generated_data_store_table,
                  fill_visits_table)

app = Flask(__name__)
app.config['JSON_AS_ASCII'] = False

default_params = {'source_tables': {"employers": {"id": {"type": "primary", "data_type": "Integer", "function": "all"}},
                                    "visits": {"line_size": {"type": "extra", "data_type": "Float", "function": "random_range"}},
                                    "product": {"id": {"type": "extra", "data_type": "Integer", "function": "all"}},
                                    "shops": {"id": {"type": "extra", "data_type": "Integer", "function": "all"}} },
                  'extra_columns_length_limit': 500,
                  'launched': True,
                  'gen_records': 1}


write_default_params(**default_params)


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

visits_generator()
# Schedule the visits_generator function to run every minute
schedule.every(5).minutes.do(visits_generator)

# Run the scheduled tasks in a separate thread
def run_schedule():
  while True:
    schedule.run_pending()
    time.sleep(1)

# Start the schedule thread
schedule_thread = threading.Thread(target=run_schedule)
schedule_thread.start()


#test routes
@app.route('/visit', methods=['GET'])
def visit():
  return make_response(jsonify({'visit': f'{res_test}'}), 200)

@app.route('/test', methods=['GET'])
def test():
  return make_response(jsonify({'engine_dwh': f'{engine_dwh}', 'engine_api': f'{engine_api}', 'default_params': f'{default_params}'}), 200)