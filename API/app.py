from flask import Flask, request, jsonify, make_response
from flask_sqlalchemy import SQLAlchemy
from os import environ
import schedule
import time
import random

app = Flask(__name__)
app.config['SQLALCHEMY_DATABASE_URI'] = environ.get('DB_URL')
db = SQLAlchemy(app)

#тестовые переменные
test_out_db = environ.get('DB_URL')
test_out_visit = []

def visit_gen():
  # Code to be executed every hour
  l1 = list(range(1, 55))
  l2 = list(range(1, 44))
  random.shuffle(l1)
  random.shuffle(l2)
  n1 = list(range(1, 6))
  for i,j,n in zip(l1, l2, n1):
    visit = str(f'{i} - {j}')
  return visit


schedule.every(1).minute.do(visit_gen)

#вывод результата выполнения функции visit_gen в виде строки через app.route
@app.route('/visit', methods=['GET'])
def visit():
  return make_response(jsonify({'visit': f'{visit_gen()}'}), 200)

#create a test route
@app.route('/test', methods=['GET'])
def test():
  return make_response(jsonify({'db': f'{test_out_db}', 'visit': f'{test_out_visit}'}), 200)