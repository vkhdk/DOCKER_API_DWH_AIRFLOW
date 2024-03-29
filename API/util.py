from sqlalchemy import create_engine, Table, Column, Integer, Float, MetaData, Date, inspect, text
import requests
import datetime
import os
import datetime
import sys
import json
sys.path.append('../')
from GLOBAL_FILE_SHARE.default_settings import (DWH_USER, 
                              DWH_PASSWORD, 
                              DWH_HOST, 
                              dwh_db_name,
                              api_db_name,
                              source_schema,
                              gen_store_schema,
                              day_gen_store,
                              external_day_gen_store,
                              day_gen_visits_config,
                              users_table_name)

engine_dwh = create_engine(f'postgresql://{DWH_USER}:{DWH_PASSWORD}@{DWH_HOST}:5432/{dwh_db_name}', future=True)
engine_api = create_engine(f'postgresql://{DWH_USER}:{DWH_PASSWORD}@{DWH_HOST}:5432/{api_db_name}', future=True)

def write_default_params(**kwargs):
    #get variables from kwargs
    launched = kwargs['launched']
    source_tables = str(kwargs['source_tables']).replace('\'', '"')
    extra_columns_length_limit = kwargs['extra_columns_length_limit']
    gen_records = kwargs['gen_records']
    current_datetime = datetime.datetime.now()
    
    filling_default_params_query = (f"INSERT INTO {gen_store_schema}.{day_gen_visits_config} "
                                    f"(launched, source_schema, gen_store_schema, day_gen_store, "
                                    f"external_day_gen_store, source_tables, extra_columns_length_limit, gen_records, dt) "
                                    f"VALUES ({launched}, '{source_schema}', '{gen_store_schema}', '{day_gen_store}', '{external_day_gen_store}', "
                                    f"'{source_tables}', '{extra_columns_length_limit}', '{gen_records}', '{current_datetime}');"
                                   )
    with engine_api.begin() as conn:
        result = conn.execute(text(filling_default_params_query)) 
        conn.commit()
    print('Default parameters are written')

def update_gen_records_param(new_gen_records_values):
    launched = ''
    gen_records = ''
    if 'launched' in new_gen_records_values and 'gen_records' not in new_gen_records_values:
        if new_gen_records_values['launched'] in ['True', 'False']:
            launched = f"{new_gen_records_values['launched']} as "
            gen_records = f"" 
        else:
            print(f'Incorrect parameter "launched". Entered parameters -> {new_gen_records_values}')
    if 'gen_records' in new_gen_records_values and 'launched' not in new_gen_records_values:
        if isinstance(new_gen_records_values['gen_records'], int):
            gen_records = f"{(new_gen_records_values['gen_records'])} as " 
            launched = f""
        else:
            print(f'Incorrect parameter "gen_records". Entered parameters -> {new_gen_records_values}')
    if ('launched' in new_gen_records_values) and ('gen_records' in new_gen_records_values):
        if new_gen_records_values['launched'] in ['True', 'False'] and isinstance(new_gen_records_values['gen_records'], int):
            launched = f"{new_gen_records_values['launched']} as " 
            gen_records = f"{new_gen_records_values['gen_records']} as " 
        else:
            print(f'Incorrect parameters received. Entered parameters -> {new_gen_records_values}')

    current_datetime = datetime.datetime.now()
    insert_query = '''
    INSERT INTO {f_gen_store_schema}.{f_day_gen_visits_config} (launched,
                                                                source_schema,
                                                                gen_store_schema, 
                                                                day_gen_store, 
                                                                external_day_gen_store, 
                                                                source_tables, 
                                                                extra_columns_length_limit, 
                                                                gen_records, 
                                                                dt)
        WITH last_val as (
            SELECT  launched,
                    source_schema, 
                    gen_store_schema, 
                    day_gen_store, 
                    external_day_gen_store, 
                    source_tables, 
                    extra_columns_length_limit, 
                    gen_records,
                    dt
            FROM {f_gen_store_schema}.{f_day_gen_visits_config}
            ORDER BY dt DESC
            LIMIT 1)
        SELECT  {f_launched} launched,
                source_schema, 
                gen_store_schema, 
                day_gen_store, 
                external_day_gen_store, 
                source_tables, 
                extra_columns_length_limit, 
                {f_gen_records} gen_records,
                '{f_current_datetime}':: timestamptz as dt 
        FROM last_val
    '''
    insert_query = insert_query.format(f_gen_store_schema = gen_store_schema,
                                       f_day_gen_visits_config = day_gen_visits_config,
                                       f_launched = launched,
                                       f_gen_records = gen_records,
                                       f_current_datetime = current_datetime)
    with engine_api.begin() as conn:
        result = conn.execute(text(insert_query)) 
        conn.commit()

def get_last_param():
    result_dict = {}
    get_params_query = f'SELECT * FROM {gen_store_schema}.{day_gen_visits_config} ORDER BY {day_gen_visits_config}.dt DESC LIMIT 1;'
    with engine_api.connect() as connection:
        result = connection.execute(text(get_params_query))
        column_names = result.keys()
        for row in result:
            for column, row in zip(column_names, row):
                result_dict[column] = row
    return result_dict

def check_users_auth(login, password):    
    check_query = (f"SELECT {gen_store_schema}.{users_table_name}.password FROM {gen_store_schema}.{users_table_name} "
                   f"WHERE {gen_store_schema}.{users_table_name}.login = '{login}' ")
    with engine_api.begin() as conn:
        true_password = conn.execute(text(check_query)).fetchall()[0][0]
        conn.commit()
    if password == true_password:
        return True
    else:
        return False

def create_generated_data_store_table(**kwargs):
    #get variables from kwargs
    day_gen_store = kwargs['day_gen_store']
    gen_store_schema = kwargs['gen_store_schema']
    source_tables = kwargs['source_tables']
    #made sqlalchemy obj
    #create clean metadata
    metadata = MetaData()
    #create inspect
    inspector_db_api = inspect(engine_api)
    
    #generate the store table format from params
    day_gen_store_columns = []
    #add "id" and "date" attribute
    day_gen_store_columns.append(Column(f'id', Integer, primary_key=True))
    day_gen_store_columns.append(Column(f'date', Date))
    #add all attribute from params
    for table_name, columns in source_tables.items():
        for column_name, params in columns.items():
            day_gen_store_columns.append(Column(f'{table_name}_{column_name}', eval(params['data_type'])))
            
    day_gen_store_table = Table(f'{day_gen_store}', 
                                metadata, 
                                *day_gen_store_columns, 
                                schema=f'{gen_store_schema}')

    #checks
    try:
        day_gen_store_inspector = inspector_db_api.get_columns(table_name = day_gen_store,
                                                               schema = gen_store_schema)
        day_gen_store_columns_check = day_gen_store_table.columns.keys()
        day_gen_store_inspector_columns_check = [column['name'] for column in day_gen_store_inspector]
        if set(day_gen_store_columns_check) == set(day_gen_store_inspector_columns_check):
            print(f'table {day_gen_store} without columns changes')
        else:
            print(f'old columns - >> {day_gen_store_inspector_columns_check}')
            print(f'new columns - >> {day_gen_store_columns_check}')
            print(f'table {day_gen_store} columns changes')
            print(f'create all tables from metadata')
            metadata.drop_all(engine_api)
            metadata.create_all(engine_api)
    except:
        print(f'{day_gen_store} сolumn check failed')
        metadata.drop_all(engine_api)
        print(f'table {day_gen_store} not exist')
        print(f'create all tables from metadata')
        metadata.create_all(engine_api)
        
    #return metadata for next steps
    return metadata

def check_filling_generated_data_store_table(metadata, **kwargs):
    #get variables from kwargs
    day_gen_store = kwargs['day_gen_store']
    gen_store_schema = kwargs['gen_store_schema']
    #get current datetime
    current_date = datetime.date.today()
    #metadata from params
    metadata = metadata

    
    #tables from metadata
    day_gen_store_table = metadata.tables.get(day_gen_store)
    
    #check day_gen_store filling
    check_filling_day_gen_store_query = (f"SELECT COUNT(*) FROM {gen_store_schema}.{day_gen_store} "
                                        f"WHERE {gen_store_schema}.{day_gen_store}.date = '{current_date}'"
                                        )
    
    with engine_api.connect() as conn:
        check_filling_day_gen_store_query_result = conn.execute(text(check_filling_day_gen_store_query)).fetchall()
    
    if check_filling_day_gen_store_query_result[0][0] == 0:
        print(f'table {gen_store_schema}.{day_gen_store} does not contain records for the current day.\n'
              f'droping old records and generating new.'
             )
        day_gen_store_empty = True
    else: 
        day_gen_store_empty = False
    return day_gen_store_empty

def fill_generated_data_store_table(metadata_outside, **kwargs):
    #get variables from kwargs
    source_schema = kwargs['source_schema']
    gen_store_schema = kwargs['gen_store_schema']
    day_gen_store = kwargs['day_gen_store']
    gen_store_schema = kwargs['gen_store_schema']
    source_tables = kwargs['source_tables']
    extra_columns_length_limit = kwargs['extra_columns_length_limit']
    #get current datetime
    current_datetime = datetime.datetime.now()
    ex_tables_dict = {}
    ex_tables_name_dict = {}
    range_columns = {}
    day_gen_store_table = metadata_outside.tables[f'{gen_store_schema}.{day_gen_store}']
    day_gen_store_columns = day_gen_store_table.columns.keys()
    day_gen_store_columns.remove('id')
    day_gen_store_columns = ', '.join(day_gen_store_columns)
    metadata_local = MetaData()
    metadata_local.drop_all(engine_api)
    res_target = (f'INSERT INTO {gen_store_schema}.{day_gen_store} ({day_gen_store_columns}) SELECT CURRENT_DATE as date' )
    res_select = f''
    res_from = f''
    
    for table_name, columns in source_tables.items():
        meta_columns = []
        for column_name, params in columns.items():
            
            if params['function'] == 'random_range':
                with engine_dwh.begin() as conn_source:
                    get_range_query = f'SELECT min({column_name}), max({column_name}) FROM {source_schema}.{table_name};'
                    get_range_query_res = conn_source.execute(text(get_range_query)).fetchall()
                    res_select += f', (FLOOR(RANDOM() * ({get_range_query_res[0][1]} - {get_range_query_res[0][0]} + 1) + {get_range_query_res[0][0]})::numeric(5,2)) as {table_name}_{column_name} '
                conn_source.commit()  
                
            if params['function'] == 'all':
                if params['type'] == 'primary':
                    res_from += f' FROM {gen_store_schema}.ex_{table_name} '
                if params['type'] == 'extra':
                    res_from += f'CROSS JOIN {gen_store_schema}.ex_{table_name} '
                res_select += f', {gen_store_schema}.ex_{table_name}.{table_name}_{column_name} '
                
                meta_columns.append(Column(f'{table_name}_{column_name}', eval(params['data_type'])))
                ex_tables_dict['table'] = Table(f'ex_{table_name}',
                                                metadata_local, 
                                                *meta_columns,
                                                schema=f'{gen_store_schema}')
                metadata_local.create_all(engine_api)
                with engine_dwh.begin() as conn_source:
                    
                    if params['type'] == 'primary':
                        get_values_query = f'SELECT DISTINCT({column_name}) FROM {source_schema}.{table_name};'
                        get_values_query_res = conn_source.execute(text(get_values_query)).fetchall()
                        
                    if params['type'] == 'extra':
                        get_values_query = (f'SELECT ({column_name}) FROM {source_schema}.{table_name} ' 
                                            f'ORDER BY random() LIMIT {extra_columns_length_limit};')
                        get_values_query_res = conn_source.execute(text(get_values_query)).fetchall()
                    conn_source.commit()
            
                with engine_api.begin() as conn_target:
                    insert_values_query = (f'INSERT INTO {gen_store_schema}.ex_{table_name} ({table_name}_{column_name}) ' 
                                           f'VALUES {", ".join([f"({str(i[0])})" for i in get_values_query_res])};')
                    insert_values_query_res = conn_target.execute(text(insert_values_query))
                    
                    conn_target.commit()
   
    with engine_api.begin() as conn_target:
        fill_generated_data_store_table_query = (res_target + res_select + res_from + ';')
        fill_generated_data_store_table_res = conn_target.execute(text(fill_generated_data_store_table_query))
        conn_target.commit()
    metadata_local.drop_all(engine_api)

def fill_visits_table(**kwargs):
    source_schema = kwargs['source_schema']
    gen_store_schema = kwargs['gen_store_schema']
    day_gen_store = kwargs['day_gen_store']
    gen_store_schema = kwargs['gen_store_schema']
    gen_records = kwargs['gen_records']
    external_day_gen_store = kwargs['external_day_gen_store']
    incr_file_name = f'visits_incr_{datetime.datetime.now().strftime("%Y%m%d_%H%M")}.csv'
    create_fill_ex_table_query = '''
        CREATE TABLE {}.{} AS (
            SELECT id,
                   product_id, 
                   date as visit_date,
                   visits_line_size as line_size,
                   employers_id as employer_id,
                   shops_id as shop_id
            FROM ( 
                SELECT 
                dv.id,
                dv.employers_id,
                dv.date, 
                dv.visits_line_size, 
                dv.product_id, 
                dv.shops_id, 
                ROW_NUMBER() OVER (
                                PARTITION BY dv.employers_id 
                                ORDER BY RANDOM()) AS rn 
                FROM api.day_gen_visits dv) AS subquery 
            WHERE rn <= {});
    '''
    create_fill_ex_table_query = create_fill_ex_table_query.format(gen_store_schema, external_day_gen_store, gen_records)
    
    delete_day_gen_store_query = '''
        DELETE FROM {gen_store_schema_f}.{day_gen_store_f}
            WHERE EXISTS (
                SELECT 1
                FROM {gen_store_schema_f}.{external_day_gen_store_f}
                WHERE {gen_store_schema_f}.{day_gen_store_f}.id = {gen_store_schema_f}.{external_day_gen_store_f}.id);
    '''
    delete_day_gen_store_query = delete_day_gen_store_query.format(gen_store_schema_f = gen_store_schema, 
                                                                   day_gen_store_f = day_gen_store, 
                                                                   external_day_gen_store_f = external_day_gen_store)
    
    save_visits_incr_query = '''
        COPY (
            SELECT product_id, 
                   visit_date,
                   line_size,
                   employer_id,
                   shop_id
            FROM {}.{})
        TO '/var/lib/postgresql/GLOBAL_FILE_SHARE/{}' WITH CSV HEADER;
    '''
    save_visits_incr_query = save_visits_incr_query.format(gen_store_schema, external_day_gen_store, incr_file_name)
    drop_ex_table_query = f'DROP TABLE {gen_store_schema}.{external_day_gen_store};'
    
    load_visits_incr_query = '''
        COPY bd_shops.visits (product_id, 
                              visit_date, 
                              line_size, 
                              employer_id, 
                              shop_id)
        FROM '/var/lib/postgresql/GLOBAL_FILE_SHARE/{}'
        DELIMITER ','
        CSV HEADER;
    '''
    load_visits_incr_query = load_visits_incr_query.format(incr_file_name)
    
    with engine_api.begin() as conn_api:
        create_fill_ex_table_query_res = conn_api.execute(text(create_fill_ex_table_query))
        delete_day_gen_store_query_res = conn_api.execute(text(delete_day_gen_store_query))
        save_visits_incr_query_res = conn_api.execute(text(save_visits_incr_query))
        drop_ex_table_query_res = conn_api.execute(text(drop_ex_table_query))
        conn_api.commit()
    with engine_dwh.begin() as conn_dwh:
        load_visits_incr_query_res = conn_dwh.execute(text(load_visits_incr_query))
        conn_dwh.commit()
        
    relative_path = f"../GLOBAL_FILE_SHARE/{incr_file_name}"

    try:
        os.remove(relative_path)
        print(f"File {relative_path} has been deleted.")
    except FileNotFoundError:
        print(f"File {relative_path} does not exist.")
    except PermissionError:
        print(f"You do not have permission to delete the file {relative_path}.")
    except Exception as e:
        print(f"An error occurred while deleting the file {relative_path}: {str(e)}") 

def fill_visits_table_from_json(json_string):
    json_string = json_string.replace('\'', '"')
    rows_count = len(json.loads(json_string))
    json_string = json_string.replace(':', '\\:')
    formatted_json_string = json_string.strip()
    insert_query = '''
    INSERT INTO bd_shops.visits (product_id,
                                 visit_date, 
                                 line_size, 
                                 employer_id, 
                                 shop_id)
        SELECT * from json_populate_recordset(null::record,'{}')
            AS (product_id int,
                visit_date date,
                line_size float,
                employer_id int,
                shop_id int);
     '''
    insert_query = insert_query.format(formatted_json_string)
    with engine_dwh.begin() as conn_dwh:
        insert_query_res = conn_dwh.execute(text(insert_query))
        conn_dwh.commit()
    return rows_count

def delete_visits_table_from_json(json_string):
    json_string = json_string.replace('\'', '"')
    rows_in = len(json.loads(json_string))
    json_string = json_string.replace(':', '\\:')
    formatted_json_string = json_string.strip()
    delete_query = '''
    WITH sub_t AS (
        SELECT * from json_populate_recordset(null::record,'{}')
            AS (product_id int,
                visit_date date,
                line_size float,
                employer_id int,
                shop_id int))
    DELETE FROM bd_shops.visits as v
    USING sub_t
    WHERE sub_t.product_id = v.product_id
          AND sub_t.visit_date = v.visit_date
          AND sub_t.line_size = v.line_size
          AND sub_t.employer_id = v.employer_id
          AND sub_t.shop_id = v.shop_id
    RETURNING *
     '''
    delete_query = delete_query.format(formatted_json_string)
    with engine_dwh.begin() as conn_dwh:
        delete_query_res = conn_dwh.execute(text(delete_query))
        rows_deleted = len(delete_query_res.fetchall())
        conn_dwh.commit()
    return rows_in, rows_deleted