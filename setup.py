from connection import Connection
from datetime import datetime, timedelta
import json
import os
import queries
from typing import Tuple

'''This module controls "imports" and CONSTANTS creation.'''

def fetch_json_data(filepath: str) -> dict:
    '''Read external JSON file. Used primarily for broker feed
    configurations.'''
    return json.loads(open(filepath, 'r').read())

def get_configs(config_path: str) -> Tuple[dict, dict]:
    '''Read an external configurations file (configuratons.json), and
    return the vendors and the delivery schedules.'''
    configs = fetch_json_data(config_path)
    return configs['vendors'], configs['schedules']

def build_configs(config_path: str) -> dict:
    '''"Join" the vendors to their respective delivery schedule, with a
    twist: add entries only if the current date is in the delivery
    schedule.'''
    configs = get_configs(config_path)
    today = datetime.today().weekday()
    return {i: configs[0][i] for i in configs[0] if today in configs[1][configs[0][i]['schedule']]}

def get_templates():
    '''Get query templates for configured feeds, expected filemasks
    (i.e., configurations) and for actual filemasks (i.e., filemasks).'''
    QUERY_TEMPLATES = queries.QueriesFactory()
    config_template = QUERY_TEMPLATES.get_template('configs')
    expected_template = QUERY_TEMPLATES.get_template('expected')
    filemasks_template = QUERY_TEMPLATES.get_template('filemasks')
    return config_template, expected_template, filemasks_template

'''CONSTANTS'''
THIS_DIR = f'{os.path.dirname(os.path.abspath(__file__))}'
CONN = Connection()
QUERIES = queries.Queries()
TEMPLATES = get_templates()
CONFIGS = build_configs(f'{THIS_DIR}/configurations.json')
FILETYPES_TO_IGNORE = {
    'ZIP',
    'Reference'
}
THIRD_PARTY = {
    '00': 'Vendor',
    '01': 'Indirect',
    '02': 'Consolidated',
    '03': 'Commingled'
}