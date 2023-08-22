import json


def get_json(element):
  j = json.loads(element.decode('utf-8'))
  return j['rows']

def extract_name(row):
  return get_json(row).name

def extract_number(row):
  return get_json(row).number