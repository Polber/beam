import json

def my_filter(value):
    obj = json.loads(value)
    return obj['name'] == "Bob"