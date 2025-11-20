import requests
from requests import RequestException

URL = "https://jsonplaceholder.typicode.com"
TIMEOUT = 10

def get_todos(user_id):
    params = {}
    if user_id is not None:
        params["user_id"] = user_id

    try:
        responses = requests.get(f"{URL}/todos", params=params, timeout=TIMEOUT)
        responses.raise_for_status()
        return responses.json()
    except RequestException as e:
        raise e

def get_todo(user_id):
    params = {}
    if user_id is not None:
        params["user_id"] = user_id

    try:
        response = requests.get(f"{URL}/todos/{user_id}", params=params, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        raise e

def create_todo(user_id, title, completed):
    insert = {"user_id": user_id, "title": title, "completed": completed}

    try:
        response = requests.post(f"{URL}/todos", json=insert, timeout=TIMEOUT)
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        raise e