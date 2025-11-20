import requests

session = requests.Session()
session.headers.update({"Authorization": "Bearer TOKEN123"})

response1 = session.get("https://httpbin.org/get")
response2 = session.get("https://httpbin.org/headers")

print(response1.status_code, response2.status_code)
