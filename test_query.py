import requests

try:
    resp = requests.post(
        "http://localhost:8000/api/query/crew",
        json={"question": "show me all users", "dialect": "mysql"}
    )
    print("Status code:", resp.status_code)
    print("Response:", resp.text)
except Exception as e:
    print("Exception:", e)
