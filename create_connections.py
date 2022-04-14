# !/bin/bin/env python

# %%

from urllib.request import Request, urlopen
import base64

auth = base64.b64encode(b"admin:admin")
headers = {"Content-Type": "application/json", "Authorization": f"Basic {auth}"}

base_url = "http://airflow-webserver.airflow:8080/api/v1"
conn_req = Request(f"{base_url}/connections", headers=headers)
conn_req.method = "GET"

content = urlopen(conn_req).read()
print(content)

# %%
