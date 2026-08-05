import json
import os
import sys
import urllib.error
import urllib.request

WORKSPACE_ID = "ea27d82a-4c32-431e-b993-f089647e7e0c"
PIPELINE_ID = "d8915bc1-68c5-4d9c-9eec-1c1a84464592"
FABRIC_ENDPOINT = "https://centralindia-api.onelake.fabric.microsoft.com"

TOKEN = (
    os.getenv("FABRIC_TOKEN")
    or os.getenv("AZURE_ACCESS_TOKEN")
    or os.getenv("ACCESS_TOKEN")
    or os.getenv("AZURE_TOKEN")
)

if not TOKEN:
    print("No bearer token found in environment. Set FABRIC_TOKEN or AZURE_ACCESS_TOKEN.")
    sys.exit(1)

notebook_reference = sys.argv[1] if len(sys.argv) > 1 else "Data_load_03082026.Notebook"
activity_name = sys.argv[2] if len(sys.argv) > 2 else "RunDailySalesNotebook"

url = f"{FABRIC_ENDPOINT}/workspaces/{WORKSPACE_ID}/pipelines/{PIPELINE_ID}"

headers = {
    "Authorization": f"Bearer {TOKEN}",
    "Content-Type": "application/json"
}

# Fetch existing pipeline definition
req_get = urllib.request.Request(url, headers=headers, method="GET")
try:
    with urllib.request.urlopen(req_get) as resp:
        pipeline_body = json.loads(resp.read().decode("utf-8"))
except urllib.error.HTTPError as err:
    print(f"GET HTTP {err.code}: {err.reason}")
    print(err.read().decode("utf-8"))
    sys.exit(1)
except urllib.error.URLError as err:
    print(f"GET request failed: {err}")
    sys.exit(1)

if "activities" not in pipeline_body or not isinstance(pipeline_body["activities"], list):
    pipeline_body["activities"] = []

new_activity = {
    "name": activity_name,
    "type": "Notebook",
    "typeProperties": {
        "notebook": {
            "referenceName": notebook_reference,
            "type": "NotebookReference"
        },
        "baseParameters": {}
    }
}

# Avoid duplicate activity names
existing_names = {act.get("name") for act in pipeline_body["activities"]}
if activity_name in existing_names:
    print(f"Activity '{activity_name}' already exists in pipeline. No changes made.")
    sys.exit(0)

pipeline_body["activities"].append(new_activity)

req_put = urllib.request.Request(
    url,
    data=json.dumps(pipeline_body).encode("utf-8"),
    headers=headers,
    method="PUT"
)

try:
    with urllib.request.urlopen(req_put) as resp:
        body = resp.read().decode("utf-8")
        print("Pipeline updated successfully.")
        print(body)
except urllib.error.HTTPError as err:
    print(f"PUT HTTP {err.code}: {err.reason}")
    print(err.read().decode("utf-8"))
    sys.exit(1)
except urllib.error.URLError as err:
    print(f"PUT request failed: {err}")
    sys.exit(1)
