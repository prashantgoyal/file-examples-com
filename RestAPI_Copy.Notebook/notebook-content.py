# Fabric notebook: REST API fetch and write to Delta
# META {
#   "kernel_info": { "name": "synapse_pyspark" },
#   "language": "python",
#   "language_group": "synapse_pyspark"
# }

from pyspark.sql import SparkSession
from pyspark.sql import types as T
import json
import time

# Config - override by editing this block or placing a JSON config at './rest_config.json'
CONFIG = {
    "api_url": "https://api.example.com/data",
    "method": "GET",
    "headers": {},
    "pagination": { "type": "None" },
    "destination_path": "Files/ingest/rest_api_data",
    "page_param": "page",
    "start_page": 1,
    "page_size_param": "pageSize",
    "page_size": 100,
    "next_link_json_path": "next",
    "items_json_path": "items"
}

# Try to load a local JSON config to override values
try:
    with open('rest_config.json','r',encoding='utf-8') as f:
        cfg = json.load(f)
        CONFIG.update(cfg)
except FileNotFoundError:
    pass

# Use requests when available, otherwise fallback to urllib
try:
    import requests
    _HAS_REQUESTS = True
except Exception:
    import urllib.request, urllib.parse
    _HAS_REQUESTS = False

spark = SparkSession.builder.appName("RestAPI_Fetch").getOrCreate()

def _extract_items_from_response(obj, items_path):
    # If response is array, return it
    if isinstance(obj, list):
        return obj
    # If items_path is a key in dict, return that
    if isinstance(obj, dict):
        # support dotted path like 'data.items'
        parts = items_path.split('.') if items_path else []
        cur = obj
        for p in parts:
            if p in cur:
                cur = cur[p]
            else:
                cur = None
                break
        if cur is None:
            # fallback: if dict has a single list value, return it
            for v in obj.values():
                if isinstance(v, list):
                    return v
            return []
        if isinstance(cur, list):
            return cur
        return [cur]
    return []


def _get_json(url, method='GET', headers=None, params=None, body=None, timeout=30):
    headers = headers or {}
    if _HAS_REQUESTS:
        r = requests.request(method, url, headers=headers, params=params, json=body, timeout=timeout)
        r.raise_for_status()
        return r.json()
    else:
        if params:
            url = url + ('?' + urllib.parse.urlencode(params))
        req = urllib.request.Request(url, headers=headers, method=method)
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            raw = resp.read()
            enc = resp.headers.get_content_charset() or 'utf-8'
            return json.loads(raw.decode(enc))


def fetch_and_write():
    cfg = CONFIG
    api_url = cfg['api_url']
    method = cfg.get('method','GET')
    headers = cfg.get('headers') or {}
    dest = cfg.get('destination_path')
    pagination = cfg.get('pagination',{})
    items_path = cfg.get('items_json_path','items')

    all_items = []

    ptype = (pagination.get('type') or 'None')

    if ptype == 'None' or not ptype:
        print(f"Fetching single-page API: {api_url}")
        data = _get_json(api_url, method=method, headers=headers)
        items = _extract_items_from_response(data, items_path)
        all_items.extend(items)

    elif ptype == 'NextLink':
        next_url = api_url
        next_path = cfg.get('next_link_json_path','next')
        iter_count = 0
        while next_url:
            iter_count += 1
            print(f"Fetching page {iter_count}: {next_url}")
            data = _get_json(next_url, method=method, headers=headers)
            items = _extract_items_from_response(data, items_path)
            all_items.extend(items)
            # find next link
            if isinstance(data, dict) and next_path in data and data[next_path]:
                next_url = data[next_path]
            else:
                # try common patterns
                next_url = data.get('next') if isinstance(data, dict) else None

    elif ptype == 'PageNumber':
        page = int(cfg.get('start_page',1))
        page_param = cfg.get('page_param','page')
        page_size_param = cfg.get('page_size_param','pageSize')
        page_size = int(cfg.get('page_size',100))
        while True:
            params = {page_param: page, page_size_param: page_size}
            print(f"Fetching page {page} with params {params}")
            data = _get_json(api_url, method=method, headers=headers, params=params)
            items = _extract_items_from_response(data, items_path)
            if not items:
                break
            all_items.extend(items)
            page += 1

    elif ptype == 'Token':
        token = None
        token_param = pagination.get('token_param','continuation')
        page_size_param = cfg.get('page_size_param','pageSize')
        page_size = int(cfg.get('page_size',100))
        while True:
            params = {page_size_param: page_size}
            if token:
                params[token_param] = token
            print(f"Fetching token page with params {params}")
            data = _get_json(api_url, method=method, headers=headers, params=params)
            items = _extract_items_from_response(data, items_path)
            all_items.extend(items)
            # get next token from response
            token = None
            if isinstance(data, dict):
                token = data.get(pagination.get('next_token_path','nextToken'))
            if not token:
                break

    else:
        raise ValueError(f"Unsupported pagination type: {ptype}")

    print(f"Total items fetched: {len(all_items)}")

    if not all_items:
        print('No items to write. Exiting.')
        return

    # Convert list of dicts to DataFrame and write to Delta
    try:
        df = spark.createDataFrame(all_items)
    except Exception as e:
        # try to infer schema by converting to RDD of rows
        print('Spark failed to create DataFrame directly, attempting manual schema inference', e)
        # best-effort: take keys from first item
        first = all_items[0]
        fields = [T.StructField(k, T.StringType(), True) for k in first.keys()]
        schema = T.StructType(fields)
        rows = [tuple(str(item.get(k,'')) for k in first.keys()) for item in all_items]
        df = spark.createDataFrame(rows, schema=schema)

    print(f"Writing {df.count()} records to delta path: {dest}")
    df.write.format('delta').mode('append').save(dest)
    print('Write complete')


if __name__ == '__main__':
    fetch_and_write()
