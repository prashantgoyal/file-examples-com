# Pagination and Notebook Usage

This document explains how to handle paginated REST APIs for ingestion and how to use the included fetch notebook.

Files added:
- `pipeline_with_pagination.json` — pipeline template showing pagination configs for `NextLink`, `PageNumber`, and `Token` APIs.
- `../RestAPI_Copy.Notebook/notebook-content.py` — a Fabric notebook that performs REST calls (supports `None`, `NextLink`, `PageNumber`, `Token`) and writes results to a Delta path.

Notebook usage
1. Edit `file-examples-com/RestAPI_Copy.Notebook/rest_config.json` (optional) with your runtime parameters, for example:

```json
{
  "api_url": "https://api.yourdomain.com/v1/orders",
  "method": "GET",
  "headers": { "Authorization": "Bearer <TOKEN>" },
  "pagination": { "type": "PageNumber" },
  "page_param": "page",
  "start_page": 1,
  "page_size_param": "pageSize",
  "page_size": 200,
  "destination_path": "Files/ingest/rest_orders",
  "items_json_path": "data.items",
  "next_link_json_path": "links.next"
}
```

2. Open the notebook `file-examples-com/RestAPI_Copy.Notebook/notebook-content.py` in Fabric or in your local editor and run it. The notebook will read `rest_config.json` if present and perform the full fetch + write.

Pipeline usage
- Use `pipeline_with_pagination.json` as a template to create a pipeline in Fabric; set `paginationType` when launching the copy activity.
- If your API requires token exchange or OAuth, prefer using Key Vault/Secrets for tokens and do not include secrets in config files.

Notes on pagination types
- `NextLink`: The API returns a `next` url in the response body that you should follow until empty.
- `PageNumber`: The API requires explicit `page` and `pageSize` parameters; iterate until an empty page.
- `Token`: The API returns a continuation token; include it in subsequent requests until no next token.

Security
- Use managed identity or store tokens in secure secrets. Do not store bearer tokens in source files.

Contact
- If you want, I can create an ADLS/OneLake shortcut and a sample Key Vault secret reference for secure runs.
