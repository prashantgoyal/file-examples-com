# RestAPI_Copy Pipeline

This pipeline copies JSON data from a REST API into a Lakehouse Delta path.

Files:
- `pipeline.json` – pipeline definition with parameters.

Parameters (in `pipeline.json`):
- `apiUrl` (string): The REST endpoint to call. Default: `https://api.example.com/data`
- `apiMethod` (string): HTTP method to use (GET/POST). Default: `GET`
- `apiHeaders` (object): Optional HTTP headers (e.g. Authorization). Default: `{}`
- `destinationPath` (string): Destination lakehouse path (relative to lakehouse Files). Default: `Files/ingest/rest_api_data`

Notes and usage
1. Edit `pipeline.json` to set `apiUrl`, or pass parameters when running the pipeline in Fabric.
2. If the REST endpoint requires authentication, set `apiHeaders` to include the required Authorization header (prefer managed identity or secrets stored in Key Vault/Secrets store inside Fabric).
3. The pipeline writes JSON to the `destinationPath` as Delta; you can change `format` under `sink` to `parquet` if desired.

Deployment and run
- Import or create a new pipeline in Fabric using the contents of `pipeline.json` (Fabric UI or REST API).
- Provide runtime values for `apiUrl` and any headers/secrets.
- Trigger the pipeline run and monitor the `CopyFromRestApi` activity for failures.

Pagination
- Currently the pipeline uses `pagination.type = None`. If your API pages results,
  replace the pagination block with the appropriate pagination configuration supported by Fabric (e.g., nextLink header, skip/token, or page-size parameters).

Security
- Do NOT hardcode secrets in the pipeline JSON. Use Fabric secrets or Key Vault and reference them at runtime.

Troubleshooting
- If the copy fails with authentication errors, verify headers and token scopes.
- If no rows are copied, check the API response shape and update the `format` or pre-processing to match the expected schema.

Example parameter override JSON (when launching the pipeline):

{
  "apiUrl": "https://api.yourdomain.com/v1/orders",
  "apiMethod": "GET",
  "apiHeaders": { "Authorization": "Bearer <TOKEN>" },
  "destinationPath": "Files/ingest/rest_orders"
}
