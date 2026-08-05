Schema inference and DDL generation for Lakehouse

Place your CSV files in this workspace (or provide full paths) and run the PowerShell helper to generate CREATE TABLE DDLs under the `ddls/` folder.

Steps:

1. Open PowerShell in the workspace root.
2. (Optional) Install Python 3.8+ and ensure `python` is on PATH.
3. Run:

```powershell
python -m pip install pandas python-dateutil
.\generate_ddls.ps1
```

4. Review generated SQL files in `ddls/`. They will create schema `Source_data_0308` and tables named after CSV filenames.

How to apply to Fabric Lakehouse `Data_load_03082026`:
- Open the Lakehouse SQL editor in Fabric (Data_load_03082026) and paste the generated CREATE TABLE statements, or
- Use Fabric REST / MCP tooling to execute the DDLs programmatically (requires tenant auth/token).

If you want, provide the CSVs here or paste sample rows and I'll run inference and produce DDLs for you. If you want me to execute the DDLs against Fabric, provide an API token and confirm the exact Lakehouse item id and that you permit me to perform the operation (note: I currently cannot access Fabric on your behalf without credentials).