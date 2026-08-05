**Data Quality Framework**: Simple PySpark-based DQ checks for Microsoft Fabric

- **Location**: dq/
- **Primary module**: dq/dq_framework.py
- **Notebook**: DataQuality.Notebook/notebook-content.xml
- **Config**: dq/dq_checks.json (tables + checks)

Usage

- Edit `dq/dq_checks.json` to add tables and checks.
- Run the notebook in Fabric (PySpark runtime) or locally with a SparkSession.
- Configure `DQ_REPORT_PATH` and `DQ_CONFIG_PATH` env vars to change output and config locations.

Built-in checks

- `row_count_gt`: ensure table has > value rows
- `null_pct_lt`: ensure null percentage for a column is below threshold (pct)
- `unique_keys`: verify uniqueness count on specified columns
- `value_range`: count rows outside a numeric range
- `regex`: check values against regex pattern

Next steps

- Add connectors to write results into a Lakehouse table.
- Add Activator/alert templates for notification on failures.
- Integrate into a scheduled pipeline `DailyDataQuality`.
