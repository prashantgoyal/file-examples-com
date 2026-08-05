# PySpark notebook: execute DDL files to create tables in Source_data_0308 schema
# Place generated SQL files under the workspace `ddls/` folder before running.

try:
    spark
except NameError:
    from pyspark.sql import SparkSession
    spark = SparkSession.builder.appName("create_tables_ddls").getOrCreate()

import os
import glob

def execute_ddls(ddls_dir="ddls"):
    wd = os.getcwd()
    path = os.path.join(wd, ddls_dir)
    if not os.path.isdir(path):
        print(f"DDLs directory not found: {path}")
        return

    sql_files = sorted(glob.glob(os.path.join(path, "*.sql")))
    if not sql_files:
        print("No .sql files found in", path)
        return

    for f in sql_files:
        print(f"\n--- Executing file: {f}")
        sql_text = open(f, 'r', encoding='utf-8').read()
        # split on semicolons and execute each statement
        stmts = [s.strip() for s in sql_text.split(';') if s.strip()]
        for stmt in stmts:
            try:
                print("Running:", stmt.splitlines()[0][:200])
                spark.sql(stmt)
                print("OK")
            except Exception as e:
                print("FAILED:", e)

if __name__ == '__main__':
    # In Fabric run this cell; locally you can run `python notebook-content.py`
    execute_ddls('ddls')
