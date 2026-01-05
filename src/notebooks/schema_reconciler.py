# Databricks notebook source
# schema_reconciler.py - Schema reconciliation job

# COMMAND ----------

import pandas as pd
from pyspark.sql import SparkSession

# COMMAND ----------

# Get job parameters
dbutils.widgets.text("source_schema", "source_db.schema_name")
dbutils.widgets.text("target_schema", "main.target_schema")
dbutils.widgets.text("tables_list", "customers,orders,products")
dbutils.widgets.text("results_table", "main.migration.reconciliation_results")
dbutils.widgets.dropdown("enable_row_counts", "true", ["true", "false"])
dbutils.widgets.dropdown("enable_data_sampling", "false", ["true", "false"])

source_schema = dbutils.widgets.get("source_schema")
target_schema = dbutils.widgets.get("target_schema")
tables_list = dbutils.widgets.get("tables_list").split(",")
results_table = dbutils.widgets.get("results_table")
enable_row_counts = dbutils.widgets.get("enable_row_counts") == "true"
enable_data_sampling = dbutils.widgets.get("enable_data_sampling") == "true"

# COMMAND ----------

# Reconcile each table
reconciliation_results = []

for table in tables_list:
    table = table.strip()
    try:
        # Get source table info
        try:
            source_df = spark.table(f"{source_schema}.{table}")
            source_count = source_df.count() if enable_row_counts else None
            source_columns = source_df.columns
            source_dtypes = source_df.dtypes
        except Exception as e:
            reconciliation_results.append({
                "table_name": table,
                "status": "source_not_found",
                "error": str(e),
                "reconciled_at": pd.Timestamp.now()
            })
            continue

        # Get target table info
        try:
            target_df = spark.table(f"{target_schema}.{table}")
            target_count = target_df.count() if enable_row_counts else None
            target_columns = target_df.columns
            target_dtypes = target_df.dtypes
        except Exception as e:
            reconciliation_results.append({
                "table_name": table,
                "status": "target_not_found",
                "error": str(e),
                "reconciled_at": pd.Timestamp.now()
            })
            continue

        # Compare schemas
        column_match = set(source_columns) == set(target_columns)
        count_match = source_count == target_count if enable_row_counts else None

        # Data sampling if enabled
        data_sample_match = None
        if enable_data_sampling and len(source_df.head(100)) > 0:
            try:
                source_sample = source_df.limit(100).toPandas()
                target_sample = target_df.limit(100).toPandas()
                data_sample_match = source_sample.equals(target_sample)
            except:
                data_sample_match = False

        status = "match"
        if not column_match:
            status = "schema_mismatch"
        elif enable_row_counts and not count_match:
            status = "count_mismatch"
        elif enable_data_sampling and data_sample_match is False:
            status = "data_mismatch"

        reconciliation_results.append({
            "table_name": table,
            "status": status,
            "source_count": source_count,
            "target_count": target_count,
            "column_match": column_match,
            "count_match": count_match,
            "data_sample_match": data_sample_match,
            "reconciled_at": pd.Timestamp.now()
        })

    except Exception as e:
        reconciliation_results.append({
            "table_name": table,
            "status": "error",
            "error": str(e),
            "reconciled_at": pd.Timestamp.now()
        })

# COMMAND ----------

# Save results to Delta table
results_df = pd.DataFrame(reconciliation_results)
spark_df = spark.createDataFrame(results_df)

spark_df.write.mode("append").saveAsTable(results_table)

print(f"Schema reconciliation completed. Results saved to {results_table}")
print(f"Tables processed: {len(reconciliation_results)}")
matches = len([r for r in reconciliation_results if r['status'] == 'match'])
print(f"Matches: {matches}")
mismatches = len([r for r in reconciliation_results if r['status'] != 'match'])
print(f"Mismatches/Errors: {mismatches}")