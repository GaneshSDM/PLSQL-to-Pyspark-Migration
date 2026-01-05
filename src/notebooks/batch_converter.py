# Databricks notebook source
# batch_converter.py - Batch SQL file conversion job

# COMMAND ----------

import os
import pandas as pd
from pyspark.sql import SparkSession
from ai_migration import MigrationAI

# COMMAND ----------

# Get job parameters from widgets
dbutils.widgets.text("input_folder", "/Workspace/Shared/sql-files/")
dbutils.widgets.text("output_folder", "/Workspace/Shared/converted-notebooks/")
dbutils.widgets.text("results_table", "main.migration.batch_results")
dbutils.widgets.text("dialect", "oracle")
dbutils.widgets.text("validation_strategy", "Validate by running EXPLAIN")
dbutils.widgets.dropdown("max_retries", "2", ["0", "1", "2", "3", "4", "5"])

input_folder = dbutils.widgets.get("input_folder")
output_folder = dbutils.widgets.get("output_folder")
results_table = dbutils.widgets.get("results_table")
dialect = dbutils.widgets.get("dialect")
validation_strategy = dbutils.widgets.get("validation_strategy")
max_retries = int(dbutils.widgets.get("max_retries"))

# COMMAND ----------

# Initialize AI migration
migration_ai = MigrationAI(api_key=dbutils.secrets.get(scope="migration", key="api_key"))

# COMMAND ----------

# Find all SQL files in input folder
sql_files = []
for root, dirs, files in os.walk(input_folder):
    for file in files:
        if file.endswith(('.sql', '.SQL')):
            sql_files.append(os.path.join(root, file))

print(f"Found {len(sql_files)} SQL files to process")

# COMMAND ----------

# Process each SQL file
results = []

for sql_file in sql_files:
    try:
        # Read SQL file
        with open(sql_file, 'r') as f:
            sql_content = f.read()

        # Convert SQL
        if "procedure" in sql_content.lower() or "begin" in sql_content.lower():
            result = migration_ai.migrate_procedure(sql_content)
        else:
            result = migration_ai.migrate_schema(sql_content)

        # Validate if requested
        validation_passed = True
        validation_error = None

        if validation_strategy != "No validation":
            try:
                # Run EXPLAIN to validate
                spark.sql(f"EXPLAIN {result}")
            except Exception as e:
                validation_passed = False
                validation_error = str(e)

                # Retry if enabled
                if "retried" in validation_strategy.lower() and max_retries > 0:
                    for attempt in range(max_retries):
                        try:
                            # Retry conversion with error context
                            retry_result = migration_ai.call_llama(
                                f"Previous conversion failed: {validation_error}\\n\\nPlease fix this SQL:\\n{result}"
                            )
                            spark.sql(f"EXPLAIN {retry_result}")
                            result = retry_result
                            validation_passed = True
                            validation_error = None
                            break
                        except Exception as retry_e:
                            validation_error = str(retry_e)

        # Save converted notebook
        notebook_name = os.path.basename(sql_file).replace('.sql', '_converted.py')
        notebook_path = os.path.join(output_folder, notebook_name)

        with open(notebook_path, 'w') as f:
            f.write(f"# Databricks notebook source\\n# Converted from {sql_file}\\n\\n# COMMAND ----------\\n\\n")
            f.write(f"spark.sql(\\\"\\\"\\\"\\n{result}\\n\\\"\\\"\\\")\\n")

        # Record result
        results.append({
            "file_path": sql_file,
            "notebook_path": notebook_path,
            "status": "success" if validation_passed else "failed",
            "validation_error": validation_error,
            "converted_at": pd.Timestamp.now()
        })

    except Exception as e:
        results.append({
            "file_path": sql_file,
            "notebook_path": None,
            "status": "error",
            "validation_error": str(e),
            "converted_at": pd.Timestamp.now()
        })

# COMMAND ----------

# Save results to Delta table
results_df = pd.DataFrame(results)
spark_df = spark.createDataFrame(results_df)

spark_df.write.mode("append").saveAsTable(results_table)

print(f"Batch conversion completed. Results saved to {results_table}")
print(f"Processed {len(results)} files")
print(f"Successful conversions: {len([r for r in results if r['status'] == 'success'])}")
print(f"Failed conversions: {len([r for r in results if r['status'] != 'success'])}")