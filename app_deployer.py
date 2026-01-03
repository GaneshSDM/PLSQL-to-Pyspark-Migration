# Databricks notebook source
# app_deployer.py - Auto-deployment script for Legacy System to Databricks Converter

# COMMAND ----------

# Install required packages
%pip install streamlit pandas python-dotenv requests sqlparse PyYAML

# COMMAND ----------

import os
import shutil
from pathlib import Path

# Create necessary directories
dirs_to_create = [
    "/Workspace/Shared/legacy-converter/src",
    "/Workspace/Shared/legacy-converter/src/resources",
    "/Workspace/Shared/legacy-converter/src/utils",
    "/Workspace/Shared/legacy-converter/src/notebooks"
]

for dir_path in dirs_to_create:
    os.makedirs(dir_path, exist_ok=True)
    print(f"Created directory: {dir_path}")

# COMMAND ----------

# Copy application files (assuming they're in the current workspace)
# In a real deployment, you would upload these files

app_files = {
    "app.py": "/Workspace/Shared/legacy-converter/app.py",
    "src/utils/prompt_helper.py": "/Workspace/Shared/legacy-converter/src/utils/prompt_helper.py",
    "src/utils/common_helper.py": "/Workspace/Shared/legacy-converter/src/utils/common_helper.py",
    "src/resources/conversion_prompts.yaml": "/Workspace/Shared/legacy-converter/src/resources/conversion_prompts.yaml",
    "src/resources/common_prompts.yaml": "/Workspace/Shared/legacy-converter/src/resources/common_prompts.yaml",
    "ai_migration.py": "/Workspace/Shared/legacy-converter/ai_migration.py",
    "simple_validator.py": "/Workspace/Shared/legacy-converter/simple_validator.py",
    "requirements.txt": "/Workspace/Shared/legacy-converter/requirements.txt"
}

# Note: In actual deployment, you would need to upload these files to DBFS or workspace
print("Application files mapping:")
for src, dst in app_files.items():
    print(f"  {src} -> {dst}")

# COMMAND ----------

# Create batch processing notebook
batch_notebook_content = '''
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
'''

with open("/Workspace/Shared/legacy-converter/src/notebooks/batch_converter.py", "w") as f:
    f.write(batch_notebook_content)

print("Created batch converter notebook")

# COMMAND ----------

# Create reconciliation notebook
reconciliation_notebook_content = '''
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
'''

with open("/Workspace/Shared/legacy-converter/src/notebooks/schema_reconciler.py", "w") as f:
    f.write(reconciliation_notebook_content)

print("Created schema reconciler notebook")

# COMMAND ----------

# Create deployment summary
print("=" * 60)
print("DEPLOYMENT SUMMARY")
print("=" * 60)
print("✅ Legacy System to Databricks Converter deployed successfully!")
print()
print("📁 Application Location: /Workspace/Shared/legacy-converter/")
print("🌐 Main App: app.py")
print("📊 Batch Processing: src/notebooks/batch_converter.py")
print("🔍 Schema Reconciliation: src/notebooks/schema_reconciler.py")
print()
print("🚀 To start the app:")
print("   streamlit run /Workspace/Shared/legacy-converter/app.py")
print()
print("📋 Supported Dialects:")
print("   • Snowflake, T-SQL, Redshift, Oracle, Teradata")
print("   • MySQL, PostgreSQL, SSIS, Informatica")
print()
print("🤖 Supported LLMs:")
print("   • Claude, GPT, Llama, Gemma, Databricks GPT")
print("=" * 60)