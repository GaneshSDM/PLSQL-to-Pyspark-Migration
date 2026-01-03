import os

def get_model_params():
    """
    Get model parameters for Databricks LLM calls.
    """
    return {
        "temperature": 0.1,
        "max_tokens": 4096,
        "top_p": 0.1,
        "stop": ["---"]
    }

def classify_sql(sql):
    """
    Classify the SQL type: ddl, dml, procedure, etc.
    """
    sql_upper = sql.upper().strip()
    if sql_upper.startswith("CREATE") or sql_upper.startswith("ALTER") or sql_upper.startswith("DROP"):
        return "ddl"
    elif sql_upper.startswith("INSERT") or sql_upper.startswith("UPDATE") or sql_upper.startswith("DELETE") or sql_upper.startswith("MERGE"):
        return "dml"
    elif "PROCEDURE" in sql_upper or "BEGIN" in sql_upper:
        return "procedure"
    else:
        return "query"

def get_databricks_endpoint():
    """
    Get the Databricks serving endpoint URL.
    """
    return os.getenv("DATABRICKS_ENDPOINT", "https://your-databricks-workspace.cloud.databricks.com/serving-endpoints/your-endpoint/invocations")

def get_databricks_token():
    """
    Get the Databricks token.
    """
    return os.getenv("DATABRICKS_TOKEN", "your-token")