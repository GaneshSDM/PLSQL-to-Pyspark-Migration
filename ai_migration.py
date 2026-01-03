# ai_migiration.py

import requests
import time
import logging
import ast
import streamlit as st
from src.utils.prompt_helper import get_conversion_prompts, get_common_prompt, build_prompt
from src.utils.common_helper import get_model_params, classify_sql

# Configure Enterprise Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class MigrationAI:
    def __init__(self, api_key):
        self.api_key = api_key

    # Universal call to Databricks endpoint using session state settings
    def call_llama(self, prompt):
        # Get settings from session state
        settings = st.session_state.model_settings
        api_url = settings["endpoint"]
        temperature = settings["temperature"]
        max_tokens = settings["max_tokens"]
        
        headers = {
            "Authorization": f"Bearer {self.api_key}",
            "Content-Type": "application/json"
        }

        payload = {
            "messages": [{"role": "user", "content": prompt}],
            "max_tokens": max_tokens,
            "temperature": temperature
        }
        retries = 3
        for attempt in range(retries):
            try:
                logger.info(f"Sending request to {settings['model']} (Attempt {attempt + 1}/{retries})...")
                response = requests.post(api_url, json=payload, headers=headers)
                response.raise_for_status()
                data = response.json()
                raw_content = data["choices"][0]["message"]["content"]
                
                # Handle new format where content is a list
                if isinstance(raw_content, list):
                    text_content = ""
                    for item in raw_content:
                        if isinstance(item, dict) and item.get("type") == "text":
                            text_content += item.get("text", "")
                    return self._clean_model_output(text_content)
                else:
                    return self._clean_model_output(raw_content)
            except Exception as e:
                logger.error(f"Attempt {attempt + 1} failed: {str(e)}")
                if attempt == retries - 1:
                    return f"Error after {retries} attempts: {str(e)}"
                time.sleep(2 ** attempt)  # Exponential backoff

    def _clean_model_output(self, content):
        """Parses and cleans the model output if it contains structured reasoning traces."""
        if not content:
            return ""
            
        # Check for the specific structured format (lines of python-dict strings)
        if "{'type': 'reasoning'" in content or "{'type': 'text'" in content:
            full_text = ""
            found_structured = False
            for line in content.splitlines():
                line = line.strip()
                if line.startswith("{") and line.endswith("}"):
                    try:
                        data = ast.literal_eval(line)
                        if data.get('type') == 'text':
                            full_text += data.get('text', '')
                        found_structured = True
                    except:
                        pass
            
            if found_structured:
                return full_text
                
        return content

    # ------------------------------
    # SQL Migration Functions
    # ------------------------------
    def migrate_schema(self, oracle_sql, **kwargs):
        # Handle large files by chunking
        if len(oracle_sql) > 8000:  # Characters threshold for chunking
            return self._process_large_sql(oracle_sql, **kwargs)
            
        src_dialect = "oracle"
        conversion_prompts = get_conversion_prompts(src_dialect)
        common_prompt = get_common_prompt("pyspark", "python", "schema")
        additional_prompts = ""  # Can add if needed
        prompt = build_prompt(common_prompt, conversion_prompts, additional_prompts, src_dialect, oracle_sql)
        
        return self.call_llama(prompt)

        return self.call_llama(prompt)

    def _process_large_sql(self, oracle_sql, **kwargs):
        """Process large SQL files by splitting into manageable chunks"""
        import re
        
        # Split by CREATE statements
        statements = re.split(r'(i)\b(CREATE\s+(:TABLE|VIEW|PROCEDURE|FUNCTION))', oracle_sql)
        chunks = []
        current_chunk = ""
        
        for i in range(0, len(statements), 2):
            if i + 1 < len(statements):
                statement = statements[i] + statements[i + 1]
                if len(current_chunk + statement) > 6000:
                    if current_chunk:
                        chunks.append(current_chunk)
                    current_chunk = statement
                else:
                    current_chunk += statement
        
        if current_chunk:
            chunks.append(current_chunk)
        
        # Process each chunk
        results = []
        for i, chunk in enumerate(chunks):
            logger.info(f"Processing chunk {i+1}/{len(chunks)}")
            result = self.migrate_schema(chunk, **kwargs)
            results.append(result)
        
        # Combine results
        combined_sql = ""
        combined_changes = "### Changes and Enhancements\n"
        combined_limitations = "### Limitations and Manual Review\n"
        
        for result in results:
            if "```sql" in result:
                sql_match = re.search(r"```sql(.*)```", result, re.DOTALL)
                if sql_match:
                    combined_sql += sql_match.group(1).strip() + "\n\n"
            
            changes_match = re.search(r"### Changes and Enhancements(.*)", result, re.DOTALL)
            if changes_match:
                combined_changes += changes_match.group(1).strip() + "\n"

            limitations_match = re.search(r"### Limitations and Manual Review(.*)", result, re.DOTALL)
            if limitations_match:
                combined_limitations += limitations_match.group(1).strip() + "\n"
        
        return f"```sql\n{combined_sql}```\n\n{combined_changes}\n{combined_limitations}"

    def migrate_procedure(self, oracle_procedure, **kwargs):
        src_dialect = "oracle"
        conversion_prompts = get_conversion_prompts(src_dialect)
        common_prompt = get_common_prompt("pyspark", "python", "procedure")
        additional_prompts = ""  # Can add if needed
        prompt = build_prompt(common_prompt, conversion_prompts, additional_prompts, src_dialect, oracle_procedure)
        
        return self.call_llama(prompt, **kwargs)

    def optimize_query(self, sql_query, **kwargs):
        prompt = f"""
Optimize the following SQL query for Databricks Spark SQL performance.

### Optimization Strategies:
1. **Explicit Joins**: Convert comma-separated joins (ANSI-89) to explicit `JOIN ... ON` (ANSI-92).
2. **Selectivity**: Avoid `SELECT *`; list specific columns.
3. **Filtering**: Push `WHERE` clauses as close to the source tables as possible.
4. **CTEs**: Use Common Table Expressions (WITH clauses) for readability and potential reuse.
5. **Window Functions**: Use window functions instead of self-joins where applicable.

### Example:

**Input:**
SELECT * FROM orders o, customers c 
WHERE o.cust_id = c.id AND c.region = 'US';

**Optimized:**
SELECT 
  o.order_id,
  o.order_date,
  c.name AS customer_name
FROM orders o
JOIN customers c ON o.cust_id = c.id
WHERE c.region = 'US';

---

**Output Format:**
1. Provide the **Optimized SQL** code in a single markdown block (```sql ... ```).
2. After the code block, provide a section titled "### Changes and Enhancements" where you list the specific changes made, optimizations applied, and any reasoning.
3. Add a section titled "### Limitations and Assumptions".

**Input:**
{sql_query}

**Optimized Query:**
"""
        return self.call_llama(prompt, **kwargs)

    def generate_plsql_to_pyspark_feasibility_doc(self, oracle_context, **kwargs):
        prompt = f"""
Create a concise, enterprise-ready markdown document that explains PL/SQL to PySpark feasibility and limitations, plus an accelerator strategy for Oracle to Databricks migration. Use the exact section structure and formatting rules below.

Formatting rules:
- Use Markdown headings (##, ###) and tables.
- Keep language professional and concise.
- Emphasize modernization over direct translation.
- Use ASCII characters only.

Required structure:

## PL/SQL to PySpark Feasibility and Limitations

### 1. Background and Context
- Explain why PL/SQL logic does not translate line-by-line to PySpark.
- State the objective: feasibility, what can/partially can/cannot be converted, automation expectations.

### 2. Key Architectural Differences
#### 2.1 Execution Model Comparison
Provide a table with: Aspect, PL/SQL, PySpark.
Include: Programming Style, Execution, Processing, State, Runtime, Optimization, Transactions.
End with a short "Key Insight" paragraph.

### 3. What Can Be Converted
#### 3.1 SQL and DML Logic
Provide a table: Oracle Construct, Feasibility, Databricks Output.
Include: SELECT, INSERT INTO...SELECT, UPDATE, DELETE, MERGE, JOIN, GROUP BY/Aggregations, CASE WHEN, non-correlated subqueries.

#### 3.2 DDL (Schema and Table Definitions)
Provide a table: Oracle, Databricks/Delta.
Include: CREATE TABLE, PARTITION BY, INDEX, CONSTRAINTS, SEQUENCE.
Add one sentence about DDL accuracy.

#### 3.3 Simple Functions
Explain when functions are convertible and to what (Python helper functions or Spark SQL UDFs).

### 4. What Can Be Partially Converted (Manual Review Required)
Include cursor-based logic, complex conditional logic, exception handling.
Explain why manual redesign is needed.

### 5. What Cannot Be Converted
Include stateful/sequential logic, transaction control, triggers, Oracle-specific packages.
Provide a table of Oracle packages and reasons (DBMS_OUTPUT, UTL_FILE, DBMS_SCHEDULER, DBMS_METADATA, database links).

### 6. Data Type Mapping
Provide a table mapping NUMBER, VARCHAR2, DATE, TIMESTAMP, CLOB, BLOB, VARRAY/Nested Table.
Add a note about precision/behavior validation.

### 7. Summary Conversion Feasibility
Provide a table: Area, Feasibility.
Include SQL and DML, DDL and Schema, ETL Transformations, Cursor-Based Logic, Triggers and Transactions, Oracle-Specific Packages.

### 8. Key Takeaways
Provide 2-4 bullets.

## Oracle to Databricks Migration Accelerators
### 1. Background and Context
Explain the need for accelerators and the shift to PySpark, Spark SQL, Delta Lake, streaming/batch pipelines.

### 2. Realistic Effort Reduction Expectations
#### 2.1 Typical Effort Distribution (Without Accelerators)
Provide a table: Activity, Typical Effort %.
Include code understanding, manual rewrite, data patterns (MERGE/CDC/SCD/cursors), testing/validation, performance tuning.

#### 2.2 Effort Reduction by Accelerator Category
Provide a table: Accelerator Type, Expected Effort Reduction.
Include inventory/dependency analysis, pattern-based conversion, standard CDC/SCD/MERGE, trigger templates, testing/reconciliation, overall program reduction, mature accelerator + skilled team.
Add a short note that >70% automation is unrealistic for complex workloads.

### 3. Correct Mental Model for Accelerators
Explain pattern identification -> standardized replacement -> guardrails.
State that 90% of logic tends to be in 20-30 patterns.

### 4. Recommended Accelerator Stack
Provide subsections:
4.1 Assessment and Inventory Accelerator
4.2 PL/SQL Pattern Classification Accelerator
4.3 Pattern-to-Framework Conversion Accelerators (Cursor/Loop, MERGE/SCD, Trigger/CDC)
4.4 Materialized View Conversion Accelerator
4.5 Code Scaffolding and Generation Accelerator
4.6 Testing and Reconciliation Accelerator

### 5. How to Build Accelerators - Step-by-Step
List 4 steps: mine code, define canonical patterns, build incrementally, measure ROI.

### 6. What Success Looks Like
Provide 3-5 bullets.

### 7. Conclusion
Provide a short summary sentence.

Use this optional context for tailoring:
{oracle_context}
"""
        return self.call_llama(prompt, **kwargs)

    def migrate_bulk(self, items, task_type="schema", **kwargs):
        """Process multiple objects in one request. items is a list of dicts with keys: name, content."""
        if not items:
            return "No items provided."

        task_type = (task_type or "schema").strip().lower()
        task_instructions = {
            "schema": "Convert Oracle DDL to Databricks SQL DDL.",
            "procedure": "Convert PL/SQL procedures/functions/packages to Databricks SQL or PySpark as required.",
            "optimize": "Optimize SQL for Databricks Spark SQL performance.",
            "feasibility": "Generate a feasibility and limitations document for each item."
        }
        instruction = task_instructions.get(task_type, task_instructions["schema"])

        bulk_items_text = []
        for item in items:
            name = item.get("name", "Unnamed Item")
            content = item.get("content", "")
            bulk_items_text.append(f"[ITEM]\nName: {name}\nContent:\n{content}\n")

        prompt = f"""
You are an expert Oracle to Databricks migration specialist.
Task: {instruction}

For each item, output in this structure:
## <Name>
1. Converted Code in a single code block.
   - Use ```sql``` for SQL outputs.
   - Use ```python``` if PySpark is required.
2. ### Changes and Enhancements
3. ### Limitations and Manual Review
4. ### Automation Feasibility (HIGH/MEDIUM/LOW)

Process all items independently and do not merge them.

Items:
{''.join(bulk_items_text)}
"""
        return self.call_llama(prompt, **kwargs)

    def generate_streamlit_code(self, requirements, **kwargs):
        prompt = f"""
Generate Streamlit code based on these requirements:

{requirements}

Include:
- Secure DB connection
- Query execution UI
- Error handling
- Dataframe display

Streamlit Code:
"""
        return self.call_llama(prompt, **kwargs)

    def get_enterprise_capabilities(self):
        """Returns a summary of enterprise features for documentation."""
        return {
            "Model": "databricks-gpt-oss-120b",
            "Features": [
                "Few-Shot Prompting for High Accuracy",
                "Exponential Backoff Retry Logic",
                "Structured Enterprise Logging",
                "Unity Catalog Compliance",
                "Delta Lake Optimization Rules"
            ],
            "Supported Conversions": [
                "Oracle DDL -> Databricks Delta DDL",
                "PL/SQL -> Databricks SQL Procedures",
                "Legacy SQL -> Spark Optimized SQL",
                "PL/SQL Feasibility -> PySpark/Databricks Guidance"
            ]
        }
