# Legacy System ➜ Databricks Converter

A Databricks-native web application for accelerating SQL migration and schema reconciliation from legacy systems (Snowflake, T-SQL, Redshift, Oracle, Teradata, MySQL, PostgreSQL, SSIS, Informatica, etc.) into Databricks SQL.

The app leverages LLMs (Claude, Llama, GPT, etc.) for query conversion, validation, and automated fixes.

## ✨ Features

### 🔹 1. Interactive Conversion
Convert individual queries in real time.
- Choose LLM model, SQL Warehouse, and Source Dialect
- Add custom prompt instructions to handle tricky translations
- Validate queries by running EXPLAIN in Databricks
- Retry mechanism: if validation fails, re-submit failed queries with error context so the LLM can correct its own mistakes

### 🔹 2. Batch Jobs
Bulk convert entire folders of SQL files.
- Configure source dialect, input folder, output notebook folder, and results table
- Choose validation strategy:
  - No validation
  - Validate by running EXPLAIN
  - Failed queries can be retried with error feedback
- Results are persisted in Delta for easy querying and history

### 🔹 3. Reconcile Tables
Compare source vs target schemas (catalog.schema format).
- Run reconciliation jobs that:
  - Count rows in source and target
  - Highlight mismatches
- Results stored in a Delta table for auditing
- Useful for validating post-migration data consistency

## 🚀 Quick Start

### 1. Clone into Databricks Git Folder
Clone this repository into a Databricks Git-enabled workspace folder.

### 2. Deploy the app
In the repo root there is a notebook called `app_deployer.py`.
Open it in Databricks and run all cells. This will automatically:
- Install dependencies
- Deploy the Streamlit app into your workspace
- Make the app available for immediate use

### 3. Start Using the App
```bash
streamlit run app.py
```

## 🔧 Configuration

### LLM Models
Select from Databricks GPT-5.2, GPT-5.1, GPT OSS 120B, Qwen3 80B, Llama models, Gemma, Claude, GPT, etc.

### Custom Prompts
(Optional) Add dialect-specific hints for complex conversions.

### Validation
Toggle validation strategy to balance speed vs correctness.

### Results Storage
Batch and reconcile results are persisted in Delta tables.

## 🛠️ Retry Logic

Both Interactive and Batch modes have built-in retry support:

1. If validation fails, the app automatically captures the validation error message
2. The failed query plus the error context are re-submitted to the LLM
3. The LLM adjusts its output and attempts to generate a corrected query
4. This iterative approach significantly increases the chance of success

## 📂 Project Structure

```
├── app.py                          # Main Streamlit app (3 tabs: Interactive, Batch, Reconcile)
├── app_deployer.py                 # Notebook for auto-deployment in Databricks
├── requirements.txt                # Python dependencies
├── ai_migration.py                 # Core migration logic with LLM integration
├── simple_validator.py             # SQL validation utilities
├── src/
│   ├── resources/                  # YAML files with LLM prompts
│   │   ├── conversion_prompts.yaml # Dialect-specific conversion rules
│   │   └── common_prompts.yaml     # Common prompt templates
│   ├── utils/                      # Helper modules
│   │   ├── prompt_helper.py        # Dynamic prompt building
│   │   └── common_helper.py        # Shared utilities
│   └── notebooks/                  # Job notebooks for batch processing
│       ├── batch_converter.py      # Batch SQL file conversion
│       └── schema_reconciler.py    # Schema reconciliation
└── README.md
```

## 📊 Example Migration Flow

1. **Test with Interactive Conversion**: Use Interactive Conversion to test a few sample queries from your legacy system
2. **Scale with Batch Jobs**: Run a Batch Job to convert hundreds of SQL files into Databricks notebooks
3. **Validate with Reconciliation**: Execute the Reconcile Tables job to ensure source and target schemas match in counts and data samples

## 🤖 Supported Systems

### Source Dialects
- Snowflake
- T-SQL (SQL Server)
- Redshift
- Oracle
- Teradata
- MySQL
- PostgreSQL
- SSIS
- Informatica
- Other (generic SQL)

### Target
- Databricks SQL
- PySpark DataFrames
- Delta Lake

### LLM Models
- Databricks GPT-5.2
- Databricks GPT-5.1
- Databricks GPT OSS 120B (default)
- Databricks GPT OSS 20B
- Databricks Qwen3 80B
- Databricks Llama 4 Maverick
- Databricks Gemma 3 12B
- Databricks Llama 3.1 8B
- Databricks Llama 3.3 70B
- Claude (Anthropic)
- GPT (OpenAI)
- Llama (Meta)
- Gemma (Google)
- Custom endpoints

## 🔒 Security & Configuration

### Environment Variables
Set these in your Databricks secrets or environment:

```bash
DATABRICKS_TOKEN=your-databricks-personal-access-token
# or
DATABRICKS_API_KEY=your-databricks-personal-access-token
```

### API Key Configuration
You can also configure your API key directly in the app:
1. Open the **Settings** tab (⚙️)
2. Enter your Databricks Personal Access Token
3. The token will be saved for the session

To get your Databricks Personal Access Token:
1. Go to your Databricks workspace
2. Click on your username (top right)
3. Select "User Settings"
4. Go to "Developer" → "Access tokens"
5. Generate a new token

## 📈 Performance & Best Practices

### Batch Processing
- Use Delta tables for results storage
- Monitor job runs through the Databricks Jobs UI
- Scale cluster size based on workload

### Validation Strategy
- Start with "No validation" for speed during initial migration
- Enable EXPLAIN validation for production-ready code
- Use retry mechanism to improve success rates

### Schema Reconciliation
- Run reconciliation after major migrations
- Use row count comparison for quick validation
- Enable data sampling for critical tables only (performance intensive)

## 🐛 Troubleshooting

### Common Issues

1. **Import Errors**: Ensure all dependencies are installed via `pip install -r requirements.txt`

2. **LLM Connection Issues**: Verify endpoint URLs and API keys in Databricks secrets

3. **Validation Failures**: Check Databricks SQL syntax compatibility and cluster configuration

4. **Batch Job Failures**: Ensure input/output paths exist and are accessible

### Logs & Debugging
- Check Databricks driver logs for detailed error messages
- Use the interactive mode first to test individual queries
- Monitor Delta tables for job results and error details

## 🤝 Contributing

1. Fork the repository
2. Create a feature branch
3. Add tests for new functionality
4. Submit a pull request

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## 🆘 Support

For issues and questions:
1. Check the troubleshooting section above
2. Review Databricks documentation
3. Open an issue in this repository
4. Contact your Databricks representative

---

**Built with ❤️ for the Databricks community** 
