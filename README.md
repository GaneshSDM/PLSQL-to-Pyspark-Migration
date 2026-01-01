
# Databricks DDL Migration - Oracle SQL/PLSQL to Databricks SQL and PySpark

A toolset to accelerate migration of Oracle DDL and PL/SQL into Databricks SQL and PySpark workflows. This repository combines automated parsing, translation heuristics, validation, and an interactive web interface to help engineers convert Oracle DDL (CREATE TABLE, ALTER TABLE, indexes, partitions, constraints, etc.) into Databricks-friendly definitions and PySpark transformations where required.

Key components:
- ai_migration.py - AI-assisted migration engine that translates Oracle DDL/PLSQL to Databricks SQL or PySpark with explicit limitations and feasibility notes.
- simple_validator.py - Lightweight validator that checks translated SQL and flags common issues (type mismatches, missing constraints, unsupported constructs).
- app.py and app.yaml - Minimal web interface / app wrapper for interactive migration, previewing results, bulk processing, and running validations.
- requirements.txt - Python dependencies used by the project.

## Features

- AI-assisted translation
  - Uses pattern-based parsing plus AI assistance to map Oracle types, constraints, and DDL constructs to Databricks SQL, falling back to PySpark when necessary.
  - Produces Databricks SQL (DDL/DML) with clear limitations and manual review notes.

- DDL parsing and mapping
  - Parses Oracle CREATE TABLE/ALTER TABLE statements, column definitions, constraints (PK/FK/UNIQUE), and common table options.
  - Maps Oracle data types to Databricks SQL types (NUMBER - DECIMAL, VARCHAR2 - VARCHAR, DATE/TIMESTAMP retained), with fallbacks for unsupported types.

- Constraint & index handling
  - Preserves primary keys and unique constraints as metadata / validation checks.
  - Notes index information and emits migration guidance (e.g., Delta table optimizations, Z-ordering recommendations) rather than trying to recreate Oracle-specific indexes verbatim.

- Partitioning & clustering guidance
  - Detects partitioning columns or partition-related clauses and suggests Databricks/Delta Lake partitioning strategies.

- Validation & reporting
  - Runs translation-time checks with simple_validator.py to identify:
    - Type mapping issues and possible precision loss
    - Missing or unsupported features
    - Inconsistencies between DDL and target schema
  - Generates actionable messages and suggestions to fix or manually verify translations.

- Interactive web UI
  - Lightweight app (app.py) to paste or upload DDL/PLSQL, preview Databricks SQL/PySpark output, run validations, and download migration artifacts.
  - Bulk upload mode for converting multiple files in one run.
  - Deployment configuration (app.yaml) included for quick hosting (example: Google App Engine / other platforms).

- Extensible & scriptable
  - Designed to be used as a CLI script, in automated migration pipelines, or interactively from the web UI.
  - Easy to extend mapping rules, add project-specific type rules, or integrate custom validation logic.

## Why use this project

- Save time translating complex Oracle DDL/PLSQL into Databricks SQL and PySpark-compatible structures.
- Reduce errors from manual conversions (data type mismatches, lost constraints).
- Provide repeatable, auditable migration artifacts for large-scale migrations.
- Combine deterministic rules with AI assistance to handle edge cases and ambiguous constructs.

## Getting started

Prerequisites
- Python 3.8+ (or your preferred supported version)
- A Databricks workspace or PySpark environment for testing produced code (recommended)
- (Optional) API keys/config for AI provider if ai_migration.py uses an external model - check the script header or configuration for details.

Install dependencies
```bash
pip install -r requirements.txt
```

Quick runs (examples)
- CLI-style usage (example pattern):
```bash
python ai_migration.py --input oracle_ddl.sql --output databricks_migration.sql
```
- Run the web app locally:
```bash
python app.py
# then open http://localhost:XXXX as printed by the app
```
- Validate a translated file:
```bash
python simple_validator.py --input databricks_migration.sql
```
Note: The exact CLI flags and options are documented in the header/docstrings of each script (ai_migration.py, simple_validator.py, app.py). Use `-h` or open the scripts to see the supported arguments.

## Usage patterns & examples

- Single-table migration
  - Paste a small CREATE TABLE DDL into the web UI or run ai_migration.py with a single-file input to generate Databricks SQL or PySpark output.

- Batch migration
  - Provide a directory or list of DDL/PLSQL files. The toolset can be scripted to process many files and produce per-object migration artifacts and a combined report from simple_validator.py.

- Integration into Databricks
  - Output artifacts are ready to be used in Databricks notebooks (DDL/DML, UDFs, and PySpark scaffolds).
  - Consider wrapping the resulting code into a notebook cell or job that writes to Delta tables.

## Files & structure

- ai_migration.py - Core migration logic. Responsible for parsing input DDL/PLSQL, applying mapping rules, invoking AI assistance (if configured), and rendering Databricks SQL or PySpark.
- simple_validator.py - Runs checks on translated SQL output and emits a report with warnings/errors/suggestions.
- app.py - A minimal interactive application to test/present migrations and validations, including bulk uploads.
- app.yaml - Example deployment config for hosting the web app (platform-specific).
- requirements.txt - Python packages used by the project.

(See docstrings and inline comments inside each script for more precise usage and configuration options.)

## Extending & customizing

- Add or adjust type mappings: Modify mapping tables inside ai_migration.py to change how Oracle types map to Databricks SQL or PySpark types for your organization.
- Add project-specific validations: Extend simple_validator.py to enforce company rules (naming conventions, required auditing columns, etc.).
- Swap or configure AI provider: If the AI-assisted flow uses an external model, add configuration to inject your own API key, model, or provider.

## Best practices & migration tips

- Validate data after schema migration by running sample reads and comparing record counts, key distributions, and data type checks.
- Preserve critical constraints as metadata checks even if the target system does not enforce them the same way as Oracle.
- Use Delta Lake features (time travel, ACID writes, partitioning) to implement robust data pipelines after migrating schemas.
- Test incremental loads and backfills with the translated schema before switching production pipelines.

## Contributing

Contributions are welcome. Suggested workflow:
1. Fork the repo.
2. Create a feature branch.
3. Add tests or examples for the new mapping/validation.
4. Open a pull request describing your changes.

Please follow idiomatic Python style and keep changes focused (type mapping, validator improvements, UI enhancements).

## License

Add your preferred license file (e.g., MIT, Apache 2.0) to the repository. If no license exists in the repo yet, add LICENSE with the chosen license text.

## Contact / Support

For questions, examples, or to request features, open an issue in this repository.

---

If you want, I can:
- Commit this README.md to the main branch,
- Open a PR with the proposed README,
- Or update the README with additional examples by inspecting ai_migration.py, simple_validator.py, and app.py to extract exact CLI flags and usage snippets. Which would you prefer? 