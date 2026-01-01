import re
import sqlparse
from typing import Dict, List, Tuple

class SimpleValidator:
    def __init__(self):
        self.oracle_to_databricks = {
            # Data types
            r'\bNUMBER\s*\(\s*\d+\s*,\s*\d+\s*\)': 'DECIMAL',
            r'\bNUMBER\s*\(\s*\d+\s*\)': 'DECIMAL',
            r'\bNUMBER\b': 'DECIMAL',
            r'\bVARCHAR2\s*\(\s*\d+\s*\)': lambda m: f"VARCHAR",
            r'\bVARCHAR2\b': 'VARCHAR',
            r'\bCLOB\b': 'STRING',
            r'\bBLOB\b': 'BINARY',
            r'\bRAW\s*\(\s*\d+\s*\)': 'BINARY',
            r'\bLONG\b': 'STRING',
            r'\bDATE\b': 'DATE',
            
            # Functions
            r'\bSYSDATE\b': 'CURRENT_DATE',
            r'\bSYSTIMESTAMP\b': 'CURRENT_TIMESTAMP()',
            r'\bUSER\b': 'CURRENT_USER()',
            r'\bNVL\s*\(': 'COALESCE(',
            r'\bNVL2\s*\(([^,]+),([^,]+),([^)]+)\)': r'CASE WHEN \1 IS NOT NULL THEN \2 ELSE \3 END',
            r'\bDECODE\s*\(': 'CASE ',
            r'\bROWNUM\b': 'ROW_NUMBER() OVER (ORDER BY 1)',
            r'\bROWID\b': 'monotonically_increasing_id()',
            
            # Dual table
            r'\bFROM\s+DUAL\b': '',
            r'\bSELECT\s+([^;]+)\s+FROM\s+DUAL\s*;?': r'SELECT \1;',
            
            # Sequences
            r'(\w+)\.NEXTVAL': r'nextval("\1")',
            r'(\w+)\.CURRVAL': r'currval("\1")',
            
            # Operators
            r'\|\|': 'CONCAT',
            r'\bCONNECT\s+BY\b': '-- CONNECT BY not supported in Databricks',
            r'\bSTART\s+WITH\b': '-- START WITH not supported in Databricks',
        }
        
        self.databricks_reserved = {
            'ANTI', 'CROSS', 'EXCEPT', 'FULL', 'INNER', 'INTERSECT', 'JOIN', 'LEFT', 
            'NATURAL', 'ON', 'RIGHT', 'SEMI', 'UNION', 'USING', 'LATERAL', 'WINDOW'
        }
        
        self.unsupported_features = [
            (r'\bTRIGGER\b', 'Triggers not supported - implement in application logic'),
            (r'\bPACKAGE\b', 'Packages not supported - use functions'),
            (r'\bPROCEDURE\b', 'Stored procedures limited - consider UDFs'),
            (r'\bCURSOR\b', 'Cursors not supported - use DataFrames'),
            (r'\bFOR\s+UPDATE\b', 'FOR UPDATE not supported - use Delta merge'),
            (r'\bPRAGMA\b', 'Pragma directives not supported'),
            (r'\bDBMS_\w+', 'DBMS packages not available'),
        ]

    def validate_migration(self, original_sql: str, migrated_sql: str) -> Dict:
        """Production-level validation"""
        
        # Clean input
        migrated_sql = self._clean_sql(migrated_sql)
        
        # Core validations
        syntax_result = self._validate_syntax(migrated_sql)
        oracle_result = self._validate_oracle_conversion(migrated_sql)
        databricks_result = self._validate_databricks_compatibility(migrated_sql)
        performance_result = self._validate_performance(migrated_sql)
        schema_result = self._validate_schema_integrity(original_sql, migrated_sql)
        
        # Calculate weighted scores
        scores = {
            'syntax_score': syntax_result['score'],
            'oracle_score': oracle_result['score'],
            'databricks_score': databricks_result['score'],
            'performance_score': performance_result['score'],
            'schema_score': schema_result['score']
        }
        
        # Weighted average (syntax and schema are most critical)
        overall_score = (
            scores['syntax_score'] * 0.25 +
            scores['oracle_score'] * 0.20 +
            scores['databricks_score'] * 0.20 +
            scores['performance_score'] * 0.15 +
            scores['schema_score'] * 0.20
        )
        
        # Aggregate issues and recommendations
        all_issues = []
        all_recommendations = []
        
        for result in [syntax_result, oracle_result, databricks_result, performance_result, schema_result]:
            all_issues.extend(result.get('issues', []))
            all_recommendations.extend(result.get('recommendations', []))
        
        return {
            'overall_score': round(overall_score, 1),
            'scores': scores,
            'issues': all_issues,
            'recommendations': list(set(all_recommendations)),  # Remove duplicates
            'critical_issues': [issue for issue in all_issues if 'ERROR' in issue or 'CRITICAL' in issue],
            'auto_fixable': self._get_auto_fixable_issues(migrated_sql)
        }

    def _clean_sql(self, sql: str) -> str:
        """Clean and normalize SQL"""
        if not sql:
            return ""
        
        # Remove markdown code blocks
        sql = re.sub(r'```sql\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'```python\s*', '', sql, flags=re.IGNORECASE)
        sql = re.sub(r'```\s*', '', sql)
        
        # Remove comments about changes
        sql = re.sub(r'--.*?Changes and Enhancements.*', '', sql, flags=re.DOTALL)
        sql = re.sub(r'###\s+Limitations and Manual Review.*?(?=###|\Z)', '', sql, flags=re.DOTALL)
        sql = re.sub(r'###\s+Limitations and Assumptions.*?(?=###|\Z)', '', sql, flags=re.DOTALL)
        sql = re.sub(r'###\s+Automation Feasibility.*?(?=###|\Z)', '', sql, flags=re.DOTALL)
        sql = re.sub(r'/\*.*?\*/', '', sql, flags=re.DOTALL)
        
        # Normalize whitespace
        sql = re.sub(r'\s+', ' ', sql).strip()
        
        return sql

    def _validate_syntax(self, sql: str) -> Dict:
        """Validate SQL syntax"""
        issues = []
        score = 100
        
        try:
            parsed = sqlparse.parse(sql)
            if not parsed:
                return {'score': 0, 'issues': ['CRITICAL: No valid SQL statements found']}
            
            # Check each statement
            for stmt in parsed:
                stmt_str = str(stmt).strip()
                if not stmt_str:
                    continue
                
                # Check balanced parentheses
                if stmt_str.count('(') != stmt_str.count(')'):
                    issues.append('ERROR: Unbalanced parentheses')
                    score -= 30
                
                # Check for incomplete statements
                if not stmt_str.rstrip().endswith((';', ')')):
                    issues.append('WARNING: Statement may be incomplete')
                    score -= 10
                
                # Check for SQL injection patterns
                if re.search(r"'[^']*;[^']*'", stmt_str):
                    issues.append('WARNING: Potential SQL injection pattern')
                    score -= 15
                    
        except Exception as e:
            issues.append(f'CRITICAL: Syntax parse error - {str(e)}')
            score = 0
        
        return {
            'score': max(0, score),
            'issues': issues,
            'recommendations': ['Fix syntax errors before deployment'] if issues else []
        }

    def _validate_oracle_conversion(self, sql: str) -> Dict:
        """Check for Oracle-specific syntax that needs conversion"""
        issues = []
        score = 100
        
        # Check for Oracle remnants
        oracle_patterns = {
            r'\bNUMBER\b': 'Use DECIMAL instead of NUMBER',
            r'\bVARCHAR2\b': 'Use VARCHAR instead of VARCHAR2',
            r'\bSYSDATE\b': 'Use CURRENT_DATE instead of SYSDATE',
            r'\bROWNUM\b': 'Use ROW_NUMBER() window function instead of ROWNUM',
            r'\bDUAL\b': 'Remove FROM DUAL - not needed in Databricks',
            r'\bSEQUENCE\b': 'Use AUTO_INCREMENT or generated columns',
            r'\bCONNECT\s+BY\b': 'Hierarchical queries not supported - use recursive CTEs',
            r'\bSTART\s+WITH\b': 'Hierarchical queries not supported',
            r'\|\|': 'Use CONCAT() function instead of || operator',
            r'\bNVL\s*\(': 'Use COALESCE() instead of NVL()',
            r'\bDECODE\s*\(': 'Use CASE statement instead of DECODE()',
        }
        
        for pattern, message in oracle_patterns.items():
            if re.search(pattern, sql, re.IGNORECASE):
                issues.append(f'Oracle syntax: {message}')
                score -= 15
        
        # Check for unsupported features
        for pattern, message in self.unsupported_features:
            if re.search(pattern, sql, re.IGNORECASE):
                issues.append(f'CRITICAL: {message}')
                score -= 25
        
        return {
            'score': max(0, score),
            'issues': issues,
            'recommendations': ['Convert all Oracle-specific syntax'] if issues else []
        }

    def _validate_databricks_compatibility(self, sql: str) -> Dict:
        """Check Databricks-specific requirements"""
        issues = []
        recommendations = []
        score = 100
        
        # Check for DELTA format
        if 'CREATE TABLE' in sql.upper() and 'USING DELTA' not in sql.upper():
            issues.append('Missing DELTA format for tables')
            recommendations.append('Add USING DELTA to CREATE TABLE statements')
            score -= 20
        
        # Check for proper data types
        if re.search(r'\bTEXT\b', sql, re.IGNORECASE):
            issues.append('Use STRING instead of TEXT')
            score -= 10
        
        # Check for reserved words as identifiers
        words = re.findall(r'\b\w+\b', sql.upper())
        for word in words:
            if word in self.databricks_reserved:
                issues.append(f'Reserved word used as identifier: {word}')
                score -= 5
        
        # Check for proper constraint syntax
        if re.search(r'CONSTRAINT\s+\w+\s+PRIMARY\s+KEY', sql, re.IGNORECASE):
            if 'USING DELTA' not in sql.upper():
                issues.append('PRIMARY KEY constraints require DELTA format')
                score -= 15
        
        return {
            'score': max(0, score),
            'issues': issues,
            'recommendations': recommendations
        }

    def _validate_performance(self, sql: str) -> Dict:
        """Check for performance issues"""
        issues = []
        recommendations = []
        score = 100
        
        # Check for SELECT *
        if re.search(r'SELECT\s+\*', sql, re.IGNORECASE):
            issues.append('SELECT * can impact performance')
            recommendations.append('Use explicit column lists')
            score -= 15
        
        # Check for missing WHERE clauses in SELECT
        select_statements = re.findall(r'SELECT\s+.*?(?:FROM\s+\w+)(?:\s+WHERE\s+.*?)?(?:;|$)', sql, re.IGNORECASE | re.DOTALL)
        for stmt in select_statements:
            if 'WHERE' not in stmt.upper() and 'LIMIT' not in stmt.upper():
                issues.append('SELECT without WHERE clause may scan entire table')
                recommendations.append('Add WHERE clauses or LIMIT for large tables')
                score -= 10
        
        # Check for proper indexing hints
        if re.search(r'CREATE\s+TABLE', sql, re.IGNORECASE):
            if not re.search(r'PARTITIONED\s+BY', sql, re.IGNORECASE):
                recommendations.append('Consider partitioning large tables')
        
        # Check for inefficient JOINs
        if re.search(r'CROSS\s+JOIN', sql, re.IGNORECASE):
            issues.append('CROSS JOIN can be expensive')
            recommendations.append('Use proper JOIN conditions')
            score -= 20
        
        return {
            'score': max(0, score),
            'issues': issues,
            'recommendations': recommendations
        }

    def _validate_schema_integrity(self, original_sql: str, migrated_sql: str) -> Dict:
        """Compare schema integrity between original and migrated"""
        issues = []
        score = 100
        
        try:
            # Extract table names
            orig_tables = set(re.findall(r'CREATE\s+TABLE\s+(\w+)', original_sql, re.IGNORECASE))
            mig_tables = set(re.findall(r'CREATE\s+TABLE\s+(\w+)', migrated_sql, re.IGNORECASE))
            
            # Check for missing tables
            missing_tables = orig_tables - mig_tables
            if missing_tables:
                issues.append(f'Missing tables: {", ".join(missing_tables)}')
                score -= 30
            
            # Check column counts for each table
            for table in orig_tables.intersection(mig_tables):
                orig_cols = self._extract_columns(original_sql, table)
                mig_cols = self._extract_columns(migrated_sql, table)
                
                if len(orig_cols) != len(mig_cols):
                    issues.append(f'Column count mismatch in table {table}')
                    score -= 15
                
                # Check for missing columns
                missing_cols = set(orig_cols) - set(mig_cols)
                if missing_cols:
                    issues.append(f'Missing columns in {table}: {", ".join(missing_cols)}')
                    score -= 20
                    
        except Exception as e:
            issues.append(f'Schema validation error: {str(e)}')
            score -= 10
        
        return {
            'score': max(0, score),
            'issues': issues,
            'recommendations': ['Review schema mapping'] if issues else []
        }

    def _extract_columns(self, sql: str, table_name: str) -> List[str]:
        """Extract column names from CREATE TABLE statement"""
        pattern = rf'CREATE\s+TABLE\s+{table_name}\s*\((.*?)\)'
        match = re.search(pattern, sql, re.IGNORECASE | re.DOTALL)
        
        if not match:
            return []
        
        columns_def = match.group(1)
        columns = []
        
        # Simple column extraction (handles basic cases)
        for line in columns_def.split(','):
            col_match = re.match(r'\s*(\w+)', line.strip())
            if col_match:
                columns.append(col_match.group(1))
        
        return columns

    def _get_auto_fixable_issues(self, sql: str) -> List[str]:
        """Identify issues that can be auto-fixed"""
        fixable = []
        
        for pattern in self.oracle_to_databricks.keys():
            if re.search(pattern, sql, re.IGNORECASE):
                fixable.append(f'Oracle syntax: {pattern}')
        
        if 'CREATE TABLE' in sql.upper() and 'USING DELTA' not in sql.upper():
            fixable.append('Missing DELTA format')
        
        return fixable

    def auto_fix(self, sql: str) -> str:
        """Apply comprehensive auto-fixes"""
        if not sql:
            return sql
        
        fixed_sql = self._clean_sql(sql)
        
        # Apply Oracle to Databricks conversions
        for pattern, replacement in self.oracle_to_databricks.items():
            if callable(replacement):
                fixed_sql = re.sub(pattern, replacement, fixed_sql, flags=re.IGNORECASE)
            else:
                fixed_sql = re.sub(pattern, replacement, fixed_sql, flags=re.IGNORECASE)
        
        # Add USING DELTA to CREATE TABLE statements
        if 'CREATE TABLE' in fixed_sql.upper() and 'USING DELTA' not in fixed_sql.upper():
            # Find CREATE TABLE statements and add USING DELTA
            pattern = r'(CREATE\s+TABLE\s+[^;]+?)(\s*;|\s*$)'
            def add_delta(match):
                table_def = match.group(1).rstrip()
                terminator = match.group(2) if match.group(2) else ';'
                if not table_def.rstrip().endswith(')'):
                    table_def += ')'
                return f'{table_def} USING DELTA{terminator}'
            
            fixed_sql = re.sub(pattern, add_delta, fixed_sql, flags=re.IGNORECASE | re.DOTALL)
        
        # Clean up extra whitespace
        fixed_sql = re.sub(r'\s+', ' ', fixed_sql).strip()
        
        return fixed_sql

    def get_validation_summary(self, results: Dict) -> str:
        """Generate a concise validation summary"""
        score = results['overall_score']
        
        if score >= 90:
            status = "OK EXCELLENT"
        elif score >= 80:
            status = "OK GOOD"
        elif score >= 70:
            status = "WARN FAIR"
        elif score >= 60:
            status = "WARN NEEDS WORK"
        else:
            status = "FAIL POOR"
        
        critical_count = len(results.get('critical_issues', []))
        total_issues = len(results.get('issues', []))
        
        summary = f"{status} - Score: {score}/100"
        if critical_count > 0:
            summary += f" | {critical_count} Critical Issues"
        if total_issues > 0:
            summary += f" | {total_issues} Total Issues"
        
        return summary








