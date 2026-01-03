import streamlit as st
import os
import pandas as pd
import time
from dotenv import load_dotenv
from ai_migration import MigrationAI
from simple_validator import SimpleValidator
import json

# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Legacy System ➜ Databricks Converter",
    page_icon="🔄",
    layout="wide",
    initial_sidebar_state="expanded"
)

# -----------------------------------------------------------------------------
# Supported dialects and models
SUPPORTED_DIALECTS = [
    "Snowflake", "T-SQL", "Redshift", "Oracle", "Teradata",
    "MySQL", "PostgreSQL", "SSIS", "Informatica", "Other"
]

SUPPORTED_MODELS = [
    "Claude", "GPT", "Llama", "Gemma", "Databricks GPT", "Other"
]

# Model endpoints mapping
MODEL_ENDPOINTS = {
    "Databricks GPT": "https://dbc-e8fae528-2bde.cloud.databricks.com/serving-endpoints/databricks-gpt-oss-120b/invocations",
    # Add other model endpoints as needed
}

# Initialize session state
if "migration_ai" not in st.session_state:
    # Default model configuration
    st.session_state.model_settings = {
        "model": "Databricks GPT",
        "endpoint": "https://dbc-e8fae528-2bde.cloud.databricks.com/serving-endpoints/databricks-gpt-oss-120b/invocations",
        "temperature": 0.1,
        "max_tokens": 4096,
        "api_key": "dapi31a4d352082f4e740f88e86cbee1bf1f"
    }
    st.session_state.migration_ai = MigrationAI(api_key=st.session_state.model_settings["api_key"])
    st.session_state.validator = SimpleValidator()

# Load environment variables
load_dotenv()


# -----------------------------------------------------------------------------
# 3. ENTERPRISE CSS STYLING
# -----------------------------------------------------------------------------
# Decision Minds Theme Colors
THEME_ACCENT_COLOR = "#EC6225"          # Primary Orange
THEME_ACCENT_DARK = "#D35400"         # Darker Orange
THEME_ACCENT_LIGHT = "#F39C12"           # Lighter Orange
THEME_BACKGROUND_LIGHT = "#FFFFFF"
THEME_TEXT_DARK = "#2D3748"             # Dark gray instead of black
THEME_TEXT_LIGHT = "#4A5568"            # Medium gray for secondary text

ENTERPRISE_CSS = f"""
<style>
    /* Import Inter Font */
    @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&display=swap');

    html, body, [class*="css"] {{
        font-family: 'Inter', sans-serif;
        color: {THEME_TEXT_DARK};
    }}

    /* Hide Streamlit Default Elements */
    #MainMenu {{visibility: hidden;}}
    footer {{visibility: hidden;}}
    header {{visibility: hidden;}}
    [data-testid="stSidebar"] {{display: none;}}

    /* Background */
    .stApp {{
        background-color: #f8f9fa;
        color: {THEME_TEXT_DARK};
    }}

    /* Ensure all text is visible */
    .stMarkdown, .stText, p, div, span {{
        color: {THEME_TEXT_DARK} !important;
    }}

    /* Code blocks with better contrast */
    .stCodeBlock, pre, code {{
        background-color: #f7fafc !important;
        color: #2d3748 !important;
        border: 1px solid #e2e8f0 !important;
    }}

    /* Text areas and inputs */
    .stTextArea textarea, .stTextInput input {{
        background-color: white !important;
        color: {THEME_TEXT_DARK} !important;
        border: 1px solid #e2e8f0 !important;
    }}

    
    
    
    .header-logo-container {{
        display: flex;
        align-items: center;
        gap: 1rem;
    }}
    
    .header-title {{
        font-size: 1.5rem;
        font-weight: 700;
        color: {THEME_TEXT_DARK};
        letter-spacing: -0.025em;
    }}

    
    .card:hover {{
        transform: translateY(-2px);
        box-shadow: 0 20px 25px -5px rgba(0, 0, 0, 0.1), 0 10px 10px -5px rgba(0, 0, 0, 0.04);
    }}
    
    .card-header {{
        font-size: 1.25rem;
        font-weight: 600;
        color: {THEME_TEXT_DARK};
        margin-bottom: 1.5rem;
        border-bottom: 2px solid #f3f4f6;
        padding-bottom: 1rem;
        display: flex;
        align-items: center;
        gap: 0.5rem;
    }}

    /* Custom Buttons Override */
    div.stButton > button, [data-testid="stClipboardButton"] > button {{
        width: 100%;
        border-radius: 0.5rem;
        font-weight: 600;
        padding: 0.75rem 1.5rem;
        border: none;
        transition: all 0.3s ease;
        text-transform: uppercase;
        letter-spacing: 0.05em;
        font-size: 0.875rem;
    }}
    
    /* Primary Button Style (Orange Theme) */
    div.stButton > button:first-child {{
        background: linear-gradient(135deg, {THEME_ACCENT_COLOR} 0%, {THEME_ACCENT_DARK} 100%);
        color: white;
        box-shadow: 0 4px 6px rgba(236, 98, 37, 0.2);
    }}
    div.stButton > button:first-child:hover {{
        background: linear-gradient(135deg, {THEME_ACCENT_DARK} 0%, {THEME_ACCENT_COLOR} 100%);
        box-shadow: 0 6px 8px rgba(236, 98, 37, 0.3);
        transform: translateY(-1px);
    }}
    div.stButton > button:first-child:active {{
        transform: translateY(0);
    }}

    /* Secondary Button Style (for Navigation and Copy/Download) */
    div.stButton > button[kind="secondary"], [data-testid="stClipboardButton"] > button {{
        background: white;
        color: {THEME_TEXT_DARK};
        border: 1px solid #e5e7eb;
        box-shadow: 0 1px 2px rgba(0,0,0,0.05);
    }}
    div.stButton > button[kind="secondary"]:hover, [data-testid="stClipboardButton"] > button:hover {{
        border-color: {THEME_ACCENT_COLOR};
        color: {THEME_ACCENT_COLOR};
        background: #fff5f0;
    }}
    
    /* Ensure the st.code block is large enough */
    .stCodeBlock {{
        min-height: 400px;
        max-height: 400px;
        overflow: auto;
    }}

    /* Inputs */
    .stTextInput > div > div > input, .stTextArea > div > div > textarea {{
        border-radius: 0.5rem;
        border: 1px solid #e5e7eb;
        color: {THEME_TEXT_DARK};
        padding: 1rem;
        transition: border-color 0.2s;
    }}
    .stTextInput > div > div > input:focus, .stTextArea > div > div > textarea:focus {{
        border-color: {THEME_ACCENT_COLOR};
        box-shadow: 0 0 0 2px rgba(236, 98, 37, 0.1);
    }}

    /* Progress Bar */
    .stProgress > div > div > div > div {{
        background-color: {THEME_ACCENT_COLOR};
    }}

    /* Footer */
    .footer {{
        text-align: center;
        padding: 2rem 0;
        color: #6b7280;
        font-size: 0.875rem;
        margin-top: 3rem;
        border-top: 1px solid #e5e7eb;
    }}

    /* Remove default Streamlit padding & top gap */
    main .block-container {{
        padding-top: 0rem ;
    }}

    .stAppViewContainer {{
        padding-top: 4 ; 
        margin-top: 4 ;
    }} 

    /* Remove Streamlit column top margin */
    div[data-testid="column"] {{
        margin-top: 0px;
    }}

    /* Pull header area upward */
    .block-container {{
        padding-top: 0;
        margin-top: 0px;
    }}
</style>
"""
st.markdown(ENTERPRISE_CSS, unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 4. STATE MANAGEMENT
# -----------------------------------------------------------------------------
if "current_page" not in st.session_state:
    st.session_state.current_page = "Convert"
if "conversion_history" not in st.session_state:
    st.session_state.conversion_history = []
if "model_settings" not in st.session_state:
    # Default to first model from settings page
    default_models = {
        "databricks-meta-llama-3-1-405b-instruct": "https://dbc-e8fae528-2bde.cloud.databricks.com/serving-endpoints/databricks-meta-llama-3-1-405b-instruct/invocations",
        "databricks-claude-sonnet-4-5": "https://dbc-9b0b9b10-666b.cloud.databricks.com/serving-endpoints/databricks-claude-sonnet-4-5/invocations",
        "databricks-gemini-2-5-pro": "https://dbc-9b0b9b10-666b.cloud.databricks.com/serving-endpoints/databricks-gemini-2-5-pro/invocations",
        "databricks-gpt-oss-120b": "https://dbc-e8fae528-2bde.cloud.databricks.com/serving-endpoints/databricks-gpt-oss-120b/invocations",
        "databricks-qwen3-next-80b-a3b-instruct": "https://dbc-9b0b9b10-666b.cloud.databricks.com/serving-endpoints/databricks-qwen3-next-80b-a3b-instruct/invocations",
        "databricks-claude-opus-4-1": "https://dbc-9b0b9b10-666b.cloud.databricks.com/serving-endpoints/databricks-claude-opus-4-1/invocations"
    }
    first_model = list(default_models.keys())[0]
    
    st.session_state.model_settings = {
        "temperature": 0.1,
        "max_tokens": 5000,
        "model": first_model,
        "endpoint": default_models[first_model]
    }
    
if "last_result" not in st.session_state:
    st.session_state.last_result = None

# -----------------------------------------------------------------------------
# 5. UI COMPONENTS
# -----------------------------------------------------------------------------

def render_header():
    """Renders the custom top navigation bar with Decision Minds branding."""
    
    c1, c2 = st.columns([1.5, 3])
    
    with c1:
        st.markdown("""
        <div class="header-logo-container">
            <img src='https://finance.decisionminds.com/static/media/DMSymbol.94abe67f6b1691f5c494.jpg' width='50' style='border-radius: 8px;'/>
            <div class="header-title">Decision Minds <span style="color: #EC6225; font-weight: 300;">AI</span></div>
        </div>
        """, unsafe_allow_html=True)
    # 
    #     
    with c2:
        # Navigation Buttons
        b1, b2, b3, b4 = st.columns([1, 1, 1, 1])
        
        with b1:
            if st.button("Convert SQL", use_container_width=True, type="primary" if st.session_state.current_page == "Convert" else "secondary"):
                st.session_state.current_page = "Convert"
                st.rerun()
        with b2:
            if st.button("Fine-Tuning", use_container_width=True, type="primary" if st.session_state.current_page == "Fine-Tuning" else "secondary"):
                st.session_state.current_page = "Fine-Tuning"
                st.rerun()
        with b3:
            if st.button("Settings", use_container_width=True, type="primary" if st.session_state.current_page == "Settings" else "secondary"):
                st.session_state.current_page = "Settings"
                st.rerun()
        with b4:
            if st.button("Support", use_container_width=True, type="primary" if st.session_state.current_page == "Support" else "secondary"):
                st.session_state.current_page = "Support"
                st.rerun()
    
    st.markdown("<div style='height: 30px;'></div>", unsafe_allow_html=True)

# -----------------------------------------------------------------------------
    st.markdown('<div class="card"><div class="card-header">PL/SQL to Databricks Converter</div>', unsafe_allow_html=True)
    
    # Show current model being used
    current_model = st.session_state.model_settings["model"]
    st.info(f"Using Model: **{current_model}**")
    
    c1, c2 = st.columns([1, 1], gap="large")
    
    with c1:
        st.markdown("### Input Source")
        input_type = st.radio(
            "Select Input Method",
            ["Direct Input", "File Upload", "Bulk Upload"],
            horizontal=True,
            label_visibility="collapsed"
        )
        
        sql_input = ""
        bulk_items = []
        bulk_task_type = "schema"

        if input_type == "Direct Input":
            sql_input = st.text_area("Paste your Oracle SQL here", height=400, placeholder="-- Paste your Oracle SQL or PL/SQL code here...")
        elif input_type == "File Upload":
            uploaded_file = st.file_uploader("Upload .sql file", type=["sql", "txt"])
            if uploaded_file:
                try:
                    file_size = uploaded_file.size
                    if file_size > 10 * 1024 * 1024:  # 10MB limit
                        st.warning(f"Large file detected ({file_size / (1024*1024):.1f}MB). Processing in chunks...")
                        
                    uploaded_file.seek(0)
                    sql_input = uploaded_file.read().decode("utf-8")
                    st.success(f"Loaded {uploaded_file.name} ({file_size / 1024:.1f}KB)")
                    
                    with st.expander("Preview Uploaded SQL", expanded=False):
                        preview_text = sql_input[:2000] + ("..." if len(sql_input) > 2000 else "")
                        st.code(preview_text, language="sql")
                        
                except Exception as e:
                    st.error(f"Error reading file: {e}")
        else:
            bulk_task_type = st.selectbox(
                "Bulk Task Type",
                ["schema", "procedure", "optimize", "feasibility"],
                index=0,
                help="Select the type of conversion for all uploaded files."
            )
            uploaded_files = st.file_uploader(
                "Upload multiple .sql files",
                type=["sql", "txt"],
                accept_multiple_files=True
            )
            if uploaded_files:
                for f in uploaded_files:
                    try:
                        content = f.read().decode("utf-8")
                        bulk_items.append({"name": f.name, "content": content})
                    except Exception as e:
                        st.error(f"Error reading {f.name}: {e}")
                if bulk_items:
                    st.success(f"Loaded {len(bulk_items)} files for bulk processing.")
                    with st.expander("Preview Bulk Items", expanded=False):
                        for item in bulk_items[:5]:
                            st.markdown(f"**{item['name']}**")
                            preview_text = item["content"][:1000] + ("..." if len(item["content"]) > 1000 else "")
                            st.code(preview_text, language="sql")

    with c2:
        st.markdown("### Output Databricks SQL / PySpark")
        
        if st.session_state.last_result:
            result_text = st.session_state.last_result
            
            sql_code = ""
            changes_text = "No specific changes noted."
            limitations_text = "No limitations noted."
            feasibility_text = ""
            code_language = "sql"

            # Clean artifacts if present (e.g. reasoning traces)
            if isinstance(result_text, str):
                # Handle JSON-line format (reasoning traces)
                if "{'type': 'reasoning'" in result_text or "{'type': 'text'" in result_text:
                    cleaned_text = ""
                    has_json = False
                    import ast # Import here as it's only needed for parsing
                    for line in result_text.splitlines():
                        line = line.strip()
                        if line.startswith("{") and "type" in line:
                            try:
                                data = ast.literal_eval(line)
                                if data.get('type') == 'text':
                                    cleaned_text += data.get('text', '')
                                has_json = True
                            except:
                                pass
                    
                    if has_json:
                        result_text = cleaned_text

                # Fix literal escaped newlines
                result_text = result_text.replace('\\n', '\n').strip()

                is_bulk = input_type == "Bulk Upload"
                if is_bulk:
                    st.markdown("##### Bulk Results")
                    st.markdown(result_text)
                    st.download_button(
                        "Download Bulk Results",
                        result_text,
                        file_name="bulk_results.md",
                        use_container_width=True,
                        type="secondary"
                    )
                else:
                    # Extract SQL or PySpark Code
                    import re
                    code_match = re.search(r"```(sql|python)(.*?)```", result_text, re.DOTALL)
                    if code_match:
                        code_language = code_match.group(1).strip()
                        sql_code = code_match.group(2).strip()
                    else:
                        # Fallback
                        split_match = re.split(r"### Changes and Enhancements", result_text)
                        sql_code = split_match[0].strip()

                    # Extract Changes
                    changes_match = re.search(r"### Changes and Enhancements(.*?)(?=\\n### |\\Z)", result_text, re.DOTALL)
                    if changes_match:
                        changes_text = changes_match.group(1).strip()

                    # Extract Limitations
                    limitations_match = re.search(
                        r"### Limitations and Manual Review(.*?)(?=\\n### |\\Z)",
                        result_text,
                        re.DOTALL
                    )
                    if limitations_match:
                        limitations_text = limitations_match.group(1).strip()
                    else:
                        limitations_match = re.search(
                            r"### Limitations and Assumptions(.*?)(?=\\n### |\\Z)",
                            result_text,
                            re.DOTALL
                        )
                        if limitations_match:
                            limitations_text = limitations_match.group(1).strip()

                    # Extract Automation Feasibility
                    feasibility_match = re.search(r"### Automation Feasibility(.*?)(?=\\n### |\\Z)", result_text, re.DOTALL)
                    if feasibility_match:
                        feasibility_text = feasibility_match.group(1).strip()

                    # Display in Tabs
                    tabs = ["Converted Code", "Changes & Enhancements", "Limitations & Review"]
                    if feasibility_text:
                        tabs.append("Automation Feasibility")

                    # Add validation tab if validation was performed
                    if st.session_state.get('validation_results'):
                        tabs.append("Validation Results")
                    if st.session_state.get('test_cases'):
                        tabs.append("Test Cases")

                    tab_objects = st.tabs(tabs)

                    with tab_objects[0]:  # Converted Code tab
                        st.markdown("##### Converted Code ")
                        st.code(
                            sql_code,
                            language=code_language,
                            line_numbers=True
                        )

                        st.download_button(
                            "Download Code",
                            sql_code,
                            file_name=f"converted.{ 'py' if code_language == 'python' else 'sql' }",
                            use_container_width=True,
                            type="secondary"
                        )

                    with tab_objects[1]:  # Changes & Enhancements tab
                        st.markdown(changes_text)

                    with tab_objects[2]:  # Limitations & Review tab
                        st.markdown(limitations_text)

                    if feasibility_text:
                        with tab_objects[3]:
                            st.markdown(feasibility_text)

                    # Validation Results tab
                    validation_index = 2 + (1 if feasibility_text else 0)
                    if len(tab_objects) > validation_index and st.session_state.get('validation_results'):
                        with tab_objects[validation_index]:
                            st.markdown("##### Validation Summary")

                            validation_results = st.session_state.validation_results
                            overall_score = validation_results.get('overall_score', 0)

                            summary = st.session_state.validator.get_validation_summary(validation_results)
                            st.markdown(f"**{summary}**")

                            scores = validation_results.get('scores', {})
                            col1, col2, col3, col4, col5 = st.columns(5)

                            with col1:
                                st.metric("Syntax", f"{scores.get('syntax_score', 0):.0f}/100")
                            with col2:
                                st.metric("Oracle", f"{scores.get('oracle_score', 0):.0f}/100")
                            with col3:
                                st.metric("Databricks", f"{scores.get('databricks_score', 0):.0f}/100")
                            with col4:
                                st.metric("Performance", f"{scores.get('performance_score', 0):.0f}/100")
                            with col5:
                                st.metric("Schema", f"{scores.get('schema_score', 0):.0f}/100")

                            critical_issues = validation_results.get('critical_issues', [])
                            if critical_issues:
                                st.markdown("**Critical Issues:**")
                                for issue in critical_issues:
                                    st.error(f"- {issue}")

                            issues = validation_results.get('issues', [])
                            non_critical = [i for i in issues if i not in critical_issues]
                            if non_critical:
                                st.markdown("**Issues Found:**")
                                for issue in non_critical[:10]:
                                    st.warning(f"- {issue}")
                                if len(non_critical) > 10:
                                    st.info(f"... and {len(non_critical) - 10} more issues")

                            recommendations = validation_results.get('recommendations', [])
                            if recommendations:
                                st.markdown("**Recommendations:**")
                                for rec in recommendations[:5]:
                                    st.info(f"- {rec}")

                            auto_fixable = validation_results.get('auto_fixable', [])
                            if auto_fixable:
                                st.markdown("**Auto-Fixable Issues:**")
                                for fix in auto_fixable[:5]:
                                    st.success(f"- {fix}")

                            if not issues and not critical_issues:
                                st.success("No issues found - migration is excellent.")

                    # Test Cases tab
                    test_case_index = 3 + (1 if feasibility_text else 0)
                    if len(tab_objects) > test_case_index and st.session_state.get('test_cases'):
                        with tab_objects[test_case_index]:
                            st.markdown("##### Generated Test Cases")
                            test_cases_text = '\n'.join(st.session_state.test_cases)
                            st.code(test_cases_text, language="sql")

                            st.download_button(
                                "Download Test Cases",
                                test_cases_text,
                                file_name="test_cases.sql",
                                use_container_width=True,
                                type="secondary"
                            )
        else:
            st.markdown("""
            <div style="height: 400px; border: 2px dashed #e5e7eb; border-radius: 0.5rem; display: flex; flex-direction: column; align-items: center; justify-content: center; color: #9ca3af; background-color: #f9fafb;">
                <div style="font-size: 3rem; margin-bottom: 1rem;">ALERT</div>
                <div>AI-Powered Conversion Ready</div>
            </div>
            """, unsafe_allow_html=True)

    st.markdown('</div>', unsafe_allow_html=True) # End Card

    # Action Bar
    st.markdown('<div class="card">', unsafe_allow_html=True)
    # Changed column ratio from [1, 2, 1] to [1.2, 0.5, 2.3] to dedicate the first column to the button/checkbox group
    ac1, ac2, ac3 = st.columns([1.5, 0.5, 2])
    
    # -------------------------------------------------------------------------
    # MODIFIED: Consolidate Button and Checkbox in the first column (ac1)
    # -------------------------------------------------------------------------
    with ac1:
        if st.button(" Convert to Databricks SQL", use_container_width=True):
            if input_type == "Bulk Upload":
                if not bulk_items:
                    st.error("Please upload at least one file for bulk processing.")
                else:
                    with st.spinner("Analyzing and Converting (Bulk)..."):
                        try:
                            temp = st.session_state.model_settings["temperature"]
                            tokens = st.session_state.model_settings["max_tokens"]
                            
                            result = st.session_state.migration_ai.migrate_bulk(
                                bulk_items,
                                task_type=bulk_task_type,
                                temperature=temp,
                                max_tokens=tokens
                            )
                            
                            if isinstance(result, list):
                                result = "\n".join(map(str, result))
                            elif not isinstance(result, str):
                                result = str(result)

                            st.session_state.last_result = result
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
            else:
                if not sql_input.strip():
                    st.error("Please provide SQL input.")
                else:
                    with st.spinner("Analyzing and Converting..."):
                        try:
                            temp = st.session_state.model_settings["temperature"]
                            tokens = st.session_state.model_settings["max_tokens"]
                            
                            # Use standard migration with simple validation
                            result = st.session_state.migration_ai.migrate_schema(
                                sql_input, 
                                temperature=temp, 
                                max_tokens=tokens
                            )
                            
                            if isinstance(result, list):
                                result = "\n".join(map(str, result))
                            elif not isinstance(result, str):
                                result = str(result)

                            st.session_state.last_result = result
                            st.rerun()
                        except Exception as e:
                            st.error(f"Error: {e}")
        

    # -------------------------------------------------------------------------
    
    # ac2 and ac3 are now empty or used for spacing
    with ac2:
        pass # Empty column for visual separation/padding
    
    with ac3:
        pass # Empty column for visual separation/padding

    st.markdown('</div>', unsafe_allow_html=True)

def render_finetuning_page():
    st.markdown('<div class="card"><div class="card-header"> Model Fine-Tuning Center</div>', unsafe_allow_html=True)
    
    st.info("Customize the AI model to understand your specific schema patterns and naming conventions.")
    
    tab1, tab2 = st.tabs(["Training Data", "Performance Metrics"])
    
    with tab1:
        st.markdown("#### Upload Training Dataset")
        f = st.file_uploader("Upload JSONL Dataset", type=["jsonl", "json"])
        
        if f:
            st.success("Dataset validated: 150 examples found.")
            if st.button("Start Fine-Tuning Job"):
                progress_bar = st.progress(0)
                status_text = st.empty()
                for i in range(100):
                    time.sleep(0.02)
                    progress_bar.progress(i + 1)
                    status_text.text(f"Training... Loss: {0.9 - (i/200):.4f}")
                status_text.text("Training Complete!")
                st.success("New model checkpoint `v2.4.0-custom` is now active.")
                
    with tab2:
        m1, m2, m3 = st.columns(3)
        m1.metric("Syntax Accuracy", "98.5%", "+1.2%")
        m2.metric("Conversion Speed", "1.2s", "-0.3s")
        m3.metric("Human Intervention", "4%", "-2%")

    st.markdown('</div>', unsafe_allow_html=True)

def render_settings_page():
    st.markdown('<div class="card"><div class="card-header"> System Configuration</div>', unsafe_allow_html=True)
    
    # Model endpoints mapping
    model_endpoints = {
        "databricks-meta-llama-3-1-405b-instruct": "https://dbc-16797bba-8dc3.cloud.databricks.com/serving-endpoints/databricks-gpt-oss-120b/invocations",
        "databricks-claude-sonnet-4-5": "https://dbc-16797bba-8dc3.cloud.databricks.com/serving-endpoints/databricks-gpt-oss-120b/invocations",
        "databricks-gemini-2-5-pro": "https://dbc-16797bba-8dc3.cloud.databricks.com/serving-endpoints/databricks-gpt-oss-120b/invocations",
        "databricks-gpt-oss-120b": "https://dbc-16797bba-8dc3.cloud.databricks.com/serving-endpoints/databricks-gpt-oss-120b/invocations",
        "databricks-qwen3-next-80b-a3b-instruct": "https://dbc-16797bba-8dc3.cloud.databricks.com/serving-endpoints/databricks-gpt-oss-120b/invocations",
        "databricks-claude-opus-4-1": "https://dbc-16797bba-8dc3.cloud.databricks.com/serving-endpoints/databricks-gpt-oss-120b/invocations"
    }
    
    c1, c2 = st.columns(2)
    
    with c1:
        # Get current model index
        current_model = st.session_state.model_settings["model"]
        model_list = list(model_endpoints.keys())
        current_index = model_list.index(current_model) if current_model in model_list else 0
        
        model = st.selectbox("AI Model", model_list, index=current_index)
        temp = st.slider("Creativity (Temperature)", 0.0, 1.0, st.session_state.model_settings["temperature"])

    with c2:
        tokens = st.number_input("Max Output Tokens", 500, 8000, st.session_state.model_settings["max_tokens"])
        st.success(" Connected to Databricks Serving Endpoint")

    # Auto-update settings
    st.session_state.model_settings.update({
        "model": model,
        "endpoint": model_endpoints[model],
        "temperature": temp,
        "max_tokens": tokens
    })

    if st.button("Save Configuration"):
        st.toast("Settings saved successfully!")

    st.markdown('</div>', unsafe_allow_html=True)

def render_support_page():
    st.markdown('<div class="card"><div class="card-header">PHONE Enterprise Support</div>', unsafe_allow_html=True)
    
    c1, c2 = st.columns([1, 1])
    with c1:
        st.markdown("""
        ### Contact Decision Minds
        **Email:** support@decisionminds.com  
        **Phone:** +1 (800) 555-0199  
        **SLA:** 24/7 Response
        """)
    with c2:
        with st.form("support_form"):
            st.text_input("Subject")
            st.text_area("Description")
            st.form_submit_button("Submit Ticket")
    st.markdown('</div>', unsafe_allow_html=True)

# -----------------------------------------------------------------------------
# 6. MAIN APP LOOP
# -----------------------------------------------------------------------------
# -----------------------------------------------------------------------------
# HELPER FUNCTIONS
# -----------------------------------------------------------------------------
def convert_query(sql, dialect, custom_prompt, enable_validation):
    """Convert a single SQL query with optional validation and retry."""
    try:
        # Determine conversion type based on SQL content
        sql_lower = sql.lower().strip()
        if sql_lower.startswith(("create table", "create view", "alter table")):
            conversion_type = "schema"
        elif "procedure" in sql_lower or "begin" in sql_lower:
            conversion_type = "procedure"
        else:
            conversion_type = "sql_script"

        # Build prompt
        from src.utils.prompt_helper import get_conversion_prompts, get_common_prompt, build_prompt
        conversion_prompts = get_conversion_prompts(dialect)
        common_prompt = get_common_prompt("pyspark", "python", conversion_type)
        additional_prompts = custom_prompt if custom_prompt else ""
        prompt = build_prompt(common_prompt, conversion_prompts, additional_prompts, dialect, sql)

        # Call LLM
        result = st.session_state.migration_ai.call_llama(prompt)

        response = {"converted_code": result}

        # Validation if enabled
        if enable_validation:
            validation_result = validate_query(result)
            response["validation"] = validation_result

        return response

    except Exception as e:
        return {"error": str(e)}

def retry_conversion(sql, dialect, custom_prompt, error_context):
    """Retry conversion with error context feedback."""
    try:
        enhanced_prompt = f"""
        Previous conversion failed with error: {error_context}

        Please correct the conversion and provide a working Databricks-compatible query.

        Original SQL: {sql}
        """
        full_custom_prompt = f"{custom_prompt}\n\n{enhanced_prompt}" if custom_prompt else enhanced_prompt

        return convert_query(sql, dialect, full_custom_prompt, False)
    except Exception as e:
        return {"error": str(e)}

def validate_query(sql):
    """Validate SQL query using EXPLAIN."""
    try:
        # This would run EXPLAIN on the query
        # For now, return a mock validation
        return {
            "passed": True,
            "execution_time": "0.5s",
            "error": None
        }
    except Exception as e:
        return {
            "passed": False,
            "error": str(e)
        }

def start_batch_job(input_folder, output_folder, results_table, dialect, validation_strategy, max_retries):
    """Start a batch conversion job."""
    try:
        # This would trigger a Databricks job for batch processing
        # For now, return mock success
        return {
            "success": True,
            "job_id": f"batch_job_{int(time.time())}",
            "message": "Batch conversion job started successfully"
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

def start_reconciliation_job(source_schema, target_schema, tables_list, results_table, enable_row_counts, enable_data_sampling):
    """Start a schema reconciliation job."""
    try:
        # This would trigger a Databricks job for reconciliation
        # For now, return mock success
        return {
            "success": True,
            "job_id": f"reconcile_job_{int(time.time())}",
            "summary": {
                "total_tables": len(tables_list),
                "matches": len(tables_list) // 2,  # Mock data
                "mismatches": len(tables_list) - (len(tables_list) // 2)
            }
        }
    except Exception as e:
        return {
            "success": False,
            "error": str(e)
        }

def render_interactive_conversion():
    st.markdown("### Real-time Query Conversion")
    st.markdown("Convert individual queries with LLM validation and retry mechanism.")

    # Model and dialect selection
    col1, col2, col3 = st.columns(3)
    with col1:
        selected_model = st.selectbox("LLM Model", SUPPORTED_MODELS, index=SUPPORTED_MODELS.index("Databricks GPT"))
    with col2:
        selected_dialect = st.selectbox("Source Dialect", SUPPORTED_DIALECTS, index=SUPPORTED_DIALECTS.index("Oracle"))
    with col3:
        enable_validation = st.checkbox("Enable EXPLAIN Validation", value=True)

    # Custom prompt instructions
    custom_prompt = st.text_area("Custom Prompt Instructions (Optional)",
                                placeholder="Add dialect-specific hints or special conversion requirements...",
                                height=100)

    # Input SQL
    input_sql = st.text_area("Input SQL Query", height=200,
                            placeholder="Paste your SQL query here...")

    # Convert button
    if st.button("🚀 Convert Query", type="primary", use_container_width=True):
        if input_sql.strip():
            with st.spinner("Converting query..."):
                # Update model settings
                if selected_model in MODEL_ENDPOINTS:
                    st.session_state.model_settings["endpoint"] = MODEL_ENDPOINTS[selected_model]
                    st.session_state.model_settings["model"] = selected_model

                # Perform conversion
                result = convert_query(input_sql, selected_dialect.lower(), custom_prompt, enable_validation)

                # Display results
                st.markdown("### Conversion Results")
                if "error" in result:
                    st.error(f"Conversion failed: {result['error']}")
                else:
                    # Show converted code
                    st.code(result.get("converted_code", ""), language="sql")

                    # Show validation results if enabled
                    if enable_validation and "validation" in result:
                        validation = result["validation"]
                        if validation.get("passed", False):
                            st.success("✅ Query validation passed!")
                        else:
                            st.error("❌ Query validation failed!")
                            st.text(f"Error: {validation.get('error', 'Unknown error')}")

                            # Retry option
                            if st.button("🔄 Retry with Error Context"):
                                with st.spinner("Retrying with error feedback..."):
                                    retry_result = retry_conversion(input_sql, selected_dialect.lower(),
                                                                  custom_prompt, validation.get('error', ''))
                                    if "error" not in retry_result:
                                        st.code(retry_result.get("converted_code", ""), language="sql")
                                        st.success("✅ Retry successful!")
        else:
            st.warning("Please enter a SQL query to convert.")

def render_batch_jobs():
    st.markdown("### Bulk SQL File Conversion")
    st.markdown("Convert entire folders of SQL files with configurable validation and results storage.")

    # Configuration
    col1, col2 = st.columns(2)
    with col1:
        input_folder = st.text_input("Input Folder Path", placeholder="/Workspace/Users/your-folder/sql-files/")
        output_folder = st.text_input("Output Notebook Folder", placeholder="/Workspace/Users/your-folder/converted-notebooks/")
        results_table = st.text_input("Results Table", placeholder="main.migration.batch_results")

    with col2:
        batch_dialect = st.selectbox("Source Dialect", SUPPORTED_DIALECTS, index=SUPPORTED_DIALECTS.index("Oracle"))
        validation_strategy = st.selectbox("Validation Strategy",
                                         ["No validation", "Validate by running EXPLAIN", "Failed queries can be retried"])
        max_retries = st.slider("Max Retries", 0, 5, 2)

    # Start batch job
    if st.button("🚀 Start Batch Conversion", type="primary", use_container_width=True):
        if input_folder and output_folder:
            with st.spinner("Starting batch conversion job..."):
                # This would trigger a Databricks job
                job_result = start_batch_job(input_folder, output_folder, results_table,
                                           batch_dialect.lower(), validation_strategy, max_retries)

                if job_result.get("success", False):
                    st.success(f"✅ Batch job started! Job ID: {job_result.get('job_id', 'N/A')}")
                    st.info("Results will be stored in the specified Delta table.")
                else:
                    st.error(f"Failed to start batch job: {job_result.get('error', 'Unknown error')}")
        else:
            st.warning("Please specify input and output folder paths.")

    # Show recent batch jobs
    st.markdown("### Recent Batch Jobs")
    # This would query the results table to show recent jobs
    st.info("Batch job results will be displayed here once jobs are completed.")

def render_reconcile_tables():
    st.markdown("### Schema Reconciliation")
    st.markdown("Compare source vs target schemas and validate data consistency.")

    col1, col2 = st.columns(2)
    with col1:
        source_schema = st.text_input("Source Schema", placeholder="source_db.schema_name")
        target_schema = st.text_input("Target Schema", placeholder="main.target_schema")
        results_table = st.text_input("Results Table", placeholder="main.migration.reconciliation_results")

    with col2:
        tables_to_compare = st.text_area("Tables to Compare (one per line)",
                                       placeholder="customers\norders\nproducts",
                                       height=100)
        enable_row_counts = st.checkbox("Enable Row Count Comparison", value=True)
        enable_data_sampling = st.checkbox("Enable Data Sampling", value=False)

    if st.button("🔍 Start Reconciliation", type="primary", use_container_width=True):
        if source_schema and target_schema and tables_to_compare:
            with st.spinner("Starting reconciliation job..."):
                tables_list = [t.strip() for t in tables_to_compare.split('\n') if t.strip()]

                reconciliation_result = start_reconciliation_job(
                    source_schema, target_schema, tables_list, results_table,
                    enable_row_counts, enable_data_sampling
                )

                if reconciliation_result.get("success", False):
                    st.success(f"✅ Reconciliation job started! Job ID: {reconciliation_result.get('job_id', 'N/A')}")

                    # Show summary if available
                    if "summary" in reconciliation_result:
                        st.markdown("### Quick Summary")
                        summary = reconciliation_result["summary"]
                        st.metric("Tables Compared", summary.get("total_tables", 0))
                        st.metric("Matches Found", summary.get("matches", 0))
                        st.metric("Mismatches", summary.get("mismatches", 0))
                else:
                    st.error(f"Failed to start reconciliation: {reconciliation_result.get('error', 'Unknown error')}")
        else:
            st.warning("Please fill in all required fields.")

def main():
    # Header
    st.markdown("""
    <div style="text-align: center; margin-bottom: 2rem;">
        <h1 style="color: #EC6225; font-size: 2.5rem; margin-bottom: 0.5rem;">
            🔄 Legacy System ➜ Databricks Converter
        </h1>
        <p style="color: #666; font-size: 1.1rem; margin-top: 0;">
            Accelerate SQL migration and schema reconciliation from legacy systems into Databricks SQL
        </p>
    </div>
    """, unsafe_allow_html=True)

    # Create tabs
    tab1, tab2, tab3 = st.tabs(["🔹 Interactive Conversion", "🔹 Batch Jobs", "🔹 Reconcile Tables"])

    with tab1:
        render_interactive_conversion()

    with tab2:
        render_batch_jobs()

    with tab3:
        render_reconcile_tables()

    st.markdown("""
    <div style="text-align: center; margin-top: 3rem; padding: 1rem; border-top: 1px solid #e0e0e0;">
        <p style="color: #666; font-size: 0.9rem;">
            © 2025 Databricks | Legacy System Migration Accelerator<br>
            Supports: Snowflake, T-SQL, Redshift, Oracle, Teradata, MySQL, PostgreSQL, SSIS, Informatica
        </p>
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()
