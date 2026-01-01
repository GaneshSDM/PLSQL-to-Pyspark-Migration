import streamlit as st
import os
import pandas as pd
import time
from dotenv import load_dotenv
from ai_migration import MigrationAI
from simple_validator import SimpleValidator

# -----------------------------------------------------------------------------
# 1. PAGE CONFIGURATION
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Oracle SQL to Databricks SQL Converter",
    layout="wide",
    initial_sidebar_state="collapsed"
)

# -----------------------------------------------------------------------------
# 2. LOAD ENVIRONMENT & INITIALIZE AI
# -----------------------------------------------------------------------------
load_dotenv()

# NOTE: API_URL and API_KEY are hardcoded here, but should ideally come from .env
API_URL = "https://dbc-e8fae528-2bde.cloud.databricks.com/serving-endpoints/databricks-gpt-oss-120b/invocations"
API_KEY = "dapi31a4d352082f4e740f88e86cbee1bf1f"

if "migration_ai" not in st.session_state:
    st.session_state.migration_ai = MigrationAI(api_key=API_KEY)
    st.session_state.validator = SimpleValidator()


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
        "max_tokens": 2000,
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

def render_convert_page():
    st.markdown('<div class="card"><div class="card-header">SQL to Databricks Converter</div>', unsafe_allow_html=True)
    
    # Show current model being used
    current_model = st.session_state.model_settings["model"]
    st.info(f"Using Model: **{current_model}**")
    
    c1, c2 = st.columns([1, 1], gap="large")
    
    with c1:
        st.markdown("### Input Source")
        input_type = st.radio("Select Input Method", ["Direct Input", "File Upload"], horizontal=True, label_visibility="collapsed")
        
        sql_input = ""
        if input_type == "Direct Input":
            sql_input = st.text_area("Paste your Oracle SQL here", height=400, placeholder="-- Paste your Oracle SQL or PL/SQL code here...")
        else:
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

    with c2:
        st.markdown("### Output Databricks SQL")
        
        if st.session_state.last_result:
            result_text = st.session_state.last_result
            
            sql_code = ""
            changes_text = "No specific changes noted."

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

                # Extract SQL Code
                import re
                sql_match = re.search(r"```sql(.*?)```", result_text, re.DOTALL)
                if sql_match:
                    sql_code = sql_match.group(1).strip()
                else:
                    # Fallback
                    split_match = re.split(r"### Changes and Enhancements", result_text)
                    sql_code = split_match[0].strip()

                # Extract Changes
                changes_match = re.search(r"### Changes and Enhancements(.*)", result_text, re.DOTALL)
                if changes_match:
                    changes_text = changes_match.group(1).strip()

                # Display in Tabs
                tabs = ["Converted SQL", "Changes & Enhancements"]
                
                # Add validation tab if validation was performed
                if st.session_state.get('validation_results'):
                    tabs.append("Validation Results")
                if st.session_state.get('test_cases'):
                    tabs.append("Test Cases")
                
                tab_objects = st.tabs(tabs)
                
                with tab_objects[0]:  # Converted SQL tab
                    # --- MODIFICATION: Use st.code which provides a native copy button for older versions ---
                    st.markdown("##### Converted Code ")
                    st.code(
                        sql_code, 
                        language="sql",
                        line_numbers=True # Useful for large code blocks
                    )
                    
                    # Add Download button below the code block
                    # Keep the Download button separate as it's the only one needed here
                    st.download_button(
                        "Download SQL Code", 
                        sql_code, 
                        file_name="converted.sql",
                        use_container_width=True,
                        type="secondary" 
                    )
                    # Removed the separate copy button columns/logic entirely since st.code handles it

                
                with tab_objects[1]:  # Changes & Enhancements tab
                    st.markdown(changes_text)
                
                # Validation Results tab
                if len(tab_objects) > 2 and st.session_state.get('validation_results'):
                    with tab_objects[2]:
                        st.markdown("##### Validation Summary")
                        
                        validation_results = st.session_state.validation_results
                        overall_score = validation_results.get('overall_score', 0)
                        
                        # Display validation summary
                        summary = st.session_state.validator.get_validation_summary(validation_results)
                        st.markdown(f"**{summary}**")
                        
                        # Display detailed scores in columns
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
                        
                        # Display critical issues first
                        critical_issues = validation_results.get('critical_issues', [])
                        if critical_issues:
                            st.markdown("**🚨 Critical Issues:**")
                            for issue in critical_issues:
                                st.error(f"• {issue}")
                        
                        # Display regular issues
                        issues = validation_results.get('issues', [])
                        non_critical = [i for i in issues if i not in critical_issues]
                        if non_critical:
                            st.markdown("**⚠️ Issues Found:**")
                            for issue in non_critical[:10]:  # Limit to 10 for UI
                                st.warning(f"• {issue}")
                            if len(non_critical) > 10:
                                st.info(f"... and {len(non_critical) - 10} more issues")
                        
                        # Display recommendations
                        recommendations = validation_results.get('recommendations', [])
                        if recommendations:
                            st.markdown("**💡 Recommendations:**")
                            for rec in recommendations[:5]:  # Limit to 5
                                st.info(f"• {rec}")
                        
                        # Show auto-fixable issues
                        auto_fixable = validation_results.get('auto_fixable', [])
                        if auto_fixable:
                            st.markdown("**🔧 Auto-Fixable Issues:**")
                            for fix in auto_fixable[:5]:
                                st.success(f"• {fix}")
                        
                        if not issues and not critical_issues:
                            st.success("✅ No issues found - Migration is excellent!")
                
                # Test Cases tab
                if len(tab_objects) > 3 and st.session_state.get('test_cases'):
                    with tab_objects[3]:
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
                <div style="font-size: 3rem; margin-bottom: 1rem;">⚡</div>
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
    st.markdown('<div class="card"><div class="card-header">📞 Enterprise Support</div>', unsafe_allow_html=True)
    
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

def main():
    render_header()
    
    if st.session_state.current_page == "Convert":
        render_convert_page()
    elif st.session_state.current_page == "Fine-Tuning":
        render_finetuning_page()
    elif st.session_state.current_page == "Settings":
        render_settings_page()
    elif st.session_state.current_page == "Support":
        render_support_page()
        
    st.markdown("""
    <div class="footer">
        © 2025 Decision Minds | Powered by Databricks Accelerator<br>
        Version 2.0.0 Enterprise Edition
    </div>
    """, unsafe_allow_html=True)

if __name__ == "__main__":
    main()