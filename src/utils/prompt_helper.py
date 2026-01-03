import yaml
import os

def get_conversion_prompts(src_dialect):
    """
    Load conversion prompts for the source dialect.
    """
    file_path = os.path.join(os.path.dirname(__file__), '..', 'resources', 'conversion_prompts.yaml')
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    return data.get(src_dialect.lower(), {})

def get_common_prompt(output_mode, language, prompt_type):
    """
    Load common prompt template.
    output_mode: e.g., 'pyspark'
    language: e.g., 'python'
    prompt_type: e.g., 'procedure', 'schema'
    """
    file_path = os.path.join(os.path.dirname(__file__), '..', 'resources', 'common_prompts.yaml')
    with open(file_path, 'r') as f:
        data = yaml.safe_load(f)
    return data.get(output_mode, {}).get(language, {}).get(prompt_type, "")

def build_prompt(common_prompt, conversion_prompts, additional_prompts, src_dialect, input_sql):
    """
    Build the full prompt by replacing placeholders.
    """
    prompt = common_prompt
    prompt = prompt.replace("%%##conversion_prompts##%%", yaml.dump(conversion_prompts, default_flow_style=False))
    prompt = prompt.replace("%%##additional_prompts##%%", additional_prompts or "")
    prompt = prompt.replace("%%##src_dialect##%%", src_dialect)
    prompt = prompt.replace("%%##input_sql##%%", input_sql)
    return prompt