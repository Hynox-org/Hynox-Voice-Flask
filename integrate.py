# integrate.py
import pandas as pd
from query_processing import process_query, GeminiLLM
from process_sql import execute_sql_query
from API_config import GEMINI_API_KEY
from visualization import get_visualization_json

def _create_response(status, summary=None, data=None, table=None, error=None):
    """Helper function to create a standardized response object."""
    return {
        "status": status,
        "summary": summary,
        "data": data,
        "table": table,
        "error": error
    }

def _transform_data_for_charts(data):
    """
    Transforms data from SQL result format to a generic chart-compatible format.
    The first non-numeric column is mapped to 'name', and subsequent numeric columns
    are mapped to 'value1', 'value2', etc.
    """
    if not data:
        return []

    df = pd.DataFrame(data)
    transformed_data = []

    # Identify columns for 'name' and 'values'
    name_column = None
    value_columns = []

    for col in df.columns:
        # Check if the column is numeric
        is_numeric = pd.to_numeric(df[col], errors='coerce').notna().all()
        
        if not is_numeric and name_column is None:
            name_column = col
        elif is_numeric:
            value_columns.append(col)

    for _, row in df.iterrows():
        new_item = {}
        if name_column:
            new_item['name'] = row[name_column]
        
        for i, val_col in enumerate(value_columns):
            new_item[f'value{i+1}'] = row[val_col]
        
        transformed_data.append(new_item)
        
    return transformed_data

def generate_conversational_response(api_key, query):
    gemini_llm = GeminiLLM(api_key=api_key)
    prompt = f"The user sent the following message: '{query}'. This is not a query that can be answered from the available data. Provide a short, friendly, conversational response. If it's a question you can't answer, say you can't answer it from the data. If it's a statement of gratitude, respond politely."
    response = gemini_llm.generate(prompt)
    return _create_response(status="conversational", summary=response)

def handle_greeting(query):
    greetings = ["hi", "hello", "greetings", "good morning", "good afternoon", "good evening"]
    if query.lower().strip() in greetings:
        response_text = f"{query.capitalize()}, how can I help you?"
        return _create_response(status="conversational", summary=response_text)
    return None

def process_data(chat_context, file_url):
    print("Processing in integrate.py...")
    
    greeting_response = handle_greeting(chat_context)
    if greeting_response:
        return greeting_response

    print("Chat Context:", chat_context)
    print("File URL:", file_url)

    api_key = GEMINI_API_KEY

    try:
        if file_url.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file_url, engine='openpyxl')
        elif file_url.lower().endswith('.csv'):
            df = pd.read_csv(file_url, encoding='latin1')
        else:
            return _create_response(
                status="error",
                summary="Unsupported file format.",
                error="Unsupported file format. Only CSV and Excel files are supported."
            )
    except Exception as e:
        return _create_response(
            status="error",
            summary="Failed to load file from URL.",
            error=f"Failed to load file from URL: {e}"
        )

    columns = df.columns.tolist()
    table_name = "df"

    # --- Send to LLM Agents ---
    response = process_query(
        api_key=api_key,
        dataset_columns=columns,
        df=df,
        user_query=chat_context
    )

    # --- If SQL is valid, execute it ---
    if response.get("status") and response.get("sql_query"):
        sql_query = response["sql_query"]["SQL"]

        result_df = execute_sql_query(df, sql_query, table_name=table_name)

        if result_df is not None and not result_df.empty:
            # Transform data for visualization
            transformed_chart_data = _transform_data_for_charts(result_df.to_dict(orient="records"))
            
            # --- Get visualization info ---
            visualization_json = get_visualization_json(api_key, response.get("refined_query"), transformed_chart_data)
            
            # --- Create table object ---
            table_json = {
                "columns": result_df.columns.tolist(),
                "rows": result_df.to_dict(orient="records")
            }

            return _create_response(
                status="success",
                summary=response.get("refined_query", "Query executed successfully."),
                data=visualization_json,
                table=table_json
            )
        else:
            return _create_response(
                status="error",
                summary="SQL execution failed or returned no data.",
                error="SQL execution failed or the query returned an empty result set."
            )
    else:
        # --- If no SQL, generate conversational response ---
        return generate_conversational_response(api_key, chat_context)
