
# integrate.py
import pandas as pd
from query_processing import process_query
from process_sql import execute_sql_query  
from API_config import GEMINI_API_KEY

def process_data(chat_context, file_url):
    """
    Process the user query and CSV file, returning the result.
    
    Args:
        chat_context (str): The user query.
        file_url (str): URL to the CSV file.
    
    Returns:
        dict: Contains original query, refined query, status, SQL query, and execution result (if successful).
    """
    # --- Original debug prints ---
    print("Processing in integrate.py...")
    print("Chat Context:", chat_context)
    print("File URL:", file_url)

    # Use GEMINI API key
    api_key = GEMINI_API_KEY

    try:
        if file_url.lower().endswith(('.xls', '.xlsx')):
            df = pd.read_excel(file_url, engine='openpyxl')
        elif file_url.lower().endswith('.csv'):
            df = pd.read_csv(file_url, encoding='latin1')
        else:
            return {"error": "Unsupported file format. Only CSV and Excel files are supported."}
    except Exception as e:
        return {"error": f"Failed to load file from URL: {e}"}

    columns = df.columns.tolist()
    table_name = "df"

    # --- Send to LLM Agents ---
    response = process_query(
        api_key=api_key,
        dataset_columns=columns,
        df=df,
        user_query=chat_context
    )

    # Prepare result dict
    result = {
        "original_query": response.get("original_query"),
        "refined_query": response.get("refined_query"),
        "status": response.get("status"),
        "sql_query": response.get("sql_query"),
        "execution_result": None
    }
    print("\n\nresult\n\n",result)
    # --- If SQL is valid, execute it ---
    if response.get("status") and response.get("sql_query"):

        
        sql_query = response["sql_query"]["SQL"]  # Extract SQL string

        # Execute SQL query using pandasql or your execute_sql_query function
        result_df = execute_sql_query(df, sql_query, table_name=table_name)

        if result_df is not None:
            # --- Optionally save to file ---
            output_path = "sql_query_output.json"
            result_df.to_json(output_path, orient="records", indent=4)
            print(f"✅ SQL result also saved to {output_path}")

            result["execution_result"] = result_df.to_dict(orient="records")
        else:
            result["execution_result"] = "SQL execution failed."


    else:
        result["execution_result"] = "Query cannot be executed."
    print("EXECUTED RESULT:\n\n",result["execution_result"])

    return result["execution_result"]
