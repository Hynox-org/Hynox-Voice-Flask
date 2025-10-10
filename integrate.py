import pandas as pd
from query_processing import process_query
from process_sql import execute_sql_query  
from API_config import GEMINI_API_KEY

# print("Using API key:", GEMINI_API_KEY)
GEMINI_API_KEY=GEMINI_API_KEY


# --- Load dataset ---
FILE_PATH = r"https://kipkprekfsybarwttqkr.supabase.co/storage/v1/object/public/file-storage/employee_details_15x100.csv"
df = pd.read_csv(FILE_PATH)
# print(df.head())

COLUMNS = df.columns.tolist()
TABLE_NAME = "df"

# --- User Query ---
# userQuery = "Calculate the average price of products in each category"
# userQuery = "I’m curious to see how our revenue has been evolving over time. Could you provide a breakdown that highlights the patterns in our sales across each month for roughly the last year? I’d like to understand if there are any noticeable trends or fluctuations, rather than just the total numbers." 
# userQuery="Calculate the average price of products in each category" 
# userQuery="Find the total sales amount for all products."
# userQuery="give me top 10 sales product"
# userQuery="Give me" 
# userQuery="Give me products"
# userQuery="Can you show me sales numbers but only for the products that ‘matter most’?"
# userQuery="Show monthly sales trend for the past 12 months."
# userQuery="Show me the sales trend for products that weren’t really selling well, but only if they match what our top customers might have liked."
userQuery="show me top 2 employee names who has greater salary than others"

# --- Send to LLM Agents ---
response = process_query(api_key=GEMINI_API_KEY, dataset_columns=COLUMNS,df=df, user_query=userQuery)

# --- Display Results ---
print("\n--- QUERY RESULT ---\n")
print("Original Query:", response["original_query"])
print("Refined Query:", response["refined_query"])
print("Execution Status:", response["status"])
print("SQL Query:", response["sql_query"])

# --- If SQL is valid, execute it ---
if response["status"] and response["sql_query"]:
    sql_query = response["sql_query"]["SQL"]  # ✅ Extract the actual SQL string

    # Execute SQL query using pandasql
    result_df = execute_sql_query(df, sql_query, table_name=TABLE_NAME)

    if result_df is not None:
        print("\n--- SQL EXECUTION RESULT ---\n")
        # print(result_df)  # show first few rows of result
        output_path = "sql_query_output.json"
        result_df.to_json(output_path, orient="records", indent=4)
        result_json = pd.read_json(output_path)
        print(result_json)
    else:
        print("\n⚠️ SQL execution failed.\n")


else:
    print("\n⚠️ Query cannot be executed. Status:", response["status"])
