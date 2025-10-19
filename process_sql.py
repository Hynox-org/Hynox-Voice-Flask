import pandas as pd
from pandasql import sqldf
import duckdb

def execute_sql_query(df: pd.DataFrame, sql_query: str, table_name: str = "df") -> pd.DataFrame:
    """
    Executes an SQL query on a Pandas DataFrame using pandasql.
    If pandasql fails, automatically retries with DuckDB.

    Parameters:
    - df: Pandas DataFrame to query.
    - sql_query: SQL query string.
    - table_name: Table name to reference in the SQL (default: 'df').

    Returns:
    - Pandas DataFrame with query result.
    """
    try:
        # --- Try with pandasql first ---
        query_result = sqldf(sql_query, {table_name: df})
        if not query_result.empty:
            print("✅ Query executed successfully with pandasql.")
            query_result = query_result.round(2)
            return query_result
        else:
            print("⚠️ Empty result from pandasql — trying DuckDB.")
    except Exception as e:
        print(f"⚠️ pandasql failed: {e}\nSwitching to DuckDB...")

    # --- Fallback: DuckDB ---
    try:
        duckdb.register(table_name, df)
        query_result = duckdb.sql(sql_query).df()
        if not query_result.empty:
            print("✅ Query executed successfully with DuckDB.")
            query_result = query_result.round(2)
            return query_result
        else:
            print("⚠️ Empty result from DuckDB")
            print("Both PandaSql and DuckDB Failed to Execute the query")
    except Exception as e:
        print(f"❌ DuckDB also failed: {e}")
        return pd.DataFrame()
