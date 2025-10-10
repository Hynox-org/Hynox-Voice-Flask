import json
from google import genai
import pandas as pd

class GeminiLLM:
    def __init__(self, api_key: str, model: str = "gemini-2.5-flash"):
        self.client = genai.Client(api_key=api_key)
        self.model = model

    def generate(self, prompt: str) -> str:
        response = self.client.models.generate_content(model=self.model, contents=prompt)
        return response.text.strip()


# Query Refinement Agent
class RefineQueryAgent:
    def __init__(self, llm: "GeminiLLM"):
        self.llm = llm

    def refine_query(self, user_query: str, focus_point: str = None, dataset_columns: list = None) -> str:
        prompt = f"""
You are an intelligent query refinement AI.

Base query: "{user_query}"
"""
        if focus_point:
            prompt += f"Focus point: {focus_point}\n"
        if dataset_columns:
            prompt += f"Dataset columns: {', '.join(dataset_columns)}\n"

        prompt += """
                Focus Point: 
                - Refine Query to the simplest form
                Task:
                - Identify patterns, trends, comparisons, or summaries that are derivable from the data.
                - Return the refined query in **one short phrase only**, maximum ~10 words.
                - Focus on what can be directly computed from the dataset.
                - Ignore verbose explanations.

                Restrictions:
                - Return only the refined query text.
                - No explanations, commentary, or verbose descriptions.
                """
        return self.llm.generate(prompt)



# SQL Check Agent
class SQLCheckAgent:
    def __init__(self, llm: GeminiLLM, dataset_columns: list):
        self.llm = llm
        self.dataset_columns = dataset_columns

    def check_query_status(self, refined_query: str, focus_point: str = None, notes: str = None) -> dict:
        """
        Checks if the refined query can be executed using the dataset columns.
        Returns strictly {"status": true} or {"status": false}.
        """

        # Fully LLM-driven check
        prompt = f"""
                Task:
                Determine whether the refined query can be executed using the provided dataset columns.

                Focus Point:
                {focus_point or "None"}

                Restrictions:
                - Return ONLY a single JSON object: {{ "status": true }} or {{ "status": false }}.
                - Do NOT include any code fences, explanations, commentary, or extra text.
                - Do NOT use markdown or any formatting.
                - Output must be valid JSON only.

                Notes:
                - Refined Query: "{refined_query}"
                - Dataset Columns: {', '.join(self.dataset_columns)}
                {notes or ""}
                """

        llm_response = self.llm.generate(prompt).strip()

        try:
            # Parse JSON safely
            status_json = json.loads(llm_response)
            if "status" in status_json:
                return status_json
        except Exception as e:
            print("Failed to parse LLM response:", llm_response, e)

        # Fallback if LLM fails
        return {"status": False}



class SQLGeneratorAgent:
    def __init__(self, llm: "GeminiLLM", dataset_columns: list, df: pd.DataFrame, table_name: str):
        self.llm = llm
        self.dataset_columns = dataset_columns
        self.df = df
        self.table_name = table_name
        self.dataset_summary = self._summarize_dataset(df)  # <-- add this

    def _summarize_dataset(self, df: pd.DataFrame) -> dict:
        summary = {}
        for col in df.columns:
            col_data = df[col].dropna()
            if pd.api.types.is_datetime64_any_dtype(col_data):
                summary[col] = {"type": "datetime", "min": str(col_data.min().date()), "max": str(col_data.max().date())}
            elif pd.api.types.is_numeric_dtype(col_data):
                summary[col] = {"type": "numeric", "min": float(col_data.min()), "max": float(col_data.max())}
            elif col_data.nunique() < 15:
                summary[col] = {"type": "categorical", "unique_values": col_data.unique().tolist()}
            else:
                sample_values = col_data.sample(min(3, len(col_data))).tolist()
                summary[col] = {"type": "text", "sample_values": sample_values}
        return summary

    def generate_sql(self, refined_query: str) -> dict:
        columns_text = ", ".join(self.dataset_columns)
        summary_text = "\n".join([f"- {col}: {info}" for col, info in self.dataset_summary.items()])

        prompt = f"""
                You are an expert SQL generator.

                Task:
                Generate a single, accurate SQL query that answers the question:
                "{refined_query}"

                Dataset Table:
                {self.table_name}

                Focus Point:
                - SQL must be strictly based on the dataset columns: {columns_text}
                - Only include columns required to answer the refined query
                - Ensure the result includes essential values to answer the query
                - Use only values or ranges actually present in the dataset
                - Do NOT assume any external dates, times, or values
                - SQL must be compatible with SQLite / pandasql

                Restrictions:
                - Return ONLY the SQL query text
                - Only select columns necessary for the query result; do not use SELECT *
                - No explanations, comments, markdown, JSON, or extra words
                - SQL must end with a semicolon
                - Response must be a single continuous line of text

                Notes:
                Dataset Summary:
                {summary_text}
                """


        sql_query_text = self.llm.generate(prompt).strip()

        # Cleanup any extra markdown or characters
        sql_only = (
            sql_query_text.replace("```sql", "")
            .replace("```", "")
            .replace("\n", " ")
            .replace("\r", "")
            .strip()
        )

        return {"SQL": sql_only}



# SQL Generator Agent

# class SQLGeneratorAgent:
#     def __init__(self, llm: "GeminiLLM", dataset_columns: list, df: pd.DataFrame, table_name: str):
#         """
#         Generates dataset-aware SQL queries from natural language.
#         - SQL is strictly based on the provided dataset columns.
#         - Learns schema and summary of the DataFrame.
#         - Adapts automatically to numeric, text, categorical, or date columns.
#         """
#         self.llm = llm
#         self.dataset_columns = dataset_columns
#         self.df = df
#         self.table_name = table_name
#         self.dataset_summary = self._summarize_dataset(df)

#     def _summarize_dataset(self, df: pd.DataFrame) -> dict:
#         """
#         Summarizes the dataset to guide SQL generation.
#         Captures column type, sample values, and data range (if applicable).
#         """
#         summary = {}
#         for col in df.columns:
#             col_data = df[col].dropna()

#             if pd.api.types.is_datetime64_any_dtype(col_data):
#                 summary[col] = {"type": "datetime", "min": str(col_data.min().date()), "max": str(col_data.max().date())}
#             elif pd.api.types.is_numeric_dtype(col_data):
#                 summary[col] = {"type": "numeric", "min": float(col_data.min()), "max": float(col_data.max())}
#             elif col_data.nunique() < 15:
#                 summary[col] = {"type": "categorical", "unique_values": col_data.unique().tolist()}
#             else:
#                 sample_values = col_data.sample(min(3, len(col_data))).tolist()
#                 summary[col] = {"type": "text", "sample_values": sample_values}

#         return summary

#     def generate_sql(self, refined_query: str) -> dict:
#         """
#         Generates SQLite-compatible SQL strictly using the dataset columns.
#         Focus point: SQL must be dataset-driven, intent-aware, and output-only.
#         """
#         summary_text = "\n".join([f"- {col}: {info}" for col, info in self.dataset_summary.items()])
#         columns_text = ", ".join(self.dataset_columns)

#         prompt = f"""
#     Task:
#     Generate a single, accurate SQL query that answers the question:
#     "{refined_query}"

#     Dataset Table:
#     {self.table_name}

#     Focus Point:
#     - SQL must be generated strictly using the provided dataset columns: {columns_text}
#     - SQL logic must rely solely on data present in the dataset.
#     - Understand the user’s intent and generate an appropriate SQL query.
#     - Must be compatible with SQLite / pandasql.
#     - Only produce the SQL query; do NOT add explanations, comments, markdown, or extra words.

#     Restrictions:
#     - Base all logic, filters, and aggregations strictly on the dataset summary below.
#     - Always assign descriptive aliases to calculated columns.
#     - SQL must end with a semicolon.
#     - The response must be a single continuous line of text.

#     Notes:
#     Dataset Summary:
#     {summary_text}
#     """

#         sql_query_text = self.llm.generate(prompt).strip()

#         # Cleanup (remove any unexpected markdown fences or extra lines)
#         sql_only = (
#             sql_query_text.replace("```sql", "")
#             .replace("```", "")
#             .replace("\n", " ")
#             .replace("\r", "")
#             .strip()
#         )

#         return {"SQL": sql_only}






# Main processing function
def process_query(api_key: str, dataset_columns: list, df: pd.DataFrame, user_query: str):
    gemini_llm = GeminiLLM(api_key=api_key)
    refine_agent = RefineQueryAgent(llm=gemini_llm)
    sql_check_agent = SQLCheckAgent(llm=gemini_llm, dataset_columns=dataset_columns)
    sql_generator_agent = SQLGeneratorAgent(
        llm=gemini_llm,
        dataset_columns=dataset_columns,
        df=df,            # Pass the dataframe here
        table_name="df"
    )

    refined_query = refine_agent.refine_query(user_query, dataset_columns=dataset_columns)
    status = sql_check_agent.check_query_status(refined_query)

    if status.get("status"):
        sql_query = sql_generator_agent.generate_sql(refined_query)
        return {
            "original_query": user_query,
            "refined_query": refined_query,
            "status": True,
            "sql_query": sql_query
        }
    else:
        return {
            "original_query": user_query,
            "refined_query": refined_query,
            "status": False,
            "sql_query": None
        }
