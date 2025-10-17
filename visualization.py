# visualization.py

from google import genai
import pandas as pd
import json

def get_visualization_json(api_key, query, data):
    """
    Ask Gemini which visualization type fits best and return JSON for frontend.
    
    Returns:
        dict: {"type": "<chart_type>", "data": [...]}
    """
    df = pd.DataFrame(data)
    sample_data = df.head(3).to_dict(orient='records')

    prompt = f"""
You are a data visualization assistant.

Given:
User Query: {query}
Data Columns: {list(df.columns)}
Sample Data: {json.dumps(sample_data)}

Choose the best visualization type from: ["bar", "line", "pie", "scatter", "kpi", "table"].

Return ONLY a JSON object in this format:
{{"visualization": "TYPE"}}
    """

    client = genai.Client(api_key=api_key)
    response = client.models.generate_content(
        model="gemini-2.0-flash",
        contents=prompt
    )

    try:
        vis = json.loads(response.text)
        chart_type = vis.get("visualization", "table")
    except Exception as e:
        print("Gemini parsing error:", e)
        print("Response text:", response.text)
        chart_type = "table"

    # Return JSON-ready object for frontend
    return {
        "type": chart_type,
        "data": data
    }
