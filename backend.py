# backend.py
from flask import Flask, request, jsonify
import os
from flask_cors import CORS
from integrate import process_data  # ✅ Import your processing function

app = Flask(__name__)

# Get CORS origins from environment variable, with a fallback for development
CORS(app, resources={r"/*": {"origins": "*"}})

@app.route('/backend', methods=['POST'])
def backend():
    data = request.get_json()
    chat_context = data.get('chat_context')
    file_url = data.get('file_url')

    # print("Received in backend.py")
    # print("Chat Context:", chat_context)
    # print("File URL:", file_url)

    # ✅ Send the data to integrate.py
    try:
        # Process data safely and get the standardized response
        response_data = process_data(chat_context, file_url)
        print("Response from integrate.py:", response_data)
        return jsonify(response_data)
    except Exception as e:
        print("Error in backend:", e)
        # Return a standardized error response
        return jsonify({
            "status": "error",
            "summary": "An unexpected error occurred in the backend.",
            "data": None,
            "table": None,
            "error": str(e)
        }), 500


if __name__ == '__main__':
    app.run(debug=os.environ.get("FLASK_DEBUG") == "True", port=int(os.environ.get("FLASK_PORT", 5000)))
