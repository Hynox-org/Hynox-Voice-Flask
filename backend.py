# backend.py
from flask import Flask, request, jsonify
from flask_cors import CORS
from integrate import process_data  # ✅ Import your processing function

app = Flask(__name__)
CORS(app, origins=["http://localhost:3000"])

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
        # Process data safely
        result = process_data(chat_context, file_url)
        return jsonify({"response": result})
    except Exception as e:
        print("Error in backend:", e)
        return jsonify({"error": str(e)}), 500


if __name__ == '__main__':
    app.run(debug=True, port=5000)
