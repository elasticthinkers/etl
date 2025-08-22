from flask import Flask, request, jsonify
import subprocess
import tempfile
import os

app = Flask(__name__)

@app.route('/generate-glue', methods=['POST'])
def generate_glue():
    data = request.get_json()
    glue_prompt = data.get('prompt')
    if not user_request:
        return jsonify({'error': 'Missing prompt'}), 400

    # Compose the Q CLI command
    q_command = [
        'aws-q', 'generate',
        f'Generate a Glue ETL job in Python for this request: {glue_prompt}'
    ]

    try:
        # Run the Q CLI and capture output
        result = subprocess.run(q_command, capture_output=True, text=True, check=True)
        glue_code = result.stdout
        return jsonify({'glue_code': glue_code})
    except subprocess.CalledProcessError as e:
        return jsonify({'error': 'Q CLI failed', 'details': e.stderr}), 500

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000)
