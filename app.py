import os
import json
from flask import Flask, request, jsonify
import boto3
import psycopg2
import re
from flask_cors import CORS
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError

app = Flask(__name__)
CORS(app)

dynamodb = boto3.resource("dynamodb", region_name="us-east-1")

TABLE = "ETLLogs"
table = dynamodb.Table(TABLE)

MODEL_ID = "us.anthropic.claude-3-7-sonnet-20250219-v1:0"
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

RDS_HOST = "db.cwnm682kmind.us-east-1.rds.amazonaws.com"
RDS_PORT = int("5432")
RDS_DB = "autoetldb"
RDS_USER = "santanu"
RDS_PASSWORD = "autoetl123"

LOG_LAMBDA = "log"
s3 = boto3.client("s3", region_name="us-east-1")
lambda_client = boto3.client("lambda", region_name="us-east-1")

TEST_PROMPT_TMPL = r"""
You are a data QA engineer. Given the following AutoETL specs JSON, create 4-5 SQL test cases to validate the target table.
Rules:
- PostgreSQL dialect only.
- Prefer non-destructive SELECT-based checks (no DDL/DML).
- Include checks for row counts, NULLs on key columns, simple transformation validity (e.g., lower/upper), join cardinality sanity, and example value checks.
- Return STRICT JSON array of objects: [{"name":"...","sql":"SELECT ..."}]

Specs:
{specs_json}
"""

ALLOWED_SQL = re.compile(r"^\s*SELECT\b", re.IGNORECASE | re.DOTALL)

def call_bedrock_messages(prompt: str, max_tokens: int = 10000, temperature: float = 0.1) -> str:
    """
    Calls Anthropic Messages via Bedrock and returns the text content.
    The prompt should be a single user message (we keep system prompt minimal per call sites).
    """
    req = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": max_tokens,
        "temperature": temperature,
        "messages": [
            {"role": "user", "content": [{"type": "text", "text": prompt}]}
        ],
    }
    try:
        resp = bedrock.invoke_model(modelId=MODEL_ID, body=json.dumps(req))
    except (ClientError, Exception) as e:
        raise RuntimeError(f"Bedrock invoke failed: {e}")

    body = json.loads(resp["body"].read())
    return body["content"][0]["text"]

def _log(session_id: str, log_type: str, message: str, details: dict = None):
    payload = {
        "session_id": session_id,
        "type": log_type,
        "message": message,
        "details": details or {},
    }
    lambda_client.invoke(
        FunctionName=LOG_LAMBDA,
        InvocationType="Event",
        Payload=json.dumps(payload).encode("utf-8"),
    )


def _read_s3_json(uri: str):
    assert uri.startswith("s3://")
    _, _, bucket_key = uri.partition("s3://")
    bucket, _, key = bucket_key.partition("/")
    obj = s3.get_object(Bucket=bucket, Key=key)
    return json.loads(obj["Body"].read())


def _pg_conn():
    return psycopg2.connect(
        host=RDS_HOST, port=RDS_PORT, dbname=RDS_DB, user=RDS_USER, password=RDS_PASSWORD
    )


@app.route("/etl", methods=["POST"])
def trigger_etl():
    try:
        data = request.get_json(force=True)
        session_id = data.get("session_id")
        prompt = data.get("prompt")
        if not prompt:
            return jsonify({"error": "Missing prompt"}), 400
            
        # Call Lambda function
        lambda_response = lambda_client.invoke(
            FunctionName="orchestrator",
            InvocationType="RequestResponse",
            Payload=json.dumps({"prompt": prompt, "session_id": session_id})
        )
        
        # Parse response
        response_payload = json.loads(lambda_response["Payload"].read())
        return jsonify(response_payload)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route("/get/logs:<sessionid>", methods=["GET"])
def get_logs(sessionid):
    try:
        # Assuming session_id is the partition key
        response = table.query(
            KeyConditionExpression=Key("session_id").eq(sessionid)
        )
        items = response.get("Items", [])
        return jsonify({"session_id": sessionid, "logs": items})
    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080, debug=False)