import os, json, boto3, traceback
from shared.bedrock import call_bedrock_messages


dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
ses = boto3.client("ses", region_name="us-east-1")

LOG_TABLE = "ETLLogs"
FROM_EMAIL = "thinkerselastic@gmail.com"
TO_EMAIL = "itsantanu24@gmail.com"

table = dynamodb.Table(LOG_TABLE)

SUMMARY_PROMPT_TMPL = """
You are an assistant that summarizes ETL pipeline execution logs for stakeholders.
Produce a clear summary covering: user intent, generated code highlights, steps and results,
errors and fixes applied, and final outcome. Provide a short bullet list and a short paragraph conclusion.

Logs (JSON array):
{logs_json}
"""


def summarize_with_bedrock(logs):
    prompt = SUMMARY_PROMPT_TMPL.replace("{logs_json}", json.dumps(logs, indent=2))
    return call_bedrock_messages(prompt, max_tokens=2000, temperature=0.2)


def lambda_handler(event, context):
    try:
        body = event.get("body") if isinstance(event, dict) else event
        if isinstance(body, str):
            body = json.loads(body)
        if not isinstance(body, dict):
            body = event

        session_id = body.get("session_id")
        if not session_id:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing session_id"})}

        resp = table.get_item(Key={"session_id": session_id})
        item = resp.get("Item")
        if not item:
            return {"statusCode": 404, "body": json.dumps({"error": "session not found"})}

        logs = item.get("logarray", [])
        summary = summarize_with_bedrock(logs)

        subject = f"AutoETL Pipeline Summary - {session_id}"
        body_text = summary
        if FROM_EMAIL and TO_EMAIL:
            ses.send_email(
                Source=FROM_EMAIL,
                Destination={"ToAddresses": [TO_EMAIL]},
                Message={
                    "Subject": {"Data": subject},
                    "Body": {"Text": {"Data": body_text}},
                },
            )
        return {"statusCode": 200, "body": json.dumps({"status": "emailed", "summary": summary})}
    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e), "trace": traceback.format_exc()}),
        }