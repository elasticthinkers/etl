import os, json, boto3, datetime


dynamodb = boto3.resource("dynamodb", region_name="us-east-1")
TABLE = "ETLLogs"
table = dynamodb.Table(TABLE)


def lambda_handler(event, context):
    try:
        body = event.get("body") if isinstance(event, dict) else event
        if isinstance(body, str):
            body = json.loads(body)
        if not isinstance(body, dict):
            body = event

        session_id = body.get("session_id")
        log_type = body.get("type")
        message = body.get("message")
        details = body.get("details", {})
        if not session_id or not log_type or not message:
            return {"statusCode": 400, "body": json.dumps({"error": "Missing session_id/type/message"})}

        timestamp = datetime.datetime.utcnow().isoformat()
        new_log = {
            "timestamp": timestamp,
            "type": log_type,
            "message": message,
            "details": details,
        }
        # Upsert & append
        table.update_item(
            Key={"session_id": session_id},
            UpdateExpression="SET logarray = list_append(if_not_exists(logarray, :empty_list), :newlogs)",
            ExpressionAttributeValues={":newlogs": [new_log], ":empty_list": []},
        )
        return {"statusCode": 200, "body": json.dumps({"status": "appended", "log": new_log})}
    except Exception as e:
        return {"statusCode": 500, "body": json.dumps({"error": str(e)})}