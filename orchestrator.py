import os
import json
import uuid
import time
import datetime
import boto3
import requests
import re
from botocore.exceptions import ClientError

FLASK_GLUE_SERVER_URL = "http://localhost:5000/generate-glue"  # Update with actual Flask server URL
s3 = boto3.client("s3", region_name="us-east-1")
glue = boto3.client("glue", region_name="us-east-1")
lambda_client = boto3.client("lambda", region_name="us-east-1")

ETL_BUCKET = "autoetlbkt"
NOTIFY_LAMBDA = "notify"
LOG_LAMBDA = "log"

GLUE_ROLE_ARN = "arn:aws:iam::564311673745:role/AutoETLGlueRole"
GLUE_JOB_PY_DEPS = "s3://autoetlbkt/scripts/"
GLUE_TEMP_BUCKET = ETL_BUCKET

RDS_HOST = "db.cwnm682kmind.us-east-1.rds.amazonaws.com"
RDS_PORT = "5432"
RDS_DB = "autoetldb"
RDS_USER = "santanu"
RDS_PASSWORD = "autoetl123"

FLASK_TEST_URL = "http://18.212.181.157"

SPECS_FEWSHOT_PROMPT = r"""
You are an ETL planner. Convert the user's ETL request into a strict JSON specification.
Return ONLY JSON, no commentary. Use the schema:
{
  "source_tables": ["<table>"...],
  "join_type": "inner|left|right|full_outer|cross|none",
  "primary_key": "<pk_column>",
  "joins": [ {"left": "table.column", "right": "table.column", "type": "<join_type>"} ],
  "transformations": [
    {"select": {"<table>": ["col", ...], "<table>": ["col", ...]}},
    {"operation": "lowercase|uppercase|cast|concat|sum|average|ceil|floor|round",
     "column": "table.column",
     "datatype": "integer|decimal|float|text|...",
     "new_column": "<name>",
     "columns": ["table.column", ...],
     "precision": 2,
     "factor": 0.9
    }
  ],
  "target_table": "<table>",
  "notes": "optional clarifications"
}

Example 1 - Input:
"Ingest customers and orders. Inner join on customer_id. Select customer_name and order_amount. Add total_spent=sum(order_amount). Load to customer_summary."
Output:
{
  "source_tables": ["customers", "orders"],
  "join_type": "inner",
  "primary_key": "customer_id",
  "joins": [{"left":"customers.customer_id","right":"orders.customer_id","type":"inner"}],
  "transformations": [
    {"select": {"customers": ["customer_name"], "orders": ["order_amount"]}},
    {"operation": "sum", "new_column":"total_spent", "columns":["orders.order_amount"]}
  ],
  "target_table": "customer_summary"
}

Example 2 - Input:
"Ingest product_table, sales_table. Left join on product_id. Select product_name, sales_qty, add discount_price = price*0.9. Load to product_sales."
Output:
{
  "source_tables": ["product_table", "sales_table"],
  "join_type": "left",
  "primary_key": "product_id",
  "joins": [{"left":"product_table.product_id","right":"sales_table.product_id","type":"left"}],
  "transformations": [
    {"select": {"product_table": ["product_name"], "sales_table": ["sales_qty"]}},
    {"operation": "multiply", "new_column": "discount_price", "columns": ["product_table.price"], "factor": 0.9}
  ],
  "target_table": "product_sales"
}

Now convert this user request into JSON (STRICT JSON ONLY):
{user_request}
"""


GLUE_GEN_PROMPT = r"""
You are an expert AWS Glue PySpark engineer. Generate a COMPLETE `glue_job.py` script given the ETL `specs.json` content below.

Specs JSON:
{specs_json}

Use these RDS connection details for both source and target:
host: db.cwnm682kmind.us-east-1.rds.amazonaws.com
port: 5432
dbname: autoetldb
user: santanu
password: autoetl123

Required imports and code structure:
```python
# Add logging configuration
import logging
logger = logging.getLogger()
logger.setLevel(logging.INFO)
import sys
import json
import boto3
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql import SparkSession
from pyspark.sql.functions import upper, col, lit, expr

def main():
    # Initialize contexts
    sc = SparkContext()
    glueContext = GlueContext(sc)
    spark = glueContext.spark_session
    job = Job(glueContext)

    # Get job parameters
    args = getResolvedOptions(sys.argv, [
        'JOB_NAME', 'specs_s3_uri',
        'rds_host', 'rds_port', 'rds_db', 'rds_user', 'rds_password'
    ])
    job.init(args['JOB_NAME'], args)

    try:
        # Read specs from S3 using boto3
        logger.info(f"Reading specs from {args['specs_s3_uri']}")
        specs_s3_uri = args['specs_s3_uri']
        s3_parts = specs_s3_uri.replace("s3://", "").split("/", 1)
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=s3_parts[0], Key=s3_parts[1])
        specs_content = response['Body'].read().decode('utf-8')
        logger.info(f"Specs content: {specs_content}")
        specs = json.loads(specs_content)
        logger.info(f"Parsed specs: {json.dumps(specs, indent=2)}")
        
        # Build JDBC URLs
        jdbc_url = f"jdbc:postgresql://{args['rds_host']}:{args['rds_port']}/{args['rds_db']}"
        jdbc_options = {
            "url": jdbc_url,
            "user": args['rds_user'],
            "password": args['rds_password'],
            "driver": "org.postgresql.Driver"
        }
        
        # Load source tables
        source_dfs = {}
        for table in specs['source_tables']:
            logger.info(f"Reading source table: {table}")
            df = spark.read \
                .format("jdbc") \
                .options(**jdbc_options) \
                .option("dbtable", table) \
                .load()
            logger.info(f"Read {df.count()} rows from {table}")
            source_dfs[table] = df
            
        # Your ETL logic here...
        
    except Exception as e:
        logger.error(f"Error in Glue job: {str(e)}", exc_info=True)
        raise

if __name__ == "__main__":
    main()
```

Constraints:
- Python (PySpark) Glue 4.0 script using `GlueContext` and `SparkSession`
- Read from Amazon RDS (PostgreSQL) via JDBC for all source tables
- Apply joins and transformations according to `specs`
- IMPORTANT: After joining tables, reference columns without table prefixes (e.g., 'product_name' not 'source_products.product_name')
- Write target table back to RDS via JDBC (Create if new/Overwrite if exists)
- Write stepwise debug counts to logs (print statements)
- Use `.option("driver", "org.postgresql.Driver")` for JDBC
- Do NOT include placeholders like <...>. Output only executable Python code
- Follow the exact code structure provided above for imports and specs reading
"""


# ---------------- HELPERS ------------------
def call_bedrock_messages(prompt: str, max_tokens: int = 10000, temperature: float = 0.1) -> str:
    print("[DEBUG] call_bedrock_messages called")
    print("[DEBUG] Prompt sent to Bedrock (truncated):", prompt[:500])
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
        print("[DEBUG] Bedrock response received")
    except (ClientError, Exception) as e:
        print("[ERROR] Bedrock invoke failed:", e)
        raise RuntimeError(f"Bedrock invoke failed: {e}")

    body = json.loads(resp["body"].read())
    print("[DEBUG] Bedrock raw body keys:", body.keys())
    return body["content"][0]["text"]

def call_flask_glue_server(user_request: str) -> str:
    print("[DEBUG] call_flask_glue_server called")
    payload = {"prompt": user_request}
    try:
        response = requests.post(FLASK_GLUE_SERVER_URL, json=payload, timeout=60)
        response.raise_for_status()
        glue_code = response.json().get("glue_code")
        if not glue_code:
            raise ValueError("No glue_code in Flask response")
        print("[DEBUG] Glue code received from Flask server, length:", len(glue_code))
        return glue_code
    except Exception as e:
        print("[ERROR] Flask server call failed:", e)
        raise RuntimeError(f"Flask server call failed: {e}")


def extract_code_block(text: str) -> str:
    print("[DEBUG] extract_code_block called")
    matches = re.findall(r'```python\n(.*?)```', text, re.DOTALL)
    if matches:
        print("[DEBUG] Found python code block")
        return matches[0].strip()
    matches = re.findall(r'```(.*?)```', text, re.DOTALL)
    if matches:
        print("[DEBUG] Found generic code block")
        return matches[0].strip()
    print("[DEBUG] No code block found, returning raw text")
    return text.strip()

def _log(session_id: str, log_type: str, message: str, details: dict = None):
    print(f"[DEBUG] _log called: type={log_type}, message={message}, details={details}")
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


# ---------------- LAMBDA HANDLER ------------------
def lambda_handler(event, context):
    print("[DEBUG] lambda_handler invoked")
    print("[DEBUG] Incoming event:", json.dumps(event)[:1000])
    session_id=event.get("session_id")
    user_prompt = event.get("prompt") if isinstance(event, dict) else None
    if not user_prompt:
        body = event.get("body") if isinstance(event, dict) else None
        if body and isinstance(body, str):
            try:
                user_prompt = json.loads(body).get("prompt")
            except Exception as e:
                print("[ERROR] Failed to parse event body:", e)
    if not user_prompt:
        print("[ERROR] Missing user_prompt in request")
        return {"statusCode": 400, "body": json.dumps({"error": "Missing prompt"})}

    specs_key = f"specs/{session_id}.json"
    glue_key = f"glue/{session_id}_glue_job.py"

    _log(session_id, "Start", "Orchestration started")
    _log(session_id, "Info", "Received prompt", {"prompt": user_prompt})

    # 1) Generate specs.json via Bedrock
    print("[DEBUG] Generating specs.json via Bedrock")
    prompt_specs = SPECS_FEWSHOT_PROMPT.replace("{user_request}", user_prompt)
    specs_text = call_bedrock_messages(prompt_specs, max_tokens=4000, temperature=0.1)
    print("[DEBUG] Raw specs_text received:", specs_text[:500])

    try:
        specs = json.loads(specs_text)
        print("[DEBUG] specs JSON parsed successfully")
    except Exception:
        print("[WARN] Specs parsing failed, trying extract_code_block")
        try:
            specs = json.loads(extract_code_block(specs_text))
            print("[DEBUG] specs JSON parsed successfully after cleanup")
        except Exception as e:
            print("[ERROR] Failed to parse specs JSON:", e)
            _log(session_id, "End", "Failed to parse specs JSON", {"error": str(e), "raw": specs_text[:2000]})
            return {"statusCode": 500, "body": json.dumps({"error": "Spec generation failed"})}

    s3.put_object(Bucket=ETL_BUCKET, Key=specs_key, Body=json.dumps(specs, indent=2).encode("utf-8"))
    print(f"[DEBUG] specs.json uploaded to s3://{ETL_BUCKET}/{specs_key}")
    _log(session_id, "Result", "specs.json generated & uploaded", {"s3_uri": f"s3://{ETL_BUCKET}/{specs_key}", "specs": specs})



    # 1) Generate Glue code via Flask server (Amazon Q CLI)
    print("[DEBUG] Generating Glue script via Flask server (Amazon Q CLI)")
    glue_code = call_flask_glue_server(GLUE_GEN_PROMPT)
    print("[DEBUG] Glue code generated, length:", len(glue_code))

    # Optionally, you can parse specs from the code if needed, or skip specs.json generation
    s3.put_object(Bucket=ETL_BUCKET, Key=glue_key, Body=glue_code.encode("utf-8"))
    print(f"[DEBUG] glue_job.py uploaded to s3://{ETL_BUCKET}/{glue_key}")
    _log(session_id, "Code", "glue_job.py generated & uploaded", {"s3_uri": f"s3://{ETL_BUCKET}/{glue_key}", "code": glue_code[:5000]})

    # 3) Create (or update) Glue Job and execute
    job_name = f"autoetl_{session_id}"
    script_location = f"s3://{ETL_BUCKET}/{glue_key}"
    print("[DEBUG] Creating/Updating Glue job:", job_name)

    try:
        glue.create_job(
            Name=job_name,
            Role=GLUE_ROLE_ARN,
            Command={"Name": "glueetl", "ScriptLocation": script_location, "PythonVersion": "3"},
            GlueVersion="4.0",
            DefaultArguments={
                "--job-language": "python",
                "--TempDir": f"s3://{GLUE_TEMP_BUCKET}/tmp/",
            },
        )
        print("[DEBUG] Glue job created successfully")
    except glue.exceptions.AlreadyExistsException:
        print("[DEBUG] Glue job already exists, updating")
        glue.update_job(Name=job_name, JobUpdate={
            "Role": GLUE_ROLE_ARN,
            "Command": {"Name": "glueetl", "ScriptLocation": script_location, "PythonVersion": "3"},
            "GlueVersion": "4.0",
        })

    _log(session_id, "Start", "Glue job submitted", {"job_name": job_name})

    run = glue.start_job_run(
        JobName=job_name,
        Arguments={
            "--specs_s3_uri": f"s3://{ETL_BUCKET}/{specs_key}",
            "--rds_host": RDS_HOST,
            "--rds_port": RDS_PORT,
            "--rds_db": RDS_DB,
            "--rds_user": RDS_USER,
            "--rds_password": RDS_PASSWORD,
        },
    )
    run_id = run["JobRunId"]
    print("[DEBUG] Glue job started with run_id:", run_id)

    status = "RUNNING"
    while status in ("RUNNING", "STARTING", "STOPPING", "WAITING"):
        time.sleep(15)
        jr = glue.get_job_run(JobName=job_name, RunId=run_id)
        status = jr["JobRun"]["JobRunState"]
        print("[DEBUG] Glue run status:", status)
        _log(session_id, "Info", "Glue run status", {"status": status})

    _log(session_id, "Result", "Glue job completed", {"status": status, "run_id": run_id})

    if status == "SUCCEEDED" and FLASK_TEST_URL:
        print("[DEBUG] Glue succeeded, calling Flask validation")
        # Validation Logic

    # 5) Summarize & notify
    print("[DEBUG] Triggering notify Lambda")
    _log(session_id, "Start", "Summarization & email")
    lambda_client.invoke(
        FunctionName=NOTIFY_LAMBDA,
        InvocationType="Event",
        Payload=json.dumps({"session_id": session_id}).encode("utf-8"),
    )

    _log(session_id, "End", "Orchestration finished")
    print("[DEBUG] lambda_handler completed for session:", session_id)

    return {
        "statusCode": 200,
        "body": json.dumps({
            "session_id": session_id,
            "specs_s3": f"s3://{ETL_BUCKET}/{specs_key}",
            "glue_script_s3": f"s3://{ETL_BUCKET}/{glue_key}",
            "glue_status": status,
        }),
    }
