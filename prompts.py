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