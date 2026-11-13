"""Iceberg Java oracle: one Spark session on the Lakekeeper warehouses, behind HTTP.

POST /sql with a SQL statement as the body returns {"columns": [...], "rows": [[...]]},
every value as a string or null. Statements run one at a time.
"""

import json
import os
import signal
from http.server import BaseHTTPRequestHandler, HTTPServer

from pyspark.sql import SparkSession

CATALOG_URI = "http://localhost:8282/catalog"
S3_KEY_ID = "GK26280000000000000000000a"
S3_SECRET = "2628000000000000000000000000000000000000000000000000000000000000"


def catalog_conf(builder, name, warehouse):
    prefix = f"spark.sql.catalog.{name}"
    return (
        builder.config(prefix, "org.apache.iceberg.spark.SparkCatalog")
        .config(f"{prefix}.type", "rest")
        .config(f"{prefix}.uri", CATALOG_URI)
        .config(f"{prefix}.warehouse", warehouse)
        .config(f"{prefix}.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
        .config(f"{prefix}.s3.access-key-id", S3_KEY_ID)
        .config(f"{prefix}.s3.secret-access-key", S3_SECRET)
        .config(f"{prefix}.s3.path-style-access", "true")
        .config(f"{prefix}.client.region", "us-east-1")
        .config(f"{prefix}.cache-enabled", "false")
    )


builder = (
    SparkSession.builder.appName("advangrid-oracle")
    .master("local[4]")
    .config("spark.driver.memory", os.environ.get("ORACLE_DRIVER_MEMORY", "2g"))
    .config("spark.sql.session.timeZone", "UTC")
    .config("spark.ui.enabled", "false")
)
spark = catalog_conf(builder, "lake", "lake").getOrCreate()


def text(value):
    return None if value is None else str(value)


class Handler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.reply(200 if self.path == "/health" else 404, {})

    def do_POST(self):
        sql = self.rfile.read(int(self.headers["Content-Length"])).decode()
        try:
            df = spark.sql(sql)
            rows = [[text(v) for v in row] for row in df.collect()]
            self.reply(200, {"columns": df.columns, "rows": rows})
        except Exception as e:
            self.reply(500, {"error": str(e)})

    def reply(self, status, body):
        data = json.dumps(body).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args):
        pass


signal.signal(signal.SIGTERM, lambda *_: os._exit(0))
HTTPServer(("0.0.0.0", 8899), Handler).serve_forever()
