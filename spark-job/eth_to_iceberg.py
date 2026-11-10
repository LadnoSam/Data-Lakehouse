from pyspark.sql import SparkSession
from pymongo import MongoClient
import os

mongo_client = MongoClient(os.environ["MONGO_URL"])
db = mongo_client["crypto"]
collection = db["ethereum_prices"]

docs = list(collection.find())
print(f"Fetched {len(docs)} documents from MongoDB")

rows = []
for d in docs:
    rows.append({
        "timestamp": d["timestamp"].isoformat(),
        "price_usd": float(d["price_usd"]),
        "change_24h": float(d.get("change_24h") or 0.0),
        "market_cap": float(d.get("market_cap") or 0.0),
        "volume_24h": float(d.get("volume_24h") or 0.0),
    })

spark = SparkSession.builder \
    .appName("eth-to-iceberg") \
    .config("spark.sql.catalog.polaris.client.region", "us-east-1") \
    .config("spark.sql.catalog.polaris.s3.access-key-id", os.environ["RUSTFS_ACCESS_KEY"]) \
    .config("spark.sql.catalog.polaris.s3.secret-access-key", os.environ["RUSTFS_SECRET_KEY"]) \
    .config("spark.sql.catalog.polaris.warehouse", os.environ["POLARIS_CATALOG_NAME"]) \
    .config("spark.sql.catalog.polaris.credential", f'{os.environ["POLARIS_OIDC_CLIENT_ID"]}:{os.environ["POLARIS_OIDC_CLIENT_SECRET"]}') \
    .config("spark.sql.catalog.polaris.oauth2-server-uri", f'http://keycloak:8080/realms/{os.environ["KEYCLOAK_REALM"]}/protocol/openid-connect/token') \
    .config("spark.hadoop.fs.s3a.access.key", os.environ["RUSTFS_ACCESS_KEY"]) \
    .config("spark.hadoop.fs.s3a.secret.key", os.environ["RUSTFS_SECRET_KEY"]) \
    .config("spark.sql.catalog.polaris.rest.sigv4-enabled", "false") \
    .config("spark.sql.catalog.polaris", "org.apache.iceberg.spark.SparkCatalog") \
    .config("spark.sql.catalog.polaris.type", "rest") \
    .config("spark.sql.catalog.polaris.uri", "http://polaris:8181/api/catalog") \
    .config("spark.sql.catalog.polaris.scope", "profile email") \
    .config("spark.sql.catalog.polaris.io-impl", "org.apache.iceberg.aws.s3.S3FileIO") \
    .config("spark.sql.catalog.polaris.s3.endpoint", "http://rustfs:9000") \
    .config("spark.sql.catalog.polaris.s3.path-style-access", "true") \
    .getOrCreate()

df = spark.createDataFrame(rows)
df.printSchema()
df.show(5)

spark.sql("CREATE NAMESPACE IF NOT EXISTS polaris.crypto")
df.writeTo("polaris.crypto.ethereum_prices").createOrReplace()

result = spark.sql("SELECT * FROM polaris.crypto.ethereum_prices LIMIT 5")
result.show()

spark.stop()
