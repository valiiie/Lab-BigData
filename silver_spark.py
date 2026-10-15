from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

BUCKET = "user-v-boury-ece"

# Initialisation directe avec les accès SeaweedFS Onyxia
spark = (
    SparkSession.builder
    .appName("lab-silver-pyspark")
    .master("local[*]")
    .config("spark.hadoop.fs.s3a.endpoint", "https://s3.seaweedfs.adm.adaltas.cloud")
    .config("spark.hadoop.fs.s3a.access.key", "ASIAd26605db3dc28ce2")
    .config("spark.hadoop.fs.s3a.secret.key", "KbpmtPiWywTe9/v/bmCGdlAWAoyNvS+HTwlROXaFBos=")
    .config("spark.hadoop.fs.s3a.session.token", """eyJhbGciOiJIUzI1NiIsInR5cCI6IkpXVCJ9.eyJpc3MiOiJzZWF3ZWVkZnMtc3RzIiwic3ViIjoiYWZmM2ExM2JkMGE1OWJhODk2MmJjOWU0NjU2YTU5MGMiLCJleHAiOjE3OTIxMTA1ODQsIm5iZiI6MTc5MjA2NzM4NCwiaWF0IjoxNzkyMDY3Mzg0LCJqdGkiOiJhZmYzYTEzYmQwYTU5YmE4OTYyYmM5ZTQ2NTZhNTkwYyIsInNpZCI6ImFmZjNhMTNiZDBhNTliYTg5NjJiYzllNDY1NmE1OTBjIiwic25hbSI6Im9ueXhpYS12LWJvdXJ5LWVjZSIsInR5cCI6InNlc3Npb24iLCJyb2xlIjoiYXJuOmF3czppYW06OnJvbGUvUzNVc2VyUm9sZSIsImFzc3VtZWQiOiJhcm46YXdzOnN0czo6YXNzdW1lZC1yb2xlL1MzVXNlclJvbGUvb255eGlhLXYtYm91cnktZWNlIiwicHJpbmNpcGFsIjoiYXJuOmF3czpzdHM6OmFzc3VtZWQtcm9sZS9TM1VzZXJSb2xlL29ueXhpYS12LWJvdXJ5LWVjZSIsImlkcCI6ImtleWNsb2FrIiwiZXh0X3VpZCI6ImQ1MjE3OWVhLWY0NzgtNGYzZS05YjcxLTdmZGViZGU1MGU3OCIsInByb3ZfaXNzIjoiaHR0cHM6Ly9rZXljbG9hay5hZG0uYWRhbHRhcy5jbG91ZC9hdXRoL3JlYWxtcy9hZGFsdGFzIiwicmVxX2N0eCI6eyJhY3IiOiIwIiwiYWxsb3dlZC1vcmlnaW5zIjoiW1wiaHR0cHM6Ly9zMy5zZWF3ZWVkZnMuYWRtLmFkYWx0YXMuY2xvdWRcIixcImh0dHBzOi8vb255eGlhLmFkbS5hZGFsdGFzLmNsb3VkXCJdIiwiYXV0aF90aW1lIjoiMTc5MjA2NTc1MCIsImF3czp1c2VyaWQiOiJZUjF5M0J6U2JFdmtROE1VVUhuQkh5VlE4c3FOaGJOSUR6SHl5aWxWOUZ3IiwiYXpwIjoic2Vhd2VlZGZzIiwiY25mIjoie1wiamt0XCI6XCJMVDhfSHIxRlBLUE83RGVyV2ZweXNWU2V5ZXBsa2JLYko0RF9DQ0pTaGkwXCIsXCJrYy1qa3QtdHlwZVwiOlwiRFBvUFwifSIsImVtYWlsIjoidmFsZW50aW5lLmJvdXJ5QGVkdS5lY2UuZnIiLCJlbWFpbF92ZXJpZmllZCI6InRydWUiLCJnaXZlbl9uYW1lIjoidi1ib3VyeS1lY2UiLCJncm91cHMiOlsiL2FkbWluIiwiL2FyZ29jZF9hZG1pbiIsIi9hcmdvY2RfcmVhZG9ubHkiLCIvdXNlcnMiXSwibmFtZSI6InYtYm91cnktZWNlIiwicHJlZmVycmVkX3VzZXJuYW1lIjoidi1ib3VyeS1lY2UiLCJyZWFsbV9hY2Nlc3MiOiJ7XCJyb2xlc1wiOltcImRlZmF1bHQtcm9sZXMtYWRhbHRhc1wiLFwib2ZmbGluZV9hY2Nlc3NcIixcInVtYV9hdXRob3JpemF0aW9uXCJdfSIsInJlc291cmNlX2FjY2VzcyI6IntcImFjY291bnRcIjp7XCJyb2xlc1wiOltcIm1hbmFnZS1hY2NvdW50XCIsXCJtYW5hZ2UtYWNjb3VudC1saW5rc1wiLFwidmlldy1wcm9maWxlXCJdfX0iLCJyb2xlcyI6ImFybjphd3M6aWFtOjpyb2xlL1MzVXNlclJvbGUiLCJzY29wZSI6Im9wZW5pZCBwcm9maWxlIGdyb3VwcyBlbWFpbCIsInNpZCI6IjdhekNoYzNRd1dpclJ0cEpPeFl5bXJCMyIsInN1YiI6ImQ1MjE3OWVhLWY0NzgtNGYzZS05YjcxLTdmZGViZGU1MGU3OCIsInR5cCI6IkRQb1AifSwiYXNzdW1lZF9hdCI6IjIwMjYtMTAtMTVUMTI6Mjk6NDQuNjUyODg0MTY1WiIsIm1heF9kdXIiOjQzMjAwLCJwdWlkIjoiWVIxeTNCelNiRXZrUThNVVVIbkJIeVZROHNxTmhiTklEekh5eWlsVjlGdyJ9.DLxuLoHZn7-3vGQ84E_L362OcA5yF7a3qjk3bHAqyeo""")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "true")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
    .config("spark.sql.catalog.lab", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.lab.type", "hadoop")
    .config("spark.sql.catalog.lab.warehouse", f"s3a://{BUCKET}/warehouse")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("ERROR")

# Lecture Bronze
users_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/users.csv",
    header=True,
    inferSchema=True,
    samplingRatio=0.01,
    multiLine=True
)

orders_bronze = spark.read.csv(
    f"s3a://{BUCKET}/bronze/orders.csv",
    header=True,
    inferSchema=True,
    samplingRatio=0.01,
    multiLine=True
)

# Modèle stg_users
users_typed = (
    users_bronze
    .select(
        F.col("uuid").cast("string").alias("user_id"),
        F.trim("username").alias("username"),
        F.trim("name").alias("name"),
        F.upper(F.trim("sex")).alias("sex"),
        F.lower(F.trim("mail")).alias("email"),
        F.col("birthdate").cast("date").alias("birthdate"),
        F.split(F.col("address"), "\n").alias("address_lines"),
    )
    .withColumn("street", F.element_at("address_lines", 1))
    .withColumn("address_line_2", F.element_at("address_lines", 2))
    .drop("address_lines")
)

stg_users = users_typed.select(
    "user_id", "username", "name", "sex", "email", "birthdate", "street",
    F.regexp_extract("address_line_2", "^(.+), [A-Z]{2} [0-9]{5}$", 1).alias("city"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 1).alias("state"),
    F.regexp_extract("address_line_2", "([A-Z]{2}) ([0-9]{5})$", 2).alias("zip_code"),
).withColumn("city", F.when(F.col("city") == "", None).otherwise(F.col("city")))

# Modèle stg_orders avec déduplication
orders_typed = orders_bronze.select(
    F.col("uuid").cast("string").alias("order_id"),
    F.col("user_uuid").cast("string").alias("user_id"),
    F.col("date").cast("timestamp").alias("ordered_at"),
    F.col("quantity").cast("int").alias("quantity"),
    F.lower(F.trim("product")).alias("product"),
)

dedup_window = Window.partitionBy("order_id").orderBy(F.col("ordered_at").desc())

stg_orders = (
    orders_typed
    .withColumn("row_number", F.row_number().over(dedup_window))
    .filter(F.col("row_number") == 1)
    .drop("row_number")
)

print("\n=== APERÇU USERS SILVER ===")
stg_users.show(5, truncate=False)

print("\n=== COMPTAGE ORDERS DÉDUPLIQUÉES ===")
print("Total orders :", stg_orders.count())

# Écriture Parquet sur S3
print("\nÉcriture Parquet sur S3...")
stg_users.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/users.parquet")
stg_orders.write.mode("overwrite").parquet(f"s3a://{BUCKET}/silver/spark/orders.parquet")
print("Écriture Parquet terminée !")

# Écriture Iceberg sur S3
print("\nÉcriture tables Iceberg sur S3...")
spark.sql("CREATE NAMESPACE IF NOT EXISTS lab.silver")
stg_users.writeTo("lab.silver.users").createOrReplace()
stg_orders.writeTo("lab.silver.orders").createOrReplace()
print("Écriture Iceberg terminée !")