import os
import time
import json
import psycopg2
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# -----------------------
# Fonction de connexion DB
# -----------------------
def get_db_connection():
    for i in range(5):
        try:
            conn = psycopg2.connect(
                host=os.environ.get('POSTGRES_HOST','host.docker.internal'),
                dbname=os.environ.get('POSTGRES_DB','orders_db'),
                user=os.environ.get('POSTGRES_USER','postgres'),
                password=os.environ.get('POSTGRES_PASSWORD','root')
            )
            return conn
        except Exception as e:
            print(f"❌ Tentative connexion DB {i+1}/5 échouée: {e}")
            time.sleep(5)
    raise Exception("Impossible de se connecter à la DB")

# -----------------------
# Fonction d'écriture
# -----------------------
def write_to_postgres(df, epoch_id):
    pdf = df.toPandas()
    if pdf.empty:
        print("⚠️ Aucune donnée à insérer pour ce batch.")
        return

    conn = get_db_connection()
    cur = conn.cursor()
    # Création de la table si elle n'existe pas
    cur.execute("""
    CREATE TABLE IF NOT EXISTS orders_processed (
        order_id TEXT,
        product_id INT,
        amount INT,
        created_at TEXT
    )
    """)
    conn.commit()

    # Insertion des lignes
    for _, row in pdf.iterrows():
        cur.execute(
            "INSERT INTO orders_processed (order_id, product_id, amount, created_at) VALUES (%s, %s, %s, %s)",
            (row['order_id'], row['product_id'], row['amount'], row['created_at'])
        )
    conn.commit()
    cur.close()
    conn.close()
    print(f"✅ {len(pdf)} lignes insérées dans Postgres pour le batch {epoch_id}")

# -----------------------
# Spark session
# -----------------------
spark = SparkSession.builder \
    .appName("KafkaToPostgres") \
    .getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# -----------------------
# Lecture du stream Kafka
# -----------------------
kafka_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

# Schéma des données
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("created_at", StringType(), True)
])

orders_df = kafka_df.selectExpr("CAST(value AS STRING) as order_json") \
    .select(from_json(col("order_json"), orders_schema).alias("data")) \
    .select("data.*")

# -----------------------
# Écriture dans Postgres
# -----------------------
query = orders_df.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

print("🚀 Stream Spark démarré - Traitement et insertion dans PostgreSQL...")
query.awaitTermination()

# from pyspark.sql import SparkSession
# from pyspark.sql.functions import from_json, col
# from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# # Créer la session Spark
# spark = SparkSession.builder \
#     .appName("KafkaSparkJob") \
#     .getOrCreate()

# # Configuration pour réduire les logs
# spark.sparkContext.setLogLevel("WARN")

# print("🚀 Job Spark démarré - Connexion à Kafka...")

# # Lire le stream Kafka
# kafka_df = spark \
#     .readStream \
#     .format("kafka") \
#     .option("kafka.bootstrap.servers", "kafka:9092") \
#     .option("subscribe", "orders") \
#     .option("startingOffsets", "earliest") \
#     .load()

# # Schéma des données orders
# orders_schema = StructType([
#     StructField("order_id", StringType(), True),
#     StructField("product_id", IntegerType(), True),
#     StructField("amount", IntegerType(), True),
#     StructField("created_at", StringType(), True)
# ])

# # Transformer les données
# orders_df = kafka_df.selectExpr("CAST(value AS STRING) as order_json") \
#     .select(from_json(col("order_json"), orders_schema).alias("data")) \
#     .select("data.*")

# # Affichage des résultats
# query = orders_df.writeStream \
#     .outputMode("append") \
#     .format("console") \
#     .option("truncate", False) \
#     .trigger(processingTime='10 seconds') \
#     .start()

# print("✅ Stream Spark démarré - Traitement des commandes Kafka...")

# # Attendre la fin du streaming
# query.awaitTermination()

















# ça marche mais avec quelques errue 
# # from pyspark.sql import SparkSession
# from pyspark.sql.functions import col, upper
# import psycopg2
# import json

# # ========================
# # Spark session
# # ========================
# spark = SparkSession.builder \
#     .appName("KafkaToPostgres") \
#     .master("spark://spark:7077") \
#     .getOrCreate()

# # ========================
# # Lire depuis Kafka
# # ========================
# # Dans jobspark.py, change l'adresse Kafka
# df = spark \
#   .readStream \
#   .format("kafka") \
#   .option("kafka.bootstrap.servers", "kafka:9092") \
#   .option("subscribe", "orders") \
#   .option("startingOffsets", "earliest") \
#   .load()
# # Kafka renvoie des bytes, on convertit en string
# products_df = kafka_df.selectExpr("CAST(value AS STRING) as product_json")

# # Transformer JSON en colonnes
# def parse_json(row):
#     try:
#         return json.loads(row['product_json'])
#     except:
#         return None

# rdd = products_df.rdd.map(parse_json).filter(lambda x: x is not None)
# parsed_df = rdd.toDF()

# # Exemple de transformation : mettre le nom du produit en majuscules
# if 'name' in parsed_df.columns:
#     parsed_df = parsed_df.withColumn("name_upper", upper(col("name")))

# # ========================
# # Écriture dans Postgres
# # ========================
# def write_to_postgres(df):
#     # Convertir en pandas pour écrire via psycopg2
#     pdf = df.toPandas()
#     if pdf.empty:
#         print("Aucune donnée à insérer")
#         return

#     conn = psycopg2.connect(
#         host="postgres",
#         dbname="project",
#         user="postgres",
#         password="root",
#         port=5432
#     )
#     cur = conn.cursor()
    
#     # Création de la table si elle n'existe pas
#     cur.execute("""
#     CREATE TABLE IF NOT EXISTS products_processed (
#         id SERIAL PRIMARY KEY,
#         name TEXT,
#         name_upper TEXT
#     )
#     """)
#     conn.commit()

#     # Insérer les données
#     for _, row in pdf.iterrows():
#         cur.execute(
#             "INSERT INTO products_processed (name, name_upper) VALUES (%s, %s)",
#             (row.get('name'), row.get('name_upper'))
#         )
#     conn.commit()
#     cur.close()
#     conn.close()
#     print(f"{len(pdf)} lignes insérées dans Postgres.")

# write_to_postgres(parsed_df)

# spark.stop()
