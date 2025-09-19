from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Créer la session Spark
spark = SparkSession.builder \
    .appName("KafkaSparkJob") \
    .getOrCreate()

# Configuration pour réduire les logs
spark.sparkContext.setLogLevel("WARN")

print("🚀 Job Spark démarré - Connexion à Kafka...")

# Lire le stream Kafka
kafka_df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka:9092") \
    .option("subscribe", "orders") \
    .option("startingOffsets", "earliest") \
    .load()

# Schéma des données orders
orders_schema = StructType([
    StructField("order_id", StringType(), True),
    StructField("product_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("created_at", StringType(), True)
])

# Transformer les données
orders_df = kafka_df.selectExpr("CAST(value AS STRING) as order_json") \
    .select(from_json(col("order_json"), orders_schema).alias("data")) \
    .select("data.*")

# Affichage des résultats
query = orders_df.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .trigger(processingTime='10 seconds') \
    .start()

print("✅ Stream Spark démarré - Traitement des commandes Kafka...")

# Attendre la fin du streaming
query.awaitTermination()

















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
