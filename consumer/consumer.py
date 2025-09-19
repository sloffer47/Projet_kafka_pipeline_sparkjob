import json
from kafka import KafkaConsumer
import psycopg2
import os
import time

# Attendre que les services soient prêts - AUGMENTÉ À 45 secondes
time.sleep(45)

# Fonction pour attendre Kafka avec retry
def wait_for_kafka():
    for i in range(10):
        try:
            print(f"🔄 Tentative connexion Kafka {i+1}/10...")
            # Test de connexion simple
            consumer_test = KafkaConsumer(
                bootstrap_servers=['kafka:9092'],
                consumer_timeout_ms=5000
            )
            consumer_test.close()
            print("✅ Kafka accessible !")
            return True
        except Exception as e:
            print(f"❌ Kafka non accessible: {e}")
            time.sleep(10)
    return False

# Attendre Kafka
if not wait_for_kafka():
    print("❌ Impossible de se connecter à Kafka après 10 tentatives")
    exit(1)

# Kafka consumer
consumer = KafkaConsumer(
    'orders',
    bootstrap_servers=['kafka:9092'],
    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
    auto_offset_reset='latest'
)

# Connexion Postgres avec retry
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

conn = get_db_connection()
cur = conn.cursor()

print("✅ Consumer démarré, attente des messages...")

# Boucle pour consommer les messages
for msg in consumer:
    try:
        order = msg.value
        cur.execute('SELECT name FROM products WHERE id = %s', (order['product_id'],))
        r = cur.fetchone()
        product_name = r[0] if r else 'unknown'

        cur.execute(
            '''
            INSERT INTO orders(order_id, product_id, product_name, amount, created_at)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (order_id) DO NOTHING
            ''',
            (order['order_id'], order['product_id'], product_name, order['amount'], order['created_at'])
        )
        conn.commit()
        print("✅ Order inséré :", order['order_id'])
    except Exception as e:
        print(f"❌ Erreur consumer: {e}")