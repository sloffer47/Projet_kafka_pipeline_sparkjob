import json
import time
import uuid
import random
from kafka import KafkaProducer
from datetime import datetime

# Attendre que Kafka soit prêt
time.sleep(10)

# Kafka producer configuré
producer = KafkaProducer(
    bootstrap_servers=['kafka:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    retries=5,
    retry_backoff_ms=1000
)

products = [1, 2, 3]

print("✅ Producer démarré, envoi des messages...")

while True:
    try:
        order = {
            'order_id': str(uuid.uuid4()),
            'product_id': random.choice(products),
            'amount': random.randint(1, 5),
            'created_at': datetime.now().isoformat()
        }
        producer.send('orders', order)
        print("➡️  Order envoyé :", order)
        time.sleep(2)
    except Exception as e:
        print(f"❌ Erreur producer: {e}")
        time.sleep(5)
