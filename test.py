from kafka import KafkaProducer, KafkaConsumer
import json
import threading
import time

TOPIC = 'orders'

# 👉 mets ici "localhost:9092" si tu lances test.py sur Windows
# 👉 mets ici "kafka:9092" si tu lances test.py depuis un conteneur Docker
BOOTSTRAP_SERVERS = "localhost:9092"

# ---- Producteur ----
def produce():
    producer = KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode('utf-8')
    )
    messages = [
        {"id": 1, "item": "T-shirt"},
        {"id": 2, "item": "Sneakers"},
        {"id": 3, "item": "Mug"}
    ]
    for msg in messages:
        print(f"📤 Envoi de : {msg}")
        producer.send(TOPIC, msg)
        time.sleep(1)
    producer.flush()
    print("✅ Producteur terminé.")

# ---- Consommateur ----
def consume():
    consumer = KafkaConsumer(
        TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        auto_offset_reset='earliest',
        group_id='my-group',
        value_deserializer=lambda m: json.loads(m.decode('utf-8'))
    )
    print("👂 Consommateur en attente des messages...")
    for i, message in enumerate(consumer):
        print(f"➡️ Message reçu : {message.value}")
        if i >= 2:
            break
    consumer.close()
    print("✅ Consommateur terminé.")

# ---- Threading ----
threading.Thread(target=consume).start()
time.sleep(2)
produce()
