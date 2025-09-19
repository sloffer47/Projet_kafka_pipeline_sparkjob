from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import subprocess
import os
import time
import json
from kafka import KafkaConsumer, KafkaProducer

# === Paramètres par défaut ===
default_args = {
    'owner': 'equipe-devops',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email': ['mbandouyorick@gmail.com'],  # ✅ Ton email
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

# === Fonctions Python ===

def verifier_sante_kafka(**context):
    """🔍 Vérification de la santé de Kafka"""
    try:
        print("🔍 Test de connexion Kafka...")

        producer = KafkaProducer(
            bootstrap_servers=['kafka:9092'],
            value_serializer=lambda v: json.dumps(v).encode('utf-8'),
            request_timeout_ms=10000
        )

        test_msg = {"test": "controle_sante", "timestamp": datetime.now().isoformat()}
        producer.send('topic-sante', test_msg)
        producer.flush()
        producer.close()
        print("✅ Kafka Producer opérationnel")

        consumer = KafkaConsumer(
            'topic-sante',
            bootstrap_servers=['kafka:9092'],
            consumer_timeout_ms=5000,
            auto_offset_reset='latest'
        )
        consumer.close()
        print("✅ Kafka Consumer opérationnel")

        result = subprocess.run([
            'docker', 'exec', 'kafka',
            'kafka-topics.sh', '--list', '--bootstrap-server', 'localhost:9092'
        ], capture_output=True, text=True)

        topics = result.stdout.strip().split('\n') if result.stdout else []
        print(f"📋 Topics disponibles: {topics}")

        return {'statut': 'sain', 'nombre_topics': len([t for t in topics if t.strip()]), 'topics': topics}

    except Exception as e:
        print(f"❌ Kafka en panne: {e}")
        return {'statut': 'en_panne', 'erreur': str(e)}


def verifier_sante_base(**context):
    """🗄️ Vérification de la santé de PostgreSQL"""
    try:
        print("🔍 Test de connexion PostgreSQL...")

        conn = psycopg2.connect(
            host="host.docker.internal",
            dbname="orders_db",
            user="postgres",
            password="root",
            connect_timeout=10
        )
        cur = conn.cursor()

        sante = {'connexion': True}

        cur.execute("SELECT COUNT(*) FROM orders")
        total_commandes = cur.fetchone()[0]
        sante['total_commandes'] = total_commandes

        cur.execute("SELECT pg_size_pretty(pg_database_size('orders_db'))")
        taille = cur.fetchone()[0]
        sante['taille_base'] = taille

        conn.close()
        print(f"✅ PostgreSQL opérationnel - {total_commandes} commandes, taille: {taille}")

        return {'statut': 'sain', 'details': sante}

    except Exception as e:
        print(f"❌ PostgreSQL en panne: {e}")
        return {'statut': 'en_panne', 'erreur': str(e)}


def verifier_conteneurs(**context):
    """🐳 Vérification du statut des conteneurs Docker"""
    try:
        print("🔍 Vérification des conteneurs Docker...")

        result = subprocess.run(
            ['docker', 'ps', '--format', 'table {{.Names}}\t{{.Status}}'],
            capture_output=True, text=True
        )
        conteneurs = {}
        lignes = result.stdout.strip().split('\n')[1:]
        for ligne in lignes:
            parties = ligne.split('\t')
            if len(parties) >= 2:
                nom, statut = parties[0], parties[1]
                conteneurs[nom] = {'statut': statut, 'en_marche': 'Up' in statut}

        conteneurs_en_panne = [n for n, i in conteneurs.items() if not i['en_marche']]
        print(f"📊 Conteneurs actifs: {len(conteneurs) - len(conteneurs_en_panne)}")
        if conteneurs_en_panne:
            print(f"❌ Conteneurs en panne: {conteneurs_en_panne}")

        return {'statut': 'sain' if not conteneurs_en_panne else 'en_panne', 'conteneurs': conteneurs}

    except Exception as e:
        print(f"❌ Erreur vérification conteneurs: {e}")
        return {'statut': 'erreur', 'erreur': str(e)}


def nettoyer_logs(**context):
    """🧹 Nettoyage des logs anciens"""
    try:
        print("🧹 Nettoyage des logs Airflow...")
        chemin_logs = "/opt/airflow/logs"
        nb_supprimes = 0
        if os.path.exists(chemin_logs):
            for root, dirs, files in os.walk(chemin_logs):
                for fichier in files:
                    chemin_fichier = os.path.join(root, fichier)
                    if os.path.getmtime(chemin_fichier) < (time.time() - 7 * 24 * 3600):
                        os.remove(chemin_fichier)
                        nb_supprimes += 1
        print(f"✅ {nb_supprimes} fichiers de log supprimés")
        return {'logs_supprimes': nb_supprimes}

    except Exception as e:
        print(f"❌ Erreur nettoyage logs: {e}")
        raise


def optimiser_base(**context):
    """⚡ Optimisation de la base PostgreSQL"""
    try:
        print("⚡ Optimisation de la base de données...")
        conn = psycopg2.connect(
            host="host.docker.internal",
            dbname="orders_db",
            user="postgres",
            password="root"
        )
        conn.autocommit = True  # ✅ Obligatoire pour VACUUM / REINDEX
        cur = conn.cursor()

        # 1. VACUUM des tables
        for table in ['orders', 'products']:
            print(f"🔧 VACUUM {table}...")
            cur.execute(f"VACUUM ANALYZE {table}")

        # 2. Réindexation des tables
        for table in ['orders', 'products']:
            print(f"📊 Réindexation {table}...")
            cur.execute(f"REINDEX TABLE {table}")

        # 3. Vérification de la taille de la DB
        cur.execute("SELECT pg_size_pretty(pg_database_size('orders_db'))")
        taille_apres = cur.fetchone()[0]

        conn.close()
        print(f"✅ Optimisation terminée - Taille DB: {taille_apres}")
        return {'optimisation': 'ok', 'taille_db': taille_apres}

    except Exception as e:
        print(f"❌ Erreur optimisation: {e}")
        raise


def generer_rapport(**context):
    """📊 Génération du rapport de santé"""
    print("📊 Génération du rapport global...")
    kafka = context['task_instance'].xcom_pull(task_ids='verifier_kafka')
    base = context['task_instance'].xcom_pull(task_ids='verifier_base')
    conteneurs = context['task_instance'].xcom_pull(task_ids='verifier_conteneurs')
    logs = context['task_instance'].xcom_pull(task_ids='nettoyer_logs')
    opti = context['task_instance'].xcom_pull(task_ids='optimiser_base')

    rapport = {
        'kafka': kafka,
        'base': base,
        'conteneurs': conteneurs,
        'nettoyage': logs,
        'optimisation': opti,
        'horodatage': datetime.now().isoformat()
    }

    os.makedirs('/opt/airflow/reports', exist_ok=True)
    with open('/opt/airflow/reports/rapport_sante.json', 'w') as f:
        json.dump(rapport, f, indent=2)

    print("✅ Rapport de santé généré")
    return rapport


def envoyer_notification(**context):
    """📧 Envoi de notification de maintenance"""
    rapport = context['task_instance'].xcom_pull(task_ids='generer_rapport')
    if not rapport:
        print("❌ Aucun rapport disponible")
        return
    print("📧 Notification envoyée (simulation)")
    return "Notification envoyée"

# === DAG ===
with DAG(
    dag_id="maintenance_pipeline",
    default_args=default_args,
    description='Maintenance et surveillance du pipeline de données',
    schedule_interval="0 2 * * *",
    catchup=False,
    tags=['maintenance', 'surveillance']
) as dag:

    t1 = PythonOperator(task_id="verifier_kafka", python_callable=verifier_sante_kafka)
    t2 = PythonOperator(task_id="verifier_base", python_callable=verifier_sante_base)
    t3 = PythonOperator(task_id="verifier_conteneurs", python_callable=verifier_conteneurs)
    t4 = PythonOperator(task_id="nettoyer_logs", python_callable=nettoyer_logs)
    t5 = PythonOperator(task_id="optimiser_base", python_callable=optimiser_base)
    t6 = PythonOperator(task_id="generer_rapport", python_callable=generer_rapport)
    t7 = PythonOperator(task_id="envoyer_notification", python_callable=envoyer_notification)

    [t1, t2, t3] >> t4 >> t5 >> t6 >> t7


# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from datetime import datetime
# from kafka import KafkaProducer, KafkaConsumer
# import psycopg2
# import json

# KAFKA_TOPIC = 'test-topic'
# KAFKA_SERVER = 'kafka:9092'  # docker-compose service name
# POSTGRES_CONN = {
#     'dbname': 'postgres',
#     'user': 'postgres',
#     'password': 'root',
#     'host': 'postgres',  # docker-compose service name
#     'port': 5432
# }

# default_args = {
#     'owner': 'airflow',
#     'start_date': datetime(2025, 9, 16),
#     'retries': 1
# }

# def produce_messages():
#     producer = KafkaProducer(
#         bootstrap_servers=KAFKA_SERVER,
#         value_serializer=lambda v: json.dumps(v).encode('utf-8')
#     )
#     messages = [{"product": "T-shirt"}, {"product": "Sneakers"}, {"product": "Mug"}]
#     for msg in messages:
#         producer.send(KAFKA_TOPIC, msg)
#     producer.flush()
#     producer.close()
#     print("Messages produced to Kafka.")

# def consume_messages(**kwargs):
#     consumer = KafkaConsumer(
#         KAFKA_TOPIC,
#         bootstrap_servers=KAFKA_SERVER,
#         auto_offset_reset='earliest',
#         enable_auto_commit=True,
#         group_id='airflow-group',
#         value_deserializer=lambda v: json.loads(v.decode('utf-8'))
#     )
#     messages = [msg.value for msg in consumer]
#     consumer.close()
#     print(f"Consumed messages: {messages}")
#     # Pass messages to next task
#     kwargs['ti'].xcom_push(key='kafka_messages', value=messages)

# def store_in_postgres(**kwargs):
#     messages = kwargs['ti'].xcom_pull(key='kafka_messages', task_ids='consume_messages')
#     conn = psycopg2.connect(**POSTGRES_CONN)
#     cur = conn.cursor()
#     for msg in messages:
#         cur.execute("INSERT INTO products (name) VALUES (%s) ON CONFLICT DO NOTHING;", (msg['product'],))
#     conn.commit()
#     cur.close()
#     conn.close()
#     print("Messages stored in PostgreSQL.")

# with DAG('kafka_postgres_dag', default_args=default_args, schedule_interval=None, catchup=False) as dag:
#     t1 = PythonOperator(
#         task_id='produce_messages',
#         python_callable=produce_messages
#     )

#     t2 = PythonOperator(
#         task_id='consume_messages',
#         python_callable=consume_messages,
#         provide_context=True
#     )

#     t3 = PythonOperator(
#         task_id='store_in_postgres',
#         python_callable=store_in_postgres,
#         provide_context=True
#     )

#     t1 >> t2 >> t3
