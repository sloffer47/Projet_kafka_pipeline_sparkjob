# 🚀 Projet Data Pipeline Temps Réel + Batch + Orchestration

## 🏗️ Architecture

- **Kafka** : Streaming temps réel des commandes
- **Producer** : Génère des commandes aléatoirement  
- **Consumer** : Consomme et stocke en base
- **PostgreSQL** : Base de données des commandes
- **Airflow** : Orchestration des tâches batch
- **Spark** : Traitement big data
- **Redis** : Cache et broker pour Airflow

## 🚀 Démarrage rapide

```bash
# Windows
start.bat

# Linux/Mac  
chmod +x start.sh && ./start.sh
```

## 🌐 Interfaces

- Airflow: http://localhost:8080 (admin/admin)
- Spark UI: http://localhost:8081

## 📋 Commandes utiles

```bash
# Voir les logs
docker logs producer
docker logs consumer  
docker logs kafka

# Redémarrer un service
docker-compose restart producer

# Arrêter tout
docker-compose down
```

## 🔧 Configuration PostgreSQL

Créez d'abord la base `orders_db` dans votre PostgreSQL local, puis lancez le pipeline.
