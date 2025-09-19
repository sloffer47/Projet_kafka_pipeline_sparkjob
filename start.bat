@echo off
echo 🚀 Démarrage du pipeline data...
echo.
echo 📋 Nettoyage des containers existants...
docker-compose down -v
docker system prune -f

echo.
echo 🏗️  Build et démarrage des services...
docker-compose up --build -d

echo.
echo ⏳ Attente du démarrage complet (30s)...
timeout /t 30 /nobreak

echo.
echo 📊 Vérification des services...
docker-compose ps

echo.
echo ✅ Pipeline démarré ! 
echo 🌐 Airflow: http://localhost:8080 (admin/admin)
echo 🌐 Spark UI: http://localhost:8081
echo 📋 Logs Kafka: docker logs kafka
echo 📋 Logs Producer: docker logs producer
echo 📋 Logs Consumer: docker logs consumer
pause
