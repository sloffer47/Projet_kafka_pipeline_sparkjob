pipeline {
    agent any

    environment {
        EC2_HOST = '13.61.100.227'        // serveur où Docker est installé
        EC2_USER = 'ubuntu'
        APP_NAME = 'Projet_jobspark'
        GITHUB_REPO = 'https://github.com/sloffer47/Projet_kafka_pipeline_sparkjob.git'
        SSH_CREDENTIALS = 'ec2-ssh-key'
        DOCKER_IMAGE = 'jobspark:latest'
    }

    stages {
        stage('🔍 Pull Code from GitHub') {
            steps {
                echo 'Récupération du code depuis GitHub...'
                git branch: 'main', url: "${GITHUB_REPO}"
            }
        }

        stage('📋 Verify Files') {
            steps {
                echo 'Vérification des fichiers du projet...'
                sh 'ls -la jobspark/'
                sh 'test -f jobspark/jobspark.py || echo "jobspark.py non trouvé"'
                sh 'test -f jobspark/Dockerfile || echo "Dockerfile non trouvé"'
            }
        }

        stage('🔑 Test SSH Connection') {
            steps {
                sshagent([SSH_CREDENTIALS]) {
                    sh 'ssh -o StrictHostKeyChecking=no ${EC2_USER}@${EC2_HOST} "echo ✅ Connexion SSH réussie"'
                }
            }
        }

        stage('🚀 Build & Deploy Docker Image') {
            steps {
                sshagent([SSH_CREDENTIALS]) {
                    sh """
                        ssh -o StrictHostKeyChecking=no ${EC2_USER}@${EC2_HOST} '
                            echo "🧹 Nettoyage anciens containers..."
                            docker stop ${APP_NAME} 2>/dev/null || true
                            docker rm ${APP_NAME} 2>/dev/null || true
                            docker rmi ${DOCKER_IMAGE} 2>/dev/null || true

                            echo "🔨 Construction image Docker jobspark..."
                            cd ~/ && git clone -b main ${GITHUB_REPO} temp_jobspark || (cd temp_jobspark && git pull)
                            cd temp_jobspark/jobspark
                            docker build -t ${DOCKER_IMAGE} .

                            echo "🚀 Lancement du container Spark Job..."
                            docker run -d --name ${APP_NAME} ${DOCKER_IMAGE}
                            
                            echo "✅ Container lancé avec succès"
                        '
                    """
                }
            }
        }

        stage('📜 Logs & Validation') {
            steps {
                sshagent([SSH_CREDENTIALS]) {
                    sh """
                        ssh -o StrictHostKeyChecking=no ${EC2_USER}@${EC2_HOST} '
                            echo "📝 Suivi des logs jobspark..."
                            docker logs -f ${APP_NAME}
                        '
                    """
                }
            }
        }

        stage('⚡ Option: Exécution manuelle Spark') {
            steps {
                echo '''
                Pour exécuter manuellement depuis le container jobspark :
                docker exec -it ${APP_NAME} /bin/bash
                spark-submit --master spark://spark:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12 jobspark.py
                '''
            }
        }
    }

    post {
        success {
            echo '''
            🎉 DÉPLOIEMENT JOBSPARK RÉUSSI!
            📊 Container en cours d'exécution : docker ps | grep ${APP_NAME}
            📝 Suivi des logs : docker logs ${APP_NAME}
            '''
        }
        failure {
            echo '''
            ❌ ÉCHEC DU DÉPLOIEMENT JOBSPARK
            🔍 Vérifiez les logs et l'état Docker sur le serveur
            '''
        }
    }
}
