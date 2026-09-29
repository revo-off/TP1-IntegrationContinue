pipeline {
    agent any

    environment {
        PYTHON = 'python3'
        KAFKA_BOOTSTRAP_SERVERS = 'kafka:29092'
        API_BASE_URL = 'http://sales-api:8000'
        POSTGRES_HOST = 'postgres'
        POSTGRES_PORT = '5432'
        POSTGRES_DB = 'sales'
        POSTGRES_USER = 'sales'
        POSTGRES_PASSWORD = 'sales'
        RUN_INTEGRATION_TESTS = 'true'
        RUN_E2E_TESTS = 'true'
    }

    stages {

        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Environment') {
            steps {
                sh 'python3 --version'
                sh 'docker --version'
            }
        }

        stage('Install') {
            steps {
                sh 'python3 -m pip install --break-system-packages --user -r requirements.txt'
            }
        }

        stage('Unit Tests') {
            steps {
                sh '''
                    python3 -m pytest tests/unit \
                      --junitxml=test-results.xml \
                      --cov=app \
                      --cov-report=xml:coverage.xml \
                      --cov-report=term-missing
                '''
            }
        }

        stage('Integration Tests') {
            steps {
                sh 'python3 -m pytest tests/integration'
            }
        }

        stage('Build') {
            steps {
                sh 'docker build -f docker/api/Dockerfile -t sales-api:latest .'
                sh 'docker build -f docker/spark/Dockerfile -t spark-streaming:latest .'
            }
        }

        stage('E2E Tests') {
            steps {
                sh 'python3 -m pytest tests/e2e'
            }
        }

        stage('SonarQube') {
            steps {
                withSonarQubeEnv('SonarQube') {
                    sh "${tool('SonarScanner')}/bin/sonar-scanner"
                }
            }
        }

        stage('Quality Gate') {
            steps {
                timeout(time: 5, unit: 'MINUTES') {
                    waitForQualityGate abortPipeline: true
                }
            }
        }
    }

    post {
        always {
            junit allowEmptyResults: true, testResults: '**/test-results.xml'
            archiveArtifacts allowEmptyArchive: true, artifacts: 'coverage.xml'
        }
    }
}
