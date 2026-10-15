@echo off
setlocal
echo =========================================================================
echo   VAULTPULSE AI - LAUNCHING FULL BIG DATA CLUSTER IN DOCKER
echo   Services: Zookeeper, Kafka, Hadoop NameNode, DataNode, VaultPulse App
echo =========================================================================

REM Add Docker to PATH if installed in user local appdata
if exist "%LOCALAPPDATA%\Programs\DockerDesktop\resources\bin\docker.exe" (
    set "PATH=%LOCALAPPDATA%\Programs\DockerDesktop\resources\bin;%PATH%"
)

echo.
echo [1/3] Building and starting all Docker services...
docker compose up --build -d

echo.
echo [2/3] Checking running containers...
docker compose ps

echo.
echo =========================================================================
echo   BIG DATA PIPELINE IS LIVE!
echo =========================================================================
echo   * VaultPulse Web Dashboard: http://localhost:8000
echo   * Hadoop HDFS NameNode UI:  http://localhost:9870
echo   * Hadoop HDFS DataNode UI:  http://localhost:9864
echo   * Apache Kafka Broker:      localhost:9092
echo.
echo   * View live application logs: docker compose logs -f app
echo   * Stop all containers:        docker_stop.bat
echo =========================================================================
pause
