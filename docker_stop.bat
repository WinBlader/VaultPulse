@echo off
setlocal
if exist "%LOCALAPPDATA%\Programs\DockerDesktop\resources\bin\docker.exe" (
    set "PATH=%LOCALAPPDATA%\Programs\DockerDesktop\resources\bin;%PATH%"
)
echo =========================================================================
echo   STOPPING VAULTPULSE AI DOCKER CLUSTER
echo =========================================================================
docker compose down
echo.
echo All containers stopped cleanly.
pause
