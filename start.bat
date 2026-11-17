@echo off
rem ------------------------------------------------------------------
rem  Start the whole platform and open the dashboard.
rem  Double-click this file. Safe to run again at any time.
rem ------------------------------------------------------------------
setlocal
title Event Processing Platform
cd /d "%~dp0airflow"

echo.
echo  Event Processing Platform
echo  =========================
echo.

docker info >nul 2>&1
if not errorlevel 1 goto docker_ready

rem Docker Desktop is not running. After a crash or power loss it can leave
rem stale socket files behind that stop it from starting ("The file cannot be
rem accessed by the system"). They are only runtime sockets, so while Docker
rem is not running they are moved aside and Docker recreates them.
tasklist /fi "imagename eq Docker Desktop.exe" | find /i "Docker Desktop.exe" >nul
if errorlevel 1 (
    for %%D in ("%LOCALAPPDATA%\Docker\run" "%LOCALAPPDATA%\docker-secrets-engine") do (
        if exist "%%~D" (
            dir /b /a "%%~D" 2>nul | findstr . >nul && (
                move "%%~D" "%%~D.stale-%RANDOM%" >nul 2>&1 && mkdir "%%~D"
            )
        )
    )
)

echo  Starting Docker Desktop...
if exist "%LOCALAPPDATA%\Programs\DockerDesktop\Docker Desktop.exe" (
    start "" "%LOCALAPPDATA%\Programs\DockerDesktop\Docker Desktop.exe"
) else (
    start "" "%ProgramFiles%\Docker\Docker\Docker Desktop.exe"
)
set /a tries=0
:wait_docker
timeout /t 5 /nobreak >nul
docker info >nul 2>&1
if not errorlevel 1 goto docker_ready
set /a tries+=1
if %tries% lss 60 goto wait_docker
echo.
echo  Docker did not start within 5 minutes. Open Docker Desktop, check it
echo  says "Engine running", then run this file again.
pause
exit /b 1

:docker_ready
echo  [1/3] Docker is running
echo  [2/3] Starting Airflow, PostgreSQL and the dashboard (first start takes a few minutes)...
docker compose up -d
if errorlevel 1 (
    echo  docker compose failed. See the messages above.
    pause
    exit /b 1
)

echo  [3/3] Waiting for the services to be ready...
set /a tries=0
:wait_services
timeout /t 5 /nobreak >nul
curl -s -f -o nul http://localhost:8080/api/v2/monitor/health || goto not_ready
curl -s -f -o nul http://localhost:8501/_stcore/health || goto not_ready
goto ready
:not_ready
set /a tries+=1
if %tries% lss 72 goto wait_services
echo  Still starting after 6 minutes. Opening the dashboard anyway; refresh it in a minute.

:ready
echo.
echo  Ready:
echo    Dashboard  http://localhost:8501
echo    Airflow    http://localhost:8080   (user: airflow)
echo.
echo  To stop everything later, double-click stop.bat
start "" http://localhost:8501
timeout /t 10 >nul
