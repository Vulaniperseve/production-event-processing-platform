@echo off
rem Stop the platform. Your data is kept (containers are stopped, not deleted).
cd /d "%~dp0airflow"
echo Stopping the platform (data is kept)...
docker compose stop
echo Done. Start again with start.bat
timeout /t 5 >nul
