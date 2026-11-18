@echo off
rem ------------------------------------------------------------------
rem  Start streaming: Kafka + the Spark Structured Streaming job, then
rem  replay one real trading day of 1-minute prices (about 7 minutes).
rem  Run it again to replay: duplicates are detected and dropped.
rem  Needs the platform running (start.bat).
rem ------------------------------------------------------------------
setlocal
title Event Processing Platform - streaming
cd /d "%~dp0airflow"

docker info >nul 2>&1
if errorlevel 1 (
    echo  Docker isn't running. Double-click start.bat first.
    pause
    exit /b 1
)

echo  [1/2] Starting Kafka and the Spark streaming job (first time: about 2 minutes)...
docker compose --profile streaming up -d kafka stream-processor
if errorlevel 1 (
    echo  Couldn't start streaming. See the messages above.
    pause
    exit /b 1
)

echo  [2/2] Replaying a trading day of real 1-minute prices into Kafka...
docker compose --profile streaming up -d --force-recreate stream-producer

echo.
echo  Streaming. Watch it on the dashboard's "Live stream" page.
echo  Stop everything later with stop.bat
rem Open the dashboard in its own app window (Microsoft Edge ships with
rem Windows); fall back to the default browser if Edge isn't there.
set "EDGE=%ProgramFiles(x86)%\Microsoft\Edge\Application\msedge.exe"
if not exist "%EDGE%" set "EDGE=%ProgramFiles%\Microsoft\Edge\Application\msedge.exe"
if exist "%EDGE%" (
    start "" "%EDGE%" --app=http://localhost:8501/stream --window-size=1400,900
) else (
    start "" http://localhost:8501/stream
)
timeout /t 8 >nul
