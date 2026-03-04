@echo off
REM ============================================================
REM Start RedditAI Platform with Ngrok Tunneling (Windows)
REM ============================================================

setlocal enabledelayedexpansion

echo.
echo ============================================
echo   RedditAI - Ngrok Public Deployment
echo ============================================
echo.

REM Resolve project root (parent of scripts/)
cd /d "%~dp0\.."
set "PROJECT_ROOT=%cd%"
set "INFRA_DIR=%PROJECT_ROOT%\infrastructure"

REM Load .env file from infrastructure/
if exist "%INFRA_DIR%\.env" (
    for /f "usebackq tokens=1,* delims==" %%A in ("%INFRA_DIR%\.env") do (
        set "line=%%A"
        if not "!line:~0,1!"=="#" (
            if not "%%B"=="" set "%%A=%%B"
        )
    )
)

REM Check NGROK_AUTHTOKEN
if "%NGROK_AUTHTOKEN%"=="" (
    echo ERROR: NGROK_AUTHTOKEN is not set.
    echo.
    echo Get your token from: https://dashboard.ngrok.com/get-started/your-authtoken
    echo Then set it in infrastructure\.env:
    echo   NGROK_AUTHTOKEN=your_token_here
    echo.
    exit /b 1
)

echo Step 1: Starting all services with ngrok profile...
docker compose -f "%INFRA_DIR%\docker-compose.yml" --profile ngrok up -d --build

echo.
echo Step 2: Waiting for ngrok tunnel to establish...

set MAX_RETRIES=30
set RETRY_DELAY=3
set FRONTEND_URL=

for /l %%i in (1,1,%MAX_RETRIES%) do (
    if "!FRONTEND_URL!"=="" (
        timeout /t %RETRY_DELAY% /nobreak >nul 2>&1

        for /f "delims=" %%u in ('curl -s http://localhost:4042/api/tunnels 2^>nul ^| python -c "import sys,json; data=json.load(sys.stdin); [print(t['public_url']) for t in data.get('tunnels',[]) if t.get('name')=='frontend']" 2^>nul') do (
            set "FRONTEND_URL=%%u"
        )

        if "!FRONTEND_URL!"=="" (
            echo   Waiting for tunnel... ^(%%i/%MAX_RETRIES%^)
        )
    )
)

if "%FRONTEND_URL%"=="" (
    echo ERROR: Could not retrieve ngrok tunnel URL.
    echo Check ngrok logs: docker compose -f "%INFRA_DIR%\docker-compose.yml" logs ngrok
    exit /b 1
)

echo.
echo ============================================
echo   PLATFORM IS LIVE!
echo ============================================
echo.
echo   Public URL (share this):  %FRONTEND_URL%
echo   Ngrok Dashboard:          http://localhost:4042
echo.
echo   Local access still works:
echo   Frontend:    http://localhost:3000
echo   API Gateway: http://localhost:8001
echo.
echo Share the Public URL above with users so they can
echo log in and converse on the platform!
echo.
echo To stop: docker compose -f "%INFRA_DIR%\docker-compose.yml" --profile ngrok down

endlocal
