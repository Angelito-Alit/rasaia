@echo off
echo ====================================
echo   CHATBOT INTELIGENTE ACADEMICO
echo ====================================

echo.
echo 1. Instalando dependencias...
pip install -r requirements_simple.txt

echo.
echo 2. Probando conexion a base de datos...
python quick_test.py

if %errorlevel% neq 0 (
    echo Error de conexion a base de datos
    pause
    exit /b 1
)

echo.
echo 3. Iniciando chatbot inteligente en puerto 5001...
echo Tu frontend debe apuntar a: http://localhost:5001/api/chat
echo.
echo Presiona Ctrl+C para detener
echo.
python version_simple.py