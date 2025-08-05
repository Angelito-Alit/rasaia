#!/bin/bash

echo "=== CHATBOT ACADÉMICO - DESPLIEGUE ==="

echo "1. Probando conexión a base de datos..."
python test_actions.py

if [ $? -eq 0 ]; then
    echo "✓ Conexión exitosa a bluebyte.space"
else
    echo "✗ Error de conexión - Verificar credenciales"
    exit 1
fi

echo "2. Entrenando modelo Rasa..."
python train.py

if [ $? -eq 0 ]; then
    echo "✓ Modelo entrenado correctamente"
else
    echo "✗ Error en entrenamiento"
    exit 1
fi

echo "3. Iniciando servicios..."
echo "Ejecutar en terminales separadas:"
echo "Terminal 1: rasa run actions --port 5055"
echo "Terminal 2: rasa run --enable-api --cors '*' --port 5005"

echo "4. Para Docker:"
echo "docker-compose up --build"

echo "5. Para Render:"
echo "git add . && git commit -m 'Deploy chatbot' && git push origin main"

echo "=== CONFIGURACIÓN COMPLETA ==="