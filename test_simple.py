import requests
import json

def test_simple_chatbot():
    base_url = "http://localhost:5001"
    
    print("=== PROBANDO CHATBOT INTELIGENTE ===\n")
    
    print("1. Probando conexión...")
    try:
        response = requests.get(f"{base_url}/test")
        if response.status_code == 200:
            data = response.json()
            print(f"✓ {data['message']}")
        else:
            print("✗ Error de conexión")
            return
    except:
        print("✗ Chatbot no está ejecutándose")
        print("Ejecuta: python version_simple.py")
        return
    
    print("\n2. Probando conversación inteligente...")
    
    preguntas = [
        "Hola, buenos días",
        "¿Cuántos estudiantes tenemos en total?",
        "Dime el promedio del alumno 2022371156",
        "¿Hay alumnos en riesgo académico?",
        "Muéstrame los horarios del grupo iDGS10",
        "¿Qué materias se ven en primer cuatrimestre?",
        "¿Cuántos grupos activos tenemos?",
        "¿Hay solicitudes de ayuda pendientes?",
        "Información completa del estudiante 2023123456",
        "Gracias, hasta luego"
    ]
    
    for i, pregunta in enumerate(preguntas, 1):
        print(f"\n{i}. Usuario: {pregunta}")
        
        try:
            response = requests.post(f"{base_url}/api/chat", json={
                "message": pregunta,
                "role": "directivo",
                "user_id": 1
            })
            
            if response.status_code == 200:
                data = response.json()
                print(f"   Asistente: {data['response']}")
                if data.get('intent'):
                    print(f"   (Intención detectada: {data['intent']})")
            else:
                print(f"   Error HTTP: {response.status_code}")
                
        except Exception as e:
            print(f"   Error: {e}")

if __name__ == "__main__":
    test_simple_chatbot()