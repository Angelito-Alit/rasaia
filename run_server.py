import subprocess
import sys
import threading
import time

def run_actions_server():
    print("Iniciando servidor de acciones en puerto 5055...")
    subprocess.run(["rasa", "run", "actions", "--port", "5055"])

def run_rasa_server():
    print("Esperando 5 segundos para iniciar servidor Rasa...")
    time.sleep(5)
    print("Iniciando servidor Rasa en puerto 5001...")
    subprocess.run([
        "rasa", "run", 
        "--enable-api", 
        "--cors", "*", 
        "--port", "5001",
        "--debug"
    ])

def main():
    print("=== CHATBOT ACADÉMICO RASA ===")
    print("Iniciando servidores...")
    
    actions_thread = threading.Thread(target=run_actions_server)
    rasa_thread = threading.Thread(target=run_rasa_server)
    
    actions_thread.daemon = True
    rasa_thread.daemon = True
    
    actions_thread.start()
    rasa_thread.start()
    
    print("\nServidores iniciados:")
    print("- Acciones: http://localhost:5055")
    print("- API REST: http://localhost:5001/webhooks/rest/webhook")
    print("\nPara enviar mensajes via POST:")
    print('curl -X POST http://localhost:5001/webhooks/rest/webhook \\')
    print('  -H "Content-Type: application/json" \\')
    print('  -d \'{"sender": "user", "message": "hola"}\'')
    
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nDeteniendo servidores...")
        sys.exit(0)

if __name__ == "__main__":
    main()