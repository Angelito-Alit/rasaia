import subprocess
import sys
import os

def install_requirements():
    print("Instalando dependencias...")
    subprocess.run([sys.executable, "-m", "pip", "install", "-r", "requirements.txt"])

def download_spacy_model():
    print("Descargando modelo de spaCy en español...")
    subprocess.run([sys.executable, "-m", "spacy", "download", "es_core_news_md"])

def train_model():
    print("Entrenando el modelo de Rasa...")
    subprocess.run(["rasa", "train"])

def run_setup():
    install_requirements()
    download_spacy_model()
    train_model()
    print("Setup completado. Para iniciar el servidor:")
    print("1. En una terminal: rasa run actions --port 5055")
    print("2. En otra terminal: rasa run --enable-api --cors '*' --port 5001")

if __name__ == "__main__":
    run_setup()