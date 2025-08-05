import subprocess
import sys
import os

def train_model():
    try:
        print("Iniciando entrenamiento del modelo...")
        
        result = subprocess.run([
            "rasa", "train", 
            "--config", "config.yml",
            "--domain", "domain.yml",
            "--data", "nlu.yml", "rules.yml", "stories.yml"
        ], capture_output=True, text=True)
        
        if result.returncode == 0:
            print("Modelo entrenado exitosamente!")
            print(result.stdout)
        else:
            print("Error durante el entrenamiento:")
            print(result.stderr)
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    train_model()