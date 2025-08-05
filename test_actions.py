import os
from dotenv import load_dotenv
from actions import DatabaseConnector

load_dotenv()

def test_database_connection():
    print("Probando conexión a la base de datos...")
    
    db = DatabaseConnector()
    
    try:
        result = db.execute_query("SELECT 1 as test")
        if result:
            print("Conexión exitosa!")
            return True
        else:
            print("Error en la conexión")
            return False
    except Exception as e:
        print(f"Error: {e}")
        return False

def test_students_count():
    print("Probando consulta de estudiantes...")
    
    db = DatabaseConnector()
    
    query = """
    SELECT COUNT(*) as total,
           SUM(CASE WHEN estado_alumno = 'activo' THEN 1 ELSE 0 END) as activos
    FROM alumnos
    """
    
    result = db.execute_query(query)
    
    if result:
        print(f"Total estudiantes: {result[0]['total']}")
        print(f"Estudiantes activos: {result[0]['activos']}")
        return True
    else:
        print("Error en la consulta")
        return False

def test_careers():
    print("Probando consulta de carreras...")
    
    db = DatabaseConnector()
    
    query = """
    SELECT nombre, COUNT(a.id) as total_alumnos
    FROM carreras c
    LEFT JOIN alumnos a ON c.id = a.carrera_id AND a.estado_alumno = 'activo'
    WHERE c.activa = TRUE
    GROUP BY c.id, c.nombre
    LIMIT 5
    """
    
    result = db.execute_query(query)
    
    if result:
        print("Carreras encontradas:")
        for row in result:
            print(f"- {row['nombre']}: {row['total_alumnos']} alumnos")
        return True
    else:
        print("Error en la consulta de carreras")
        return False

if __name__ == "__main__":
    print("=== PRUEBAS DE CONEXIÓN Y CONSULTAS ===\n")
    
    tests = [
        test_database_connection,
        test_students_count,
        test_careers
    ]
    
    passed = 0
    total = len(tests)
    
    for test in tests:
        try:
            if test():
                passed += 1
                print("✓ PASÓ\n")
            else:
                print("✗ FALLÓ\n")
        except Exception as e:
            print(f"✗ ERROR: {e}\n")
    
    print(f"=== RESULTADO: {passed}/{total} pruebas pasaron ===")
    
    if passed == total:
        print("Todas las pruebas pasaron! El sistema está listo.")
    else:
        print("Algunas pruebas fallaron. Revisa la configuración.")