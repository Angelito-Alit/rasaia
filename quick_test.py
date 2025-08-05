import mysql.connector

config = {
    'host': 'bluebyte.space',
    'user': 'bluebyte_angel',
    'password': 'orbitalsoft',
    'database': 'bluebyte_dtai_web',
    'port': 3306
}

def test_connection():
    try:
        conn = mysql.connector.connect(**config)
        cursor = conn.cursor()
        
        cursor.execute("SHOW TABLES")
        tables = cursor.fetchall()
        
        print("=== CONEXIÓN EXITOSA ===")
        print(f"Base de datos: {config['database']}")
        print(f"Servidor: {config['host']}")
        print(f"Total tablas: {len(tables)}")
        
        print("\nTablas encontradas:")
        for table in tables:
            print(f"- {table[0]}")
        
        cursor.execute("SELECT COUNT(*) FROM usuarios")
        users = cursor.fetchone()[0]
        print(f"\nTotal usuarios: {users}")
        
        cursor.execute("SELECT COUNT(*) FROM alumnos WHERE estado_alumno = 'activo'")
        active_students = cursor.fetchone()[0]
        print(f"Alumnos activos: {active_students}")
        
        cursor.close()
        conn.close()
        
        print("\n✓ CHATBOT LISTO PARA USAR")
        return True
        
    except Exception as e:
        print(f"✗ ERROR: {e}")
        return False

if __name__ == "__main__":
    test_connection()