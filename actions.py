from typing import Any, Text, Dict, List
import mysql.connector
import os
from dotenv import load_dotenv
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher

load_dotenv()

config = {
    'host': os.getenv('DB_HOST', 'bluebyte.space'),
    'user': os.getenv('DB_USER', 'bluebyte_angel'),
    'password': os.getenv('DB_PASSWORD', 'orbitalsoft'),
    'database': os.getenv('DB_NAME', 'bluebyte_dtai_web'),
    'port': int(os.getenv('DB_PORT', 3306))
}

def get_db_connection():
    return mysql.connector.connect(**config)

def execute_query(query, params=None):
    try:
        conn = get_db_connection()
        cursor = conn.cursor(dictionary=True)
        cursor.execute(query, params or ())
        result = cursor.fetchall()
        cursor.close()
        conn.close()
        return result
    except Exception as e:
        return {"error": f"Error al consultar la base de datos: {str(e)}"}

class ActionGetStudentsByCareer(Action):
    def name(self) -> Text:
        return "action_get_students_by_career"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        carrera_nombre = tracker.get_slot('carrera')
        
        where_condition = "WHERE a.estado_alumno = 'activo' AND u.activo = TRUE"
        params = []
        
        if carrera_nombre:
            where_condition += " AND c.nombre LIKE %s"
            params.append(f"%{carrera_nombre}%")
        
        query = f"""
        SELECT CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
               a.matricula,
               c.nombre as carrera,
               a.cuatrimestre_actual,
               a.fecha_ingreso,
               AVG(ed.calificacion_final) as promedio_general
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN evaluacion_detalle ed ON a.id = ed.alumno_id
        {where_condition}
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, a.cuatrimestre_actual, a.fecha_ingreso
        ORDER BY c.nombre, a.cuatrimestre_actual, promedio_general DESC
        LIMIT 30
        """
        
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontraron estudiantes activos{' en ' + carrera_nombre if carrera_nombre else ''}."
            else:
                response = f"Estudiantes activos{' de ' + carrera_nombre if carrera_nombre else ''}:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"✅ {i}. {row['nombre_completo']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Cuatrimestre: {row['cuatrimestre_actual']}\n"
                    if row['promedio_general']:
                        response += f"   Promedio: {row['promedio_general']:.2f}\n"
                    response += f"   Fecha ingreso: {row['fecha_ingreso']}\n\n"
        else:
            response = "No pude obtener la información de estudiantes activos."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetStudentByNameOrId(Action):
    def name(self) -> Text:
        return "action_get_student_by_name_or_id"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        matricula = next(tracker.get_latest_entity_values("matricula"), None)
        nombre_estudiante = next(tracker.get_latest_entity_values("nombre_estudiante"), None)
        
        if matricula:
            where_condition = "WHERE a.matricula = %s"
            params = [matricula]
        elif nombre_estudiante:
            where_condition = "WHERE CONCAT(u.nombre, ' ', u.apellido) LIKE %s"
            params = [f"%{nombre_estudiante}%"]
        else:
            dispatcher.utter_message(text="Necesito el nombre o matrícula del estudiante para consultar su información.")
            return []
        
        query_alumno = f"""
        SELECT a.matricula, u.nombre, u.apellido, u.correo,
               a.cuatrimestre_actual, a.fecha_ingreso, a.telefono,
               a.estado_alumno, c.nombre as carrera,
               g.codigo as grupo_actual,
               a.tutor_nombre, a.tutor_telefono,
               CONCAT(pt.nombre, ' ', pt.apellido) as tutor_grupo,
               pt_prof.numero_empleado as tutor_empleado
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN profesores pt_prof ON g.profesor_tutor_id = pt_prof.id
        LEFT JOIN usuarios pt ON pt_prof.usuario_id = pt.id
        {where_condition}
        LIMIT 1
        """
        
        result = execute_query(query_alumno, params)
        
        if not result or isinstance(result, dict) or not result:
            search_term = matricula if matricula else nombre_estudiante
            dispatcher.utter_message(text=f"No encontré información para el estudiante: {search_term}.")
            return []
        
        data = result[0]
        response = f"Información del estudiante {data['nombre']} {data['apellido']}:\n\n"
        response += f"Matrícula: {data['matricula']}\n"
        response += f"Correo: {data['correo']}\n"
        response += f"Estado: {data['estado_alumno']}\n"
        response += f"Carrera: {data['carrera']}\n"
        response += f"Cuatrimestre: {data['cuatrimestre_actual']}\n"
        response += f"Grupo: {data['grupo_actual'] or 'Sin asignar'}\n"
        
        if data['tutor_grupo']:
            response += f"Tutor del grupo: {data['tutor_grupo']} ({data['tutor_empleado']})\n"
        else:
            response += "Tutor del grupo: Sin asignar\n"
        
        response += f"Fecha ingreso: {data['fecha_ingreso']}\n"
        if data['telefono']:
            response += f"Teléfono: {data['telefono']}\n"
        if data['tutor_nombre']:
            response += f"Tutor personal: {data['tutor_nombre']}\n"
        if data['tutor_telefono']:
            response += f"Teléfono tutor personal: {data['tutor_telefono']}\n"
        
        query_calificaciones = f"""
        SELECT 
            asig.nombre as asignatura,
            ed.numero_parcial,
            ed.calificacion,
            ed.oportunidad,
            ed.fecha_evaluacion,
            cal.calificacion_final,
            cal.estatus,
            CONCAT(up.nombre, ' ', up.apellido) as profesor
        FROM alumnos a
        JOIN calificaciones cal ON a.id = cal.alumno_id
        JOIN asignaturas asig ON cal.asignatura_id = asig.id
        JOIN profesores p ON cal.profesor_id = p.id
        JOIN usuarios up ON p.usuario_id = up.id
        LEFT JOIN evaluaciones_detalle ed ON cal.id = ed.calificacion_id
        {where_condition}
        ORDER BY asig.nombre, ed.numero_parcial, ed.fecha_evaluacion
        """
        
        calificaciones = execute_query(query_calificaciones, params)
        
        if calificaciones and not isinstance(calificaciones, dict):
            response += f"\nAsignaturas y calificaciones ({len(calificaciones)} registros):\n"
            current_subject = None
            for cal in calificaciones:
                if current_subject != cal['asignatura']:
                    current_subject = cal['asignatura']
                    response += f"\n  {cal['asignatura']} - Prof. {cal['profesor']}\n"
                    response += f"    Calificación final: {cal['calificacion_final']} - Estatus: {cal['estatus']}\n"
                
                if cal['numero_parcial'] and cal['calificacion']:
                    response += f"    Parcial {cal['numero_parcial']} ({cal['oportunidad']}): {cal['calificacion']}\n"
        
        query_indices_alumno = f"""
        SELECT 
            COUNT(cal.id) as total_materias,
            COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) as aprobadas,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as reprobadas,
            COUNT(CASE WHEN cal.estatus = 'cursando' THEN 1 END) as cursando,
            ROUND((COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2) as indice_aprobacion,
            ROUND((COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2) as indice_reprobacion
        FROM alumnos a
        JOIN calificaciones cal ON a.id = cal.alumno_id
        {where_condition}
        """
        
        indices = execute_query(query_indices_alumno, params)
        
        if indices and not isinstance(indices, dict) and indices:
            ind = indices[0]
            response += f"\nÍndices académicos:\n"
            response += f"  Total materias: {ind['total_materias']}\n"
            response += f"  Aprobadas: {ind['aprobadas']}\n"
            response += f"  Reprobadas: {ind['reprobadas']}\n"
            response += f"  Cursando: {ind['cursando']}\n"
            response += f"  Índice de aprobación: {ind['indice_aprobacion']}%\n"
            response += f"  Índice de reprobación: {ind['indice_reprobacion']}%\n"
        
        dispatcher.utter_message(text=response)
        return []
    
class ActionGetAllStudents(Action):
    def name(self) -> Text:
        return "action_get_all_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            c.nombre as carrera,
            g.cuatrimestre,
            g.codigo as grupo,
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            a.estado_alumno
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        ORDER BY c.nombre, g.cuatrimestre, g.codigo, u.apellido, u.nombre
        """
        
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron estudiantes en el sistema."
            else:
                response = f"Información de estudiantes ({len(result)} registros):\n\n"
                current_career = None
                current_cuatrimestre = None
                current_grupo = None
                
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        current_cuatrimestre = None
                        current_grupo = None
                        response += f"{current_career}:\n"
                    
                    if current_cuatrimestre != row['cuatrimestre']:
                        current_cuatrimestre = row['cuatrimestre']
                        current_grupo = None
                        response += f"  Cuatrimestre {row['cuatrimestre'] or 'Sin asignar'}:\n"
                    
                    if current_grupo != row['grupo']:
                        current_grupo = row['grupo']
                        response += f"    Grupo {row['grupo'] or 'Sin grupo'}:\n"
                    
                    response += f"      {row['nombre_completo']} ({row['matricula']}) - {row['estado_alumno']}\n"
                
        else:
            response = "No pude obtener la información de estudiantes."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetStudentGrades(Action):
    def name(self) -> Text:
        return "action_get_student_grades"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        matricula = next(tracker.get_latest_entity_values("matricula"), None)
        nombre_estudiante = next(tracker.get_latest_entity_values("nombre_estudiante"), None)
        
        if matricula:
            query = """
            SELECT u.nombre, u.apellido, a.matricula, asig.nombre as asignatura,
                   ed.parcial_1, ed.parcial_2, ed.parcial_3,
                   ed.calificacion_ordinario, ed.calificacion_extraordinario,
                   ed.calificacion_final, ed.estatus, ed.ciclo_escolar
            FROM alumnos a
            JOIN usuarios u ON a.usuario_id = u.id
            JOIN evaluacion_detalle ed ON a.id = ed.alumno_id
            JOIN asignaturas asig ON ed.asignatura_id = asig.id
            WHERE a.matricula = %s
            ORDER BY ed.ciclo_escolar DESC, asig.nombre
            """
            result = execute_query(query, (matricula,))
        elif nombre_estudiante:
            query = """
            SELECT u.nombre, u.apellido, a.matricula, asig.nombre as asignatura,
                   ed.parcial_1, ed.parcial_2, ed.parcial_3,
                   ed.calificacion_ordinario, ed.calificacion_extraordinario,
                   ed.calificacion_final, ed.estatus, ed.ciclo_escolar
            FROM alumnos a
            JOIN usuarios u ON a.usuario_id = u.id
            JOIN evaluacion_detalle ed ON a.id = ed.alumno_id
            JOIN asignaturas asig ON ed.asignatura_id = asig.id
            WHERE CONCAT(u.nombre, ' ', u.apellido) LIKE %s
            ORDER BY ed.ciclo_escolar DESC, asig.nombre
            """
            result = execute_query(query, (f"%{nombre_estudiante}%",))
        else:
            dispatcher.utter_message(text="Necesito el nombre o matrícula del estudiante para consultar sus calificaciones.")
            return []
        
        if result and not isinstance(result, dict) and result:
            student_name = f"{result[0]['nombre']} {result[0]['apellido']}"
            response = f"Calificaciones de {student_name} ({result[0]['matricula']}):\n\n"
            current_cycle = None
            
            for row in result:
                if current_cycle != row['ciclo_escolar']:
                    current_cycle = row['ciclo_escolar']
                    response += f"{current_cycle}:\n"
                
                response += f"  {row['asignatura']}:\n"
                if row['parcial_1'] is not None:
                    response += f"    Parcial 1: {row['parcial_1']}\n"
                if row['parcial_2'] is not None:
                    response += f"    Parcial 2: {row['parcial_2']}\n"
                if row['parcial_3'] is not None:
                    response += f"    Parcial 3: {row['parcial_3']}\n"
                if row['calificacion_ordinario'] is not None:
                    response += f"    Ordinario: {row['calificacion_ordinario']}\n"
                if row['calificacion_extraordinario'] is not None:
                    response += f"    Extraordinario: {row['calificacion_extraordinario']}\n"
                if row['calificacion_final'] is not None:
                    response += f"    Final: {row['calificacion_final']}\n"
                response += f"    Estatus: {row['estatus']}\n\n"
        else:
            search_term = matricula if matricula else nombre_estudiante
            response = f"No encontré calificaciones para el estudiante: {search_term}."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTeacherByNameOrId(Action):
    def name(self) -> Text:
        return "action_get_teacher_by_name_or_id"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        numero_empleado = next(tracker.get_latest_entity_values("numero_empleado"), None)
        nombre_profesor = next(tracker.get_latest_entity_values("nombre_profesor"), None)
        
        if numero_empleado:
            where_condition = "WHERE p.numero_empleado = %s AND p.activo = TRUE"
            params = [numero_empleado]
        elif nombre_profesor:
            where_condition = "WHERE CONCAT(u.nombre, ' ', u.apellido) LIKE %s AND p.activo = TRUE"
            params = [f"%{nombre_profesor}%"]
        else:
            dispatcher.utter_message(text="Necesito el nombre o número de empleado del profesor para consultar su información.")
            return []
        
        query_profesor = f"""
        SELECT p.numero_empleado, u.nombre, u.apellido, u.correo,
               p.titulo_academico, p.especialidad, p.experiencia_años,
               c.nombre as carrera, p.fecha_contratacion,
               p.telefono, p.extension, p.cedula_profesional
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN carreras c ON p.carrera_id = c.id
        {where_condition}
        """
        
        result = execute_query(query_profesor, params)
        
        if not result or isinstance(result, dict) or not result:
            search_term = numero_empleado if numero_empleado else nombre_profesor
            dispatcher.utter_message(text=f"No encontré información para el profesor: {search_term}.")
            return []
        
        data = result[0]
        response = f"Información del profesor {data['nombre']} {data['apellido']}:\n\n"
        response += f"Número empleado: {data['numero_empleado']}\n"
        response += f"Correo: {data['correo']}\n"
        response += f"Carrera: {data['carrera']}\n"
        response += f"Título académico: {data['titulo_academico'] or 'No especificado'}\n"
        response += f"Especialidad: {data['especialidad'] or 'No especificada'}\n"
        response += f"Experiencia: {data['experiencia_años'] or 0} años\n"
        response += f"Fecha contratación: {data['fecha_contratacion']}\n"
        if data['telefono']:
            response += f"Teléfono: {data['telefono']}\n"
        if data['extension']:
            response += f"Extensión: {data['extension']}\n"
        if data['cedula_profesional']:
            response += f"Cédula profesional: {data['cedula_profesional']}\n"
        
        query_reprobados = f"""
        SELECT DISTINCT 
            CONCAT(ua.nombre, ' ', ua.apellido) as alumno_nombre,
            a.matricula,
            g.codigo as grupo,
            asig.nombre as asignatura,
            cal.calificacion_final
        FROM profesores p
        JOIN calificaciones cal ON p.id = cal.profesor_id
        JOIN alumnos a ON cal.alumno_id = a.id
        JOIN usuarios ua ON a.usuario_id = ua.id
        JOIN grupos g ON cal.grupo_id = g.id
        JOIN asignaturas asig ON cal.asignatura_id = asig.id
        {where_condition} AND cal.estatus = 'reprobado'
        ORDER BY g.codigo, ua.apellido
        """
        
        reprobados = execute_query(query_reprobados, params)
        
        if reprobados and not isinstance(reprobados, dict):
            response += f"\nAlumnos reprobados ({len(reprobados)}):\n"
            for rep in reprobados:
                response += f"  {rep['alumno_nombre']} ({rep['matricula']}) - Grupo {rep['grupo']}\n"
                response += f"    Asignatura: {rep['asignatura']} - Calificación: {rep['calificacion_final']}\n"
        
        query_asignaturas = f"""
        SELECT DISTINCT asig.nombre as asignatura, g.codigo as grupo
        FROM profesores p
        JOIN calificaciones cal ON p.id = cal.profesor_id
        JOIN asignaturas asig ON cal.asignatura_id = asig.id
        JOIN grupos g ON cal.grupo_id = g.id
        {where_condition}
        ORDER BY asig.nombre
        """
        
        asignaturas = execute_query(query_asignaturas, params)
        
        if asignaturas and not isinstance(asignaturas, dict):
            response += f"\nAsignaturas que imparte ({len(asignaturas)}):\n"
            for asig in asignaturas:
                response += f"  {asig['asignatura']} - Grupo {asig['grupo']}\n"
        
        query_indices = f"""
        SELECT 
            COUNT(cal.id) as total_calificaciones,
            COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) as aprobados,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as reprobados,
            ROUND((COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2) as indice_aprobacion,
            ROUND((COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2) as indice_reprobacion
        FROM profesores p
        JOIN calificaciones cal ON p.id = cal.profesor_id
        {where_condition}
        """
        
        indices = execute_query(query_indices, params)
        
        if indices and not isinstance(indices, dict) and indices:
            ind = indices[0]
            response += f"\nÍndices académicos:\n"
            response += f"  Total evaluaciones: {ind['total_calificaciones']}\n"
            response += f"  Aprobados: {ind['aprobados']}\n"
            response += f"  Reprobados: {ind['reprobados']}\n"
            response += f"  Índice de aprobación: {ind['indice_aprobacion']}%\n"
            response += f"  Índice de reprobación: {ind['indice_reprobacion']}%\n"
        
        query_grupos = f"""
        SELECT DISTINCT g.codigo as grupo, c.nombre as carrera
        FROM profesores p
        JOIN calificaciones cal ON p.id = cal.profesor_id
        JOIN grupos g ON cal.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        {where_condition}
        ORDER BY c.nombre, g.codigo
        """
        
        grupos = execute_query(query_grupos, params)
        
        if grupos and not isinstance(grupos, dict):
            response += f"\nGrupos a los que da clases ({len(grupos)}):\n"
            for grupo in grupos:
                response += f"  {grupo['grupo']} - {grupo['carrera']}\n"
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetGroupInfo(Action):
    def name(self) -> Text:
        return "action_get_group_info"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        grupo = next(tracker.get_latest_entity_values("grupo"), None)
        
        if not grupo:
            dispatcher.utter_message(text="Necesito el código del grupo para consultar su información.")
            return []
        
        query = """
        SELECT g.codigo, g.cuatrimestre, g.ciclo_escolar, g.periodo, g.año,
               c.nombre as carrera, g.aula, g.capacidad_maxima,
               CONCAT(ut.nombre, ' ', ut.apellido) as tutor,
               p.numero_empleado as tutor_empleado,
               COUNT(DISTINCT ag.alumno_id) as total_alumnos,
               COUNT(DISTINCT pag.asignatura_id) as total_asignaturas,
               COUNT(DISTINCT pag.profesor_id) as total_profesores
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN profesores p ON g.profesor_tutor_id = p.id
        LEFT JOIN usuarios ut ON p.usuario_id = ut.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        LEFT JOIN profesor_asignatura_grupo pag ON g.id = pag.grupo_id AND pag.activo = TRUE
        WHERE g.codigo = %s AND g.activo = TRUE
        GROUP BY g.id
        """
        result = execute_query(query, (grupo,))
        
        if result and not isinstance(result, dict) and result:
            data = result[0]
            response = f"Información del grupo {data['codigo']}:\n\n"
            response += f"Carrera: {data['carrera']}\n"
            response += f"Cuatrimestre: {data['cuatrimestre']}\n"
            response += f"Ciclo escolar: {data['ciclo_escolar']}\n"
            response += f"Período: {data['periodo']} {data['año']}\n"
            response += f"Aula asignada: {data['aula'] or 'No asignada'}\n"
            response += f"Capacidad máxima: {data['capacidad_maxima']}\n"
            response += f"Total alumnos: {data['total_alumnos']}\n"
            response += f"Total asignaturas: {data['total_asignaturas']}\n"
            response += f"Total profesores: {data['total_profesores']}\n"
            
            if data['tutor']:
                response += f"Tutor: {data['tutor']} ({data['tutor_empleado']})\n"
            else:
                response += "Sin tutor asignado\n"
                
            query_students = """
            SELECT u.nombre, u.apellido, a.matricula
            FROM alumnos_grupos ag
            JOIN alumnos a ON ag.alumno_id = a.id
            JOIN usuarios u ON a.usuario_id = u.id
            JOIN grupos g ON ag.grupo_id = g.id
            WHERE g.codigo = %s AND ag.activo = TRUE
            ORDER BY u.apellido, u.nombre
            """
            students = execute_query(query_students, (grupo,))
            
            if students and not isinstance(students, dict):
                response += f"\nAlumnos inscritos ({len(students)}):\n"
                for student in students[:10]:
                    response += f"  - {student['nombre']} {student['apellido']} ({student['matricula']})\n"
                if len(students) > 10:
                    response += f"  ... y {len(students) - 10} más\n"
        else:
            response = f"No encontré información para el grupo {grupo}."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetLowGradeStudents(Action):
    def name(self) -> Text:
        return "action_get_low_grade_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT DISTINCT u.nombre, u.apellido, a.matricula,
               asig.nombre as asignatura, ed.calificacion_final,
               c.nombre as carrera, g.codigo as grupo
        FROM evaluacion_detalle ed
        JOIN alumnos a ON ed.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN asignaturas asig ON ed.asignatura_id = asig.id
        JOIN grupos g ON ed.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        WHERE ed.calificacion_final < 8 AND ed.calificacion_final IS NOT NULL
              AND a.estado_alumno = 'activo'
        ORDER BY ed.calificacion_final ASC, u.apellido
        LIMIT 30
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay alumnos con calificaciones menores a 8."
            else:
                response = f"Alumnos con calificaciones menores a 8 ({len(result)} casos):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"    Grupo: {row['grupo']}\n"
                    response += f"    Asignatura: {row['asignatura']}\n"
                    response += f"    Calificación: {row['calificacion_final']}\n\n"
        else:
            response = "No pude obtener información de calificaciones bajas."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTopStudents(Action):
    def name(self) -> Text:
        return "action_get_top_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula, c.nombre as carrera,
               AVG(ed.calificacion_final) as promedio,
               COUNT(ed.id) as materias_evaluadas,
               g.codigo as grupo
        FROM evaluacion_detalle ed
        JOIN alumnos a ON ed.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN grupos g ON ed.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        WHERE ed.calificacion_final IS NOT NULL AND a.estado_alumno = 'activo'
              AND ed.calificacion_final >= 8
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo
        HAVING materias_evaluadas >= 3
        ORDER BY promedio DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No encontré estudiantes destacados con suficientes evaluaciones."
            else:
                response = "Estudiantes destacados (Top 15):\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"   Promedio: {row['promedio']:.2f}\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Grupo: {row['grupo']}\n"
                    response += f"   Materias evaluadas: {row['materias_evaluadas']}\n\n"
        else:
            response = "No pude obtener información de estudiantes destacados."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetHighRiskStudents(Action):
    def name(self) -> Text:
        return "action_get_high_risk_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula,
               COUNT(rr.id) as total_reportes,
               GROUP_CONCAT(DISTINCT rr.tipo_riesgo) as tipos_riesgo,
               GROUP_CONCAT(DISTINCT rr.nivel_riesgo) as niveles_riesgo,
               c.nombre as carrera, g.codigo as grupo
        FROM reportes_riesgo rr
        JOIN alumnos a ON rr.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN carreras c ON g.carrera_id = c.id
        WHERE rr.estado IN ('abierto', 'en_proceso') AND a.estado_alumno = 'activo'
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo
        HAVING total_reportes >= 2
        ORDER BY total_reportes DESC, u.apellido
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay estudiantes con múltiples reportes de riesgo activos."
            else:
                response = f"Estudiantes en alto riesgo ({len(result)} casos):\n\n"
                for row in result:
                    response += f"{row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"  Carrera: {row['carrera'] or 'Sin carrera'}\n"
                    response += f"  Grupo: {row['grupo'] or 'Sin grupo'}\n"
                    response += f"  Total reportes activos: {row['total_reportes']}\n"
                    response += f"  Tipos de riesgo: {row['tipos_riesgo']}\n"
                    response += f"  Niveles: {row['niveles_riesgo']}\n\n"
        else:
            response = "No pude obtener información de reportes de riesgo."
        
        dispatcher.utter_message(text=response)
        return []




class ActionGetStudentsWithoutGroup(Action):
    def name(self) -> Text:
        return "action_get_students_without_group"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula, c.nombre as carrera,
               a.cuatrimestre_actual, a.fecha_ingreso
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        WHERE ag.id IS NULL AND a.estado_alumno = 'activo'
        ORDER BY c.nombre, a.cuatrimestre_actual, u.apellido
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "Todos los estudiantes activos tienen grupo asignado."
            else:
                response = f"Estudiantes sin grupo asignado ({len(result)} casos):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"    Cuatrimestre: {row['cuatrimestre_actual']}\n"
                    response += f"    Fecha ingreso: {row['fecha_ingreso']}\n\n"
        else:
            response = "No pude obtener información de estudiantes sin grupo."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetInactiveStudents(Action):
    def name(self) -> Text:
        return "action_get_inactive_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula, c.nombre as carrera,
               a.estado_alumno, a.cuatrimestre_actual, a.fecha_ingreso
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        WHERE a.estado_alumno != 'activo'
        ORDER BY a.estado_alumno, c.nombre, u.apellido
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "Todos los alumnos registrados están activos."
            else:
                response = f"Alumnos con estado inactivo ({len(result)} casos):\n\n"
                current_status = None
                for row in result:
                    if current_status != row['estado_alumno']:
                        current_status = row['estado_alumno']
                        status_name = {
                            'baja_temporal': 'BAJA TEMPORAL',
                            'egresado': 'EGRESADOS',
                            'baja_definitiva': 'BAJA DEFINITIVA'
                        }.get(current_status, current_status.upper())
                        response += f"{status_name}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"    Carrera: {row['carrera']}\n"
                    response += f"    Último cuatrimestre: {row['cuatrimestre_actual']}\n\n"
        else:
            response = "No pude obtener información de estudiantes inactivos."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTeachersByCareer(Action):
    def name(self) -> Text:
        return "action_get_teachers_by_career"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT c.nombre as carrera,
               COUNT(p.id) as total_profesores,
               GROUP_CONCAT(CONCAT(u.nombre, ' ', u.apellido, ' (', p.numero_empleado, ')') 
                          ORDER BY u.apellido SEPARATOR ', ') as profesores_lista
        FROM carreras c
        LEFT JOIN profesores p ON c.id = p.carrera_id AND p.activo = TRUE
        LEFT JOIN usuarios u ON p.usuario_id = u.id
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre
        ORDER BY total_profesores DESC, c.nombre
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            response = "Profesores activos por carrera:\n\n"
            total_general = 0
            
            for row in result:
                response += f"{row['carrera']}: {row['total_profesores']} profesores\n"
                if row['profesores_lista'] and row['total_profesores'] > 0:
                    if len(row['profesores_lista']) > 300:
                        response += f"  Algunos profesores: {row['profesores_lista'][:300]}...\n"
                    else:
                        response += f"  Profesores: {row['profesores_lista']}\n"
                response += "\n"
                total_general += row['total_profesores']
            
            response += f"Total en el sistema: {total_general} profesores activos"
        else:
            response = "No pude obtener información de profesores por carrera."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTeacherTutors(Action):
    def name(self) -> Text:
        return "action_get_teacher_tutors"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido, g.codigo as grupo,
               c.nombre as carrera, g.cuatrimestre,
               COUNT(ag.alumno_id) as total_alumnos
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN grupos g ON p.id = g.profesor_tutor_id
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE p.activo = TRUE AND g.activo = TRUE
        GROUP BY p.id, g.id
        ORDER BY c.nombre, g.codigo
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay profesores asignados como tutores actualmente."
            else:
                response = f"Profesores tutores ({len(result)} asignaciones):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['numero_empleado']})\n"
                    response += f"    Grupo: {row['grupo']} - {row['cuatrimestre']} cuatrimestre\n"
                    response += f"    Alumnos: {row['total_alumnos']}\n\n"
        else:
            response = "No pude obtener información de profesores tutores."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTeachersLoad(Action):
    def name(self) -> Text:
        return "action_get_teachers_load"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido,
               COUNT(DISTINCT pag.grupo_id) as total_grupos,
               COUNT(DISTINCT pag.asignatura_id) as total_asignaturas,
               GROUP_CONCAT(DISTINCT g.codigo ORDER BY g.codigo) as grupos_lista,
               GROUP_CONCAT(DISTINCT asig.nombre ORDER BY asig.nombre) as asignaturas_lista
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN profesor_asignatura_grupo pag ON p.id = pag.profesor_id
        JOIN grupos g ON pag.grupo_id = g.id
        JOIN asignaturas asig ON pag.asignatura_id = asig.id
        WHERE p.activo = TRUE AND pag.activo = TRUE
        GROUP BY p.id
        ORDER BY (total_grupos + total_asignaturas) DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No encontré asignaciones de profesores."
            else:
                response = "Profesores con mayor carga académica:\n\n"
                for i, row in enumerate(result, 1):
                    carga_total = row['total_grupos'] + row['total_asignaturas']
                    response += f"{i}. {row['nombre']} {row['apellido']} ({row['numero_empleado']})\n"
                    response += f"   Grupos: {row['total_grupos']} | Asignaturas: {row['total_asignaturas']}\n"
                    response += f"   Carga total: {carga_total}\n"
                    response += f"   Grupos: {row['grupos_lista']}\n"
                    if len(row['asignaturas_lista']) > 150:
                        response += f"   Materias: {row['asignaturas_lista'][:150]}...\n\n"
                    else:
                        response += f"   Materias: {row['asignaturas_lista']}\n\n"
        else:
            response = "No pude obtener información de carga académica."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTeachersLowGrades(Action):
    def name(self) -> Text:
        return "action_get_teachers_low_grades"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido,
               COUNT(CASE WHEN ed.calificacion_final <= 7 THEN 1 END) as calificaciones_bajas,
               COUNT(ed.id) as total_calificaciones,
               COUNT(DISTINCT a.matricula) as alumnos_afectados,
               ROUND(COUNT(CASE WHEN ed.calificacion_final <= 7 THEN 1 END) * 100.0 / COUNT(ed.id), 2) as porcentaje_bajas
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN evaluacion_detalle ed ON p.id = ed.profesor_id
        JOIN alumnos a ON ed.alumno_id = a.id
        WHERE ed.calificacion_final IS NOT NULL AND p.activo = TRUE
        GROUP BY p.id
        HAVING calificaciones_bajas > 0
        ORDER BY porcentaje_bajas DESC, calificaciones_bajas DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay profesores con calificaciones menores o iguales a 7."
            else:
                response = "Profesores con mayor índice de calificaciones bajas:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre']} {row['apellido']} ({row['numero_empleado']})\n"
                    response += f"   Calificaciones ≤7: {row['calificaciones_bajas']}\n"
                    response += f"   Total calificaciones: {row['total_calificaciones']}\n"
                    response += f"   Porcentaje bajas: {row['porcentaje_bajas']}%\n"
                    response += f"   Alumnos afectados: {row['alumnos_afectados']}\n\n"
        else:
            response = "No pude obtener información de calificaciones bajas."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetClassroomUsage(Action):
    def name(self) -> Text:
        return "action_get_classroom_usage"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT h.aula, COUNT(*) as frecuencia_uso,
               GROUP_CONCAT(DISTINCT h.dia_semana ORDER BY 
                   CASE h.dia_semana 
                       WHEN 'lunes' THEN 1
                       WHEN 'martes' THEN 2
                       WHEN 'miercoles' THEN 3
                       WHEN 'jueves' THEN 4
                       WHEN 'viernes' THEN 5
                       WHEN 'sabado' THEN 6
                   END) as dias_uso,
               COUNT(DISTINCT CONCAT(h.dia_semana, h.hora_inicio)) as franjas_ocupadas
        FROM horarios h
        WHERE h.activo = TRUE AND h.aula IS NOT NULL AND h.aula != ''
        GROUP BY h.aula
        ORDER BY frecuencia_uso DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No encontré información de uso de aulas."
            else:
                response = "Aulas más utilizadas:\n\n"
                for i, row in enumerate(result, 1):
                    ocupacion_pct = (row['franjas_ocupadas'] / 30) * 100
                    response += f"{i}. Aula {row['aula']}\n"
                    response += f"   Frecuencia de uso: {row['frecuencia_uso']} horarios\n"
                    response += f"   Franjas ocupadas: {row['franjas_ocupadas']}\n"
                    response += f"   Ocupación estimada: {ocupacion_pct:.1f}%\n"
                    response += f"   Días: {row['dias_uso']}\n\n"
        else:
            response = "No pude obtener información de uso de aulas."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetAllGroups(Action):
    def name(self) -> Text:
        return "action_get_all_groups"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT g.codigo, g.cuatrimestre, g.ciclo_escolar, c.nombre as carrera,
               COUNT(ag.alumno_id) as total_alumnos,
               CONCAT(ut.nombre, ' ', ut.apellido) as tutor,
               g.aula
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN profesores p ON g.profesor_tutor_id = p.id
        LEFT JOIN usuarios ut ON p.usuario_id = ut.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE g.activo = TRUE
        GROUP BY g.id
        ORDER BY c.nombre, g.cuatrimestre, g.codigo
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron grupos activos."
            else:
                response = f"Todos los grupos activos ({len(result)} grupos):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['codigo']} ({row['cuatrimestre']} cuatrimestre)\n"
                    response += f"    Ciclo: {row['ciclo_escolar']}\n"
                    response += f"    Alumnos: {row['total_alumnos']}\n"
                    response += f"    Aula: {row['aula'] or 'Sin asignar'}\n"
                    response += f"    Tutor: {row['tutor'] or 'Sin tutor'}\n\n"
        else:
            response = "No pude obtener información de los grupos."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetGroupsByCareer(Action):
    def name(self) -> Text:
        return "action_get_groups_by_career"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT c.nombre as carrera, g.cuatrimestre,
               COUNT(g.id) as total_grupos,
               SUM(CASE WHEN g.activo = TRUE THEN 1 ELSE 0 END) as grupos_activos,
               AVG(alumnos_data.total_alumnos) as promedio_alumnos
        FROM carreras c
        LEFT JOIN grupos g ON c.id = g.carrera_id
        LEFT JOIN (
            SELECT g.id, COUNT(ag.alumno_id) as total_alumnos
            FROM grupos g
            LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
            GROUP BY g.id
        ) alumnos_data ON g.id = alumnos_data.id
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre, g.cuatrimestre
        HAVING total_grupos > 0
        ORDER BY c.nombre, g.cuatrimestre
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            response = "Grupos por carrera y cuatrimestre:\n\n"
            current_career = None
            total_general = 0
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  {row['cuatrimestre']} cuatrimestre: {row['grupos_activos']} grupos activos\n"
                if row['promedio_alumnos']:
                    response += f"    Promedio alumnos: {row['promedio_alumnos']:.1f}\n"
                total_general += row['grupos_activos']
            
            response += f"\nTotal: {total_general} grupos activos"
        else:
            response = "No pude obtener información de grupos por carrera."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetGroupsWithoutTutor(Action):
    def name(self) -> Text:
        return "action_get_groups_without_tutor"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
               g.ciclo_escolar, COUNT(ag.alumno_id) as total_alumnos
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE g.activo = TRUE AND g.profesor_tutor_id IS NULL
        GROUP BY g.id
        ORDER BY c.nombre, g.cuatrimestre
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "Todos los grupos activos tienen tutor asignado."
            else:
                response = f"Grupos sin tutor asignado ({len(result)} grupos):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['codigo']} ({row['cuatrimestre']} cuatrimestre)\n"
                    response += f"    Ciclo: {row['ciclo_escolar']}\n"
                    response += f"    Alumnos: {row['total_alumnos']}\n"
                    response += f"    REQUIERE TUTOR\n\n"
        else:
            response = "No pude obtener información de grupos sin tutor."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetGroupTutor(Action):
    def name(self) -> Text:
        return "action_get_group_tutor"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        grupo = next(tracker.get_latest_entity_values("grupo"), None)
        
        if not grupo:
            dispatcher.utter_message(text="Necesito el código del grupo para consultar su tutor.")
            return []
        
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
               p.numero_empleado, u.nombre, u.apellido,
               COUNT(ag.alumno_id) as total_alumnos,
               p.telefono, p.extension
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN profesores p ON g.profesor_tutor_id = p.id
        LEFT JOIN usuarios u ON p.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE g.codigo = %s AND g.activo = TRUE
        GROUP BY g.id
        """
        result = execute_query(query, (grupo,))
        
        if result and not isinstance(result, dict) and result:
            data = result[0]
            response = f"Información del grupo {data['codigo']}:\n\n"
            response += f"Carrera: {data['carrera']}\n"
            response += f"Cuatrimestre: {data['cuatrimestre']}\n"
            response += f"Total alumnos: {data['total_alumnos']}\n\n"
            
            if data['numero_empleado']:
                response += f"Tutor asignado:\n"
                response += f"  Nombre: {data['nombre']} {data['apellido']}\n"
                response += f"  Número empleado: {data['numero_empleado']}\n"
                if data['telefono']:
                    response += f"  Teléfono: {data['telefono']}\n"
                if data['extension']:
                    response += f"  Extensión: {data['extension']}\n"
            else:
                response += "Este grupo NO TIENE TUTOR ASIGNADO"
        else:
            response = f"No encontré el grupo {grupo}."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTutorGroups(Action):
    def name(self) -> Text:
        return "action_get_tutor_groups"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        numero_empleado = next(tracker.get_latest_entity_values("numero_empleado"), None)
        nombre_profesor = next(tracker.get_latest_entity_values("nombre_profesor"), None)
        
        if numero_empleado:
            query = """
            SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
                   COUNT(ag.alumno_id) as total_alumnos,
                   u.nombre, u.apellido, p.numero_empleado
            FROM profesores p
            JOIN usuarios u ON p.usuario_id = u.id
            JOIN grupos g ON p.id = g.profesor_tutor_id
            JOIN carreras c ON g.carrera_id = c.id
            LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
            WHERE p.numero_empleado = %s AND g.activo = TRUE
            GROUP BY g.id
            ORDER BY c.nombre, g.cuatrimestre
            """
            result = execute_query(query, (numero_empleado,))
        elif nombre_profesor:
            query = """
            SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
                   COUNT(ag.alumno_id) as total_alumnos,
                   u.nombre, u.apellido, p.numero_empleado
            FROM profesores p
            JOIN usuarios u ON p.usuario_id = u.id
            JOIN grupos g ON p.id = g.profesor_tutor_id
            JOIN carreras c ON g.carrera_id = c.id
            LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
            WHERE CONCAT(u.nombre, ' ', u.apellido) LIKE %s AND g.activo = TRUE
            GROUP BY g.id
            ORDER BY c.nombre, g.cuatrimestre
            """
            result = execute_query(query, (f"%{nombre_profesor}%",))
        else:
            dispatcher.utter_message(text="Necesito el número de empleado o nombre del tutor.")
            return []
        
        if result and not isinstance(result, dict):
            if not result:
                response = "Este profesor no tiene grupos asignados como tutor."
            else:
                data = result[0]
                response = f"Grupos del tutor {data['nombre']} {data['apellido']} ({data['numero_empleado']}):\n\n"
                total_estudiantes = 0
                for row in result:
                    response += f"Grupo: {row['codigo']} ({row['cuatrimestre']} cuatrimestre)\n"
                    response += f"  Carrera: {row['carrera']}\n"
                    response += f"  Alumnos: {row['total_alumnos']}\n\n"
                    total_estudiantes += row['total_alumnos']
                
                response += f"Total estudiantes bajo su tutoría: {total_estudiantes}"
        else:
            response = "No pude obtener información de grupos del tutor."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetGroupsRisk(Action):
    def name(self) -> Text:
        return "action_get_groups_risk"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
               COUNT(DISTINCT a.id) as total_alumnos,
               COUNT(rr.id) as total_reportes,
               ROUND(COUNT(rr.id) / COUNT(DISTINCT a.id), 2) as reportes_por_alumno,
               GROUP_CONCAT(DISTINCT rr.tipo_riesgo) as tipos_riesgo
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        JOIN alumnos a ON ag.alumno_id = a.id
        JOIN reportes_riesgo rr ON a.id = rr.alumno_id AND rr.estado IN ('abierto', 'en_proceso')
        WHERE g.activo = TRUE
        GROUP BY g.id
        HAVING total_reportes > 0
        ORDER BY reportes_por_alumno DESC, total_reportes DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay grupos con reportes de riesgo activos."
            else:
                response = "Grupos con mayor número de reportes de riesgo:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. Grupo {row['codigo']} ({row['cuatrimestre']} cuatrimestre)\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Total alumnos: {row['total_alumnos']}\n"
                    response += f"   Reportes activos: {row['total_reportes']}\n"
                    response += f"   Promedio por alumno: {row['reportes_por_alumno']}\n"
                    response += f"   Tipos de riesgo: {row['tipos_riesgo']}\n\n"
        else:
            response = "No pude obtener información de reportes de riesgo."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetNewsViews(Action):
    def name(self) -> Text:
        return "action_get_news_views"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT n.titulo, n.vistas, cn.nombre as categoria, n.fecha_publicacion,
               CONCAT(ud.nombre, ' ', ud.apellido) as autor,
               n.es_destacada
        FROM noticias n
        JOIN categorias_noticias cn ON n.categoria_id = cn.id
        JOIN directivos d ON n.autor_id = d.id
        JOIN usuarios ud ON d.usuario_id = ud.id
        WHERE n.publicada = TRUE
        ORDER BY n.vistas DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron noticias publicadas."
            else:
                response = "Noticias más vistas:\n\n"
                for i, row in enumerate(result, 1):
                    destacada = " ⭐" if row['es_destacada'] else ""
                    response += f"{i}. {row['titulo']}{destacada}\n"
                    response += f"   Autor: {row['autor']}\n"
                    response += f"   Categoría: {row['categoria']}\n"
                    response += f"   Vistas: {row['vistas']}\n"
                    response += f"   Fecha: {row['fecha_publicacion']}\n\n"
        else:
            response = "No pude obtener información de noticias."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetFallback(Action):
    def name(self) -> Text:
        return "action_fallback"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        response = "No entendí completamente tu pregunta. Puedo ayudarte con información sobre:\n\n"
        response += "• Estudiantes (por nombre, matrícula, carrera, estado académico)\n"
        response += "• Profesores (por nombre, número empleado, carga académica)\n"
        response += "• Grupos (información, tutores, horarios, promedios)\n"
        response += "• Calificaciones y rendimiento académico\n"
        response += "• Reportes de riesgo y estudiantes vulnerables\n"
        response += "• Estadísticas generales del sistema\n"
        response += "• Noticias, foro y actividades\n\n"
        response += "Ejemplos de consultas:\n"
        response += "- 'información del alumno Juan Pérez'\n"
        response += "- 'calificaciones del estudiante 2022371156'\n"
        response += "- 'grupos del profesor EMP001'\n"
        response += "- 'estudiantes en riesgo académico'\n\n"
        response += "¿Podrías ser más específico en tu consulta?"
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetFailureAnalysis(Action):
    def name(self) -> Text:
        return "action_get_failure_analysis"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT a.nombre as asignatura, a.codigo,
               COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) as reprobados,
               COUNT(ed.id) as total_evaluaciones,
               ROUND((COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(ed.id)), 2) as porcentaje_reprobacion,
               AVG(ed.calificacion_final) as promedio_general,
               a.complejidad
        FROM asignaturas a
        LEFT JOIN evaluacion_detalle ed ON a.id = ed.asignatura_id
        WHERE a.activa = TRUE
        GROUP BY a.id, a.nombre, a.codigo, a.complejidad
        HAVING total_evaluaciones > 0
        ORDER BY porcentaje_reprobacion DESC, reprobados DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos de reprobación por materia."
            else:
                response = "Análisis de reprobación por materia:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['asignatura']} ({row['codigo']})\n"
                    response += f"   Reprobados: {row['reprobados']}/{row['total_evaluaciones']}\n"
                    response += f"   Porcentaje reprobación: {row['porcentaje_reprobacion']}%\n"
                    response += f"   Promedio general: {row['promedio_general']:.2f}\n"
                    response += f"   Complejidad: {row['complejidad']}/10\n\n"
        else:
            response = "No pude obtener el análisis de reprobación."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTeachersComparison(Action):
    def name(self) -> Text:
        return "action_get_teachers_comparison"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT CONCAT(up.nombre, ' ', up.apellido) as profesor,
               p.numero_empleado,
               COUNT(ed.id) as total_evaluaciones,
               AVG(ed.calificacion_final) as promedio_general,
               COUNT(CASE WHEN ed.estatus = 'aprobado' THEN 1 END) as aprobados,
               COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) as reprobados,
               ROUND((COUNT(CASE WHEN ed.estatus = 'aprobado' THEN 1 END) * 100.0 / COUNT(ed.id)), 2) as porcentaje_aprobacion
        FROM profesores p
        JOIN usuarios up ON p.usuario_id = up.id
        LEFT JOIN evaluacion_detalle ed ON p.id = ed.profesor_id
        WHERE p.activo = TRUE
        GROUP BY p.id, up.nombre, up.apellido, p.numero_empleado
        HAVING total_evaluaciones > 0
        ORDER BY promedio_general DESC, porcentaje_aprobacion DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos para comparar profesores."
            else:
                response = "Comparación de rendimiento entre profesores:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['profesor']} ({row['numero_empleado']})\n"
                    response += f"   Promedio general: {row['promedio_general']:.2f}\n"
                    response += f"   Aprobación: {row['porcentaje_aprobacion']}%\n"
                    response += f"   Evaluaciones: {row['total_evaluaciones']}\n\n"
        else:
            response = "No pude obtener la comparación de profesores."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetStudentTracking(Action):
    def name(self) -> Text:
        return "action_get_student_tracking"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT CONCAT(ua.nombre, ' ', ua.apellido) as alumno,
               a.matricula,
               COUNT(rr.id) as total_reportes,
               MAX(rr.nivel_riesgo) as nivel_maximo,
               COUNT(CASE WHEN rr.estado IN ('abierto', 'en_proceso') THEN 1 END) as reportes_activos,
               MAX(rr.fecha_reporte) as ultimo_reporte,
               GROUP_CONCAT(DISTINCT rr.tipo_riesgo) as tipos_riesgo
        FROM alumnos a
        JOIN usuarios ua ON a.usuario_id = ua.id
        LEFT JOIN reportes_riesgo rr ON a.id = rr.alumno_id
        WHERE a.estado_alumno = 'activo'
        GROUP BY a.id, ua.nombre, ua.apellido, a.matricula
        HAVING total_reportes > 0
        ORDER BY 
            CASE 
                WHEN nivel_maximo = 'critico' THEN 4
                WHEN nivel_maximo = 'alto' THEN 3
                WHEN nivel_maximo = 'medio' THEN 2
                ELSE 1
            END DESC,
            reportes_activos DESC,
            ultimo_reporte DESC
        LIMIT 20
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron estudiantes con seguimiento activo."
            else:
                response = "Estudiantes bajo seguimiento:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['alumno']} ({row['matricula']})\n"
                    response += f"   Nivel máximo: {row['nivel_maximo']}\n"
                    response += f"   Reportes activos: {row['reportes_activos']}/{row['total_reportes']}\n"
                    response += f"   Tipos: {row['tipos_riesgo']}\n"
                    response += f"   Último reporte: {row['ultimo_reporte']}\n\n"
        else:
            response = "No pude obtener información de seguimiento."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetEarlyWarning(Action):
    def name(self) -> Text:
        return "action_get_early_warning"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT CONCAT(ua.nombre, ' ', ua.apellido) as alumno,
               a.matricula,
               c.nombre as carrera,
               AVG(ed.calificacion_final) as promedio_actual,
               COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) as materias_reprobadas,
               COUNT(CASE WHEN ed.calificacion_final < 6 AND ed.estatus = 'cursando' THEN 1 END) as en_riesgo_reprobacion,
               MAX(CASE WHEN rr.nivel_riesgo IN ('alto', 'critico') THEN 1 ELSE 0 END) as tiene_reporte_critico
        FROM alumnos a
        JOIN usuarios ua ON a.usuario_id = ua.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN evaluacion_detalle ed ON a.id = ed.alumno_id
        LEFT JOIN reportes_riesgo rr ON a.id = rr.alumno_id AND rr.estado IN ('abierto', 'en_proceso')
        WHERE a.estado_alumno = 'activo'
        GROUP BY a.id, ua.nombre, ua.apellido, a.matricula, c.nombre
        HAVING (promedio_actual < 7 OR materias_reprobadas >= 2 OR en_riesgo_reprobacion >= 1 OR tiene_reporte_critico = 1)
        ORDER BY 
            tiene_reporte_critico DESC,
            materias_reprobadas DESC,
            promedio_actual ASC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se detectaron estudiantes en alerta temprana."
            else:
                response = "Estudiantes en alerta temprana:\n\n"
                for i, row in enumerate(result, 1):
                    alerta = "🔴" if row['tiene_reporte_critico'] else "🟡"
                    response += f"{alerta} {i}. {row['alumno']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Promedio: {row['promedio_actual']:.2f}\n"
                    response += f"   Materias reprobadas: {row['materias_reprobadas']}\n"
                    response += f"   En riesgo: {row['en_riesgo_reprobacion']} materias\n\n"
        else:
            response = "No pude obtener alertas tempranas."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetSubjectsFailureRate(Action):
    def name(self) -> Text:
        return "action_get_subjects_failure_rate"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT a.nombre as asignatura,
               a.codigo,
               a.cuatrimestre,
               COUNT(ed.id) as total_estudiantes,
               COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) as reprobados,
               ROUND((COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(ed.id)), 2) as tasa_reprobacion,
               AVG(ed.calificacion_final) as promedio,
               MIN(ed.calificacion_final) as calificacion_minima,
               MAX(ed.calificacion_final) as calificacion_maxima
        FROM asignaturas a
        LEFT JOIN evaluacion_detalle ed ON a.id = ed.asignatura_id
        WHERE a.activa = TRUE
        GROUP BY a.id, a.nombre, a.codigo, a.cuatrimestre
        HAVING total_estudiantes >= 5
        ORDER BY tasa_reprobacion DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos de reprobación."
            else:
                response = "Materias con mayor índice de reprobación:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['asignatura']} ({row['codigo']})\n"
                    response += f"   Cuatrimestre: {row['cuatrimestre']}\n"
                    response += f"   Tasa reprobación: {row['tasa_reprobacion']}%\n"
                    response += f"   Reprobados: {row['reprobados']}/{row['total_estudiantes']}\n"
                    response += f"   Promedio: {row['promedio']:.2f}\n\n"
        else:
            response = "No pude obtener las tasas de reprobación."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetProfessorSubjectPerformance(Action):
    def name(self) -> Text:
        return "action_get_professor_subject_performance"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        nombre_profesor = tracker.get_slot('nombre_profesor')
        numero_empleado = tracker.get_slot('numero_empleado')
        asignatura = tracker.get_slot('asignatura')
        
        where_conditions = ["p.activo = TRUE"]
        params = []
        
        if nombre_profesor:
            where_conditions.append("(CONCAT(up.nombre, ' ', up.apellido) LIKE %s)")
            params.append(f"%{nombre_profesor}%")
        
        if numero_empleado:
            where_conditions.append("p.numero_empleado = %s")
            params.append(numero_empleado)
        
        if asignatura:
            where_conditions.append("a.nombre LIKE %s")
            params.append(f"%{asignatura}%")
        
        where_clause = " AND ".join(where_conditions)
        
        query = f"""
        SELECT CONCAT(up.nombre, ' ', up.apellido) as profesor,
               p.numero_empleado,
               a.nombre as asignatura,
               a.codigo as codigo_asignatura,
               COUNT(ed.id) as total_evaluaciones,
               AVG(ed.calificacion_final) as promedio_asignatura,
               COUNT(CASE WHEN ed.estatus = 'aprobado' THEN 1 END) as aprobados,
               COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) as reprobados,
               ROUND((COUNT(CASE WHEN ed.estatus = 'aprobado' THEN 1 END) * 100.0 / COUNT(ed.id)), 2) as porcentaje_aprobacion
        FROM profesores p
        JOIN usuarios up ON p.usuario_id = up.id
        LEFT JOIN evaluacion_detalle ed ON p.id = ed.profesor_id
        LEFT JOIN asignaturas a ON ed.asignatura_id = a.id
        WHERE {where_clause}
        GROUP BY p.id, up.nombre, up.apellido, p.numero_empleado, a.id, a.nombre, a.codigo
        HAVING total_evaluaciones > 0
        ORDER BY profesor, promedio_asignatura DESC
        """
        
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos para el profesor o asignatura especificada."
            else:
                response = "Rendimiento del profesor por asignatura:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['profesor']} ({row['numero_empleado']})\n"
                    response += f"   Asignatura: {row['asignatura']} ({row['codigo_asignatura']})\n"
                    response += f"   Promedio: {row['promedio_asignatura']:.2f}\n"
                    response += f"   Aprobación: {row['porcentaje_aprobacion']}%\n"
                    response += f"   Evaluaciones: {row['total_evaluaciones']}\n\n"
        else:
            response = "No pude obtener el rendimiento del profesor."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetDropoutPrediction(Action):
    def name(self) -> Text:
        return "action_get_dropout_prediction"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT CONCAT(ua.nombre, ' ', ua.apellido) as alumno,
               a.matricula,
               c.nombre as carrera,
               a.cuatrimestre_actual,
               AVG(ed.calificacion_final) as promedio_general,
               COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) as materias_reprobadas,
               COUNT(CASE WHEN rr.nivel_riesgo IN ('alto', 'critico') THEN 1 END) as reportes_criticos,
               DATEDIFF(CURDATE(), a.fecha_ingreso) as dias_desde_ingreso,
               (COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) * 2 + 
                COUNT(CASE WHEN rr.nivel_riesgo = 'critico' THEN 1 END) * 3 +
                COUNT(CASE WHEN rr.nivel_riesgo = 'alto' THEN 1 END) * 2 +
                CASE WHEN AVG(ed.calificacion_final) < 6 THEN 3 
                     WHEN AVG(ed.calificacion_final) < 7 THEN 2 
                     ELSE 0 END) as factor_riesgo
        FROM alumnos a
        JOIN usuarios ua ON a.usuario_id = ua.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN evaluacion_detalle ed ON a.id = ed.alumno_id
        LEFT JOIN reportes_riesgo rr ON a.id = rr.alumno_id AND rr.estado IN ('abierto', 'en_proceso')
        WHERE a.estado_alumno = 'activo'
        GROUP BY a.id, ua.nombre, ua.apellido, a.matricula, c.nombre, a.cuatrimestre_actual, a.fecha_ingreso
        HAVING factor_riesgo >= 4
        ORDER BY factor_riesgo DESC, materias_reprobadas DESC
        LIMIT 20
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se identificaron estudiantes con alto riesgo de deserción."
            else:
                response = "Estudiantes en riesgo de deserción:\n\n"
                for i, row in enumerate(result, 1):
                    riesgo = "CRÍTICO" if row['factor_riesgo'] >= 8 else "ALTO" if row['factor_riesgo'] >= 6 else "MODERADO"
                    response += f"🚨 {i}. {row['alumno']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Cuatrimestre: {row['cuatrimestre_actual']}\n"
                    response += f"   Promedio: {row['promedio_general']:.2f}\n"
                    response += f"   Materias reprobadas: {row['materias_reprobadas']}\n"
                    response += f"   Reportes críticos: {row['reportes_criticos']}\n"
                    response += f"   Nivel de riesgo: {riesgo}\n\n"
        else:
            response = "No pude obtener predicciones de deserción."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetInterventionNeeded(Action):
    def name(self) -> Text:
        return "action_get_intervention_needed"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT CONCAT(ua.nombre, ' ', ua.apellido) as alumno,
               a.matricula,
               c.nombre as carrera,
               COUNT(CASE WHEN rr.nivel_riesgo = 'critico' THEN 1 END) as reportes_criticos,
               COUNT(CASE WHEN rr.estado = 'abierto' THEN 1 END) as reportes_abiertos,
               MAX(rr.fecha_reporte) as ultimo_reporte,
               GROUP_CONCAT(DISTINCT rr.tipo_riesgo) as tipos_riesgo,
               GROUP_CONCAT(DISTINCT rr.acciones_recomendadas SEPARATOR '; ') as acciones_recomendadas,
               AVG(ed.calificacion_final) as promedio_actual
        FROM alumnos a
        JOIN usuarios ua ON a.usuario_id = ua.id
        JOIN carreras c ON a.carrera_id = c.id
        JOIN reportes_riesgo rr ON a.id = rr.alumno_id
        LEFT JOIN evaluacion_detalle ed ON a.id = ed.alumno_id
        WHERE a.estado_alumno = 'activo' 
          AND rr.nivel_riesgo IN ('critico', 'alto')
          AND rr.estado IN ('abierto', 'en_proceso')
        GROUP BY a.id, ua.nombre, ua.apellido, a.matricula, c.nombre
        ORDER BY reportes_criticos DESC, ultimo_reporte DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron estudiantes que requieran intervención urgente."
            else:
                response = "Estudiantes que requieren intervención urgente:\n\n"
                for i, row in enumerate(result, 1):
                    urgencia = "🔴 URGENTE" if row['reportes_criticos'] > 0 else "🟡 PRIORITARIO"
                    response += f"{urgencia} {i}. {row['alumno']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Reportes críticos: {row['reportes_criticos']}\n"
                    response += f"   Reportes abiertos: {row['reportes_abiertos']}\n"
                    response += f"   Tipos de riesgo: {row['tipos_riesgo']}\n"
                    if row['promedio_actual']:
                        response += f"   Promedio actual: {row['promedio_actual']:.2f}\n"
                    response += f"   Último reporte: {row['ultimo_reporte']}\n\n"
        else:
            response = "No pude obtener información de intervenciones necesarias."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetGradeTrends(Action):
    def name(self) -> Text:
        return "action_get_grade_trends"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT ed.ciclo_escolar,
               COUNT(ed.id) as total_evaluaciones,
               AVG(ed.calificacion_final) as promedio_general,
               AVG(ed.parcial_1) as promedio_parcial_1,
               AVG(ed.parcial_2) as promedio_parcial_2,
               AVG(ed.parcial_3) as promedio_parcial_3,
               COUNT(CASE WHEN ed.estatus = 'aprobado' THEN 1 END) as aprobados,
               COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) as reprobados,
               ROUND((COUNT(CASE WHEN ed.estatus = 'aprobado' THEN 1 END) * 100.0 / COUNT(ed.id)), 2) as porcentaje_aprobacion
        FROM evaluacion_detalle ed
        WHERE ed.calificacion_final IS NOT NULL
        GROUP BY ed.ciclo_escolar
        ORDER BY ed.ciclo_escolar DESC
        LIMIT 8
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos de tendencias de calificaciones."
            else:
                response = "Tendencias de calificaciones por período:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. Ciclo {row['ciclo_escolar']}\n"
                    response += f"   Promedio general: {row['promedio_general']:.2f}\n"
                    response += f"   Aprobación: {row['porcentaje_aprobacion']}%\n"
                    response += f"   Evaluaciones: {row['total_evaluaciones']}\n"
                    response += f"   Parciales: P1={row['promedio_parcial_1']:.2f}, P2={row['promedio_parcial_2']:.2f}, P3={row['promedio_parcial_3']:.2f}\n\n"
        else:
            response = "No pude obtener las tendencias de calificaciones."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTeacherEffectiveness(Action):
    def name(self) -> Text:
        return "action_get_teacher_effectiveness"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT CONCAT(up.nombre, ' ', up.apellido) as profesor,
               p.numero_empleado,
               COUNT(DISTINCT ed.asignatura_id) as materias_impartidas,
               COUNT(ed.id) as total_estudiantes,
               AVG(ed.calificacion_final) as promedio_efectividad,
               ROUND((COUNT(CASE WHEN ed.estatus = 'aprobado' THEN 1 END) * 100.0 / COUNT(ed.id)), 2) as tasa_aprobacion,
               COUNT(CASE WHEN ed.calificacion_final >= 9 THEN 1 END) as estudiantes_excelencia,
               COUNT(CASE WHEN ed.estatus = 'extraordinario' THEN 1 END) as estudiantes_extraordinario,
               ROUND((COUNT(CASE WHEN ed.calificacion_final >= 9 THEN 1 END) * 100.0 / COUNT(ed.id)), 2) as porcentaje_excelencia
        FROM profesores p
        JOIN usuarios up ON p.usuario_id = up.id
        LEFT JOIN evaluacion_detalle ed ON p.id = ed.profesor_id
        WHERE p.activo = TRUE
        GROUP BY p.id, up.nombre, up.apellido, p.numero_empleado
        HAVING total_estudiantes >= 10
        ORDER BY promedio_efectividad DESC, tasa_aprobacion DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos de efectividad docente."
            else:
                response = "Efectividad de profesores:\n\n"
                for i, row in enumerate(result, 1):
                    efectividad = "⭐ EXCELENTE" if row['promedio_efectividad'] >= 8.5 else "✅ BUENA" if row['promedio_efectividad'] >= 7.5 else "📈 REGULAR"
                    response += f"{efectividad} {i}. {row['profesor']} ({row['numero_empleado']})\n"
                    response += f"   Promedio de efectividad: {row['promedio_efectividad']:.2f}\n"
                    response += f"   Tasa de aprobación: {row['tasa_aprobacion']}%\n"
                    response += f"   Estudiantes con excelencia: {row['porcentaje_excelencia']}%\n"
                    response += f"   Materias impartidas: {row['materias_impartidas']}\n"
                    response += f"   Total estudiantes: {row['total_estudiantes']}\n\n"
        else:
            response = "No pude obtener datos de efectividad docente."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetCorrelations(Action):
    def name(self) -> Text:
        return "action_get_correlations"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            'Promedio vs Reportes de Riesgo' as analisis,
            AVG(CASE WHEN riesgo_count = 0 THEN promedio END) as promedio_sin_reportes,
            AVG(CASE WHEN riesgo_count > 0 THEN promedio END) as promedio_con_reportes,
            COUNT(CASE WHEN riesgo_count = 0 THEN 1 END) as estudiantes_sin_reportes,
            COUNT(CASE WHEN riesgo_count > 0 THEN 1 END) as estudiantes_con_reportes
        FROM (
            SELECT a.id,
                   AVG(ed.calificacion_final) as promedio,
                   COUNT(rr.id) as riesgo_count
            FROM alumnos a
            LEFT JOIN evaluacion_detalle ed ON a.id = ed.alumno_id
            LEFT JOIN reportes_riesgo rr ON a.id = rr.alumno_id
            WHERE a.estado_alumno = 'activo'
            GROUP BY a.id
            HAVING promedio IS NOT NULL
        ) as correlacion
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se pudieron calcular correlaciones."
            else:
                row = result[0]
                response = "Análisis de correlaciones:\n\n"
                response += f"📊 {row['analisis']}:\n"
                response += f"   Promedio sin reportes: {row['promedio_sin_reportes']:.2f}\n"
                response += f"   Promedio con reportes: {row['promedio_con_reportes']:.2f}\n"
                response += f"   Estudiantes sin reportes: {row['estudiantes_sin_reportes']}\n"
                response += f"   Estudiantes con reportes: {row['estudiantes_con_reportes']}\n"
                
                diferencia = row['promedio_sin_reportes'] - row['promedio_con_reportes']
                response += f"   Diferencia: {diferencia:.2f} puntos\n\n"
                
                if diferencia > 1:
                    response += "✅ Los estudiantes sin reportes de riesgo tienen mejor rendimiento académico."
                else:
                    response += "⚠️ La correlación entre reportes de riesgo y calificaciones es débil."
        else:
            response = "No pude calcular las correlaciones."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetStudentsByPeriod(Action):
    def name(self) -> Text:
        return "action_get_students_by_period"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        periodo = tracker.get_slot('periodo')
        
        where_condition = ""
        params = []
        
        if periodo:
            where_condition = "AND ed.ciclo_escolar LIKE %s"
            params.append(f"%{periodo}%")
        
        query = f"""
        SELECT CONCAT(ua.nombre, ' ', ua.apellido) as alumno,
               a.matricula,
               c.nombre as carrera,
               ed.ciclo_escolar,
               AVG(ed.calificacion_final) as promedio_periodo,
               COUNT(ed.id) as materias_cursadas,
               COUNT(CASE WHEN ed.estatus = 'aprobado' THEN 1 END) as aprobadas,
               COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) as reprobadas
        FROM alumnos a
        JOIN usuarios ua ON a.usuario_id = ua.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN evaluacion_detalle ed ON a.id = ed.alumno_id
        WHERE a.estado_alumno = 'activo' {where_condition}
        GROUP BY a.id, ua.nombre, ua.apellido, a.matricula, c.nombre, ed.ciclo_escolar
        HAVING materias_cursadas > 0
        ORDER BY promedio_periodo DESC
        LIMIT 20
        """
        
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontraron estudiantes para el período especificado."
            else:
                response = f"Estudiantes del período {periodo or 'seleccionado'}:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['alumno']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Ciclo: {row['ciclo_escolar']}\n"
                    response += f"   Promedio: {row['promedio_periodo']:.2f}\n"
                    response += f"   Materias: {row['aprobadas']} aprobadas, {row['reprobadas']} reprobadas\n\n"
        else:
            response = "No pude obtener información del período."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetGradeRangeStudents(Action):
    def name(self) -> Text:
        return "action_get_grade_range_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        calificacion_minima = tracker.get_slot('calificacion_minima')
        calificacion_maxima = tracker.get_slot('calificacion_maxima')
        rango_calificacion = tracker.get_slot('rango_calificacion')
        calificacion_exacta = tracker.get_slot('calificacion_exacta')
        
        where_conditions = []
        params = []
        
        if calificacion_exacta:
            where_conditions.append("AVG(ed.calificacion_final) = %s")
            params.append(float(calificacion_exacta))
        elif rango_calificacion:
            if '-' in rango_calificacion or 'entre' in rango_calificacion.lower():
                import re
                numeros = re.findall(r'\d+(?:\.\d+)?', rango_calificacion)
                if len(numeros) >= 2:
                    where_conditions.append("AVG(ed.calificacion_final) BETWEEN %s AND %s")
                    params.extend([float(numeros[0]), float(numeros[1])])
        else:
            if calificacion_minima:
                where_conditions.append("AVG(ed.calificacion_final) >= %s")
                params.append(float(calificacion_minima))
            if calificacion_maxima:
                where_conditions.append("AVG(ed.calificacion_final) <= %s")
                params.append(float(calificacion_maxima))
        
        where_clause = ""
        if where_conditions:
            where_clause = "HAVING " + " AND ".join(where_conditions)
        
        query = f"""
        SELECT CONCAT(ua.nombre, ' ', ua.apellido) as alumno,
               a.matricula,
               c.nombre as carrera,
               a.cuatrimestre_actual,
               AVG(ed.calificacion_final) as promedio_general,
               COUNT(ed.id) as total_materias,
               COUNT(CASE WHEN ed.estatus = 'aprobado' THEN 1 END) as aprobadas,
               COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) as reprobadas
        FROM alumnos a
        JOIN usuarios ua ON a.usuario_id = ua.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN evaluacion_detalle ed ON a.id = ed.alumno_id
        WHERE a.estado_alumno = 'activo' AND ed.calificacion_final IS NOT NULL
        GROUP BY a.id, ua.nombre, ua.apellido, a.matricula, c.nombre, a.cuatrimestre_actual
        {where_clause}
        ORDER BY promedio_general DESC
        LIMIT 25
        """
        
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron estudiantes en el rango de calificaciones especificado."
            else:
                response = "Estudiantes en el rango de calificaciones:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['alumno']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Cuatrimestre: {row['cuatrimestre_actual']}\n"
                    response += f"   Promedio: {row['promedio_general']:.2f}\n"
                    response += f"   Materias: {row['aprobadas']} aprobadas, {row['reprobadas']} reprobadas\n\n"
        else:
            response = "No pude obtener estudiantes en el rango especificado."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetSpecificRiskType(Action):
    def name(self) -> Text:
        return "action_get_specific_risk_type"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        tipo_riesgo = tracker.get_slot('tipo_riesgo')
        
        where_condition = ""
        params = []
        
        if tipo_riesgo:
            where_condition = "AND rr.tipo_riesgo = %s"
            params.append(tipo_riesgo)
        
        query = f"""
        SELECT CONCAT(ua.nombre, ' ', ua.apellido) as alumno,
               a.matricula,
               c.nombre as carrera,
               rr.tipo_riesgo,
               rr.nivel_riesgo,
               rr.descripcion,
               rr.acciones_recomendadas,
               rr.estado,
               rr.fecha_reporte,
               AVG(ed.calificacion_final) as promedio_actual
        FROM alumnos a
        JOIN usuarios ua ON a.usuario_id = ua.id
        JOIN carreras c ON a.carrera_id = c.id
        JOIN reportes_riesgo rr ON a.id = rr.alumno_id
        LEFT JOIN evaluacion_detalle ed ON a.id = ed.alumno_id
        WHERE a.estado_alumno = 'activo' 
          AND rr.estado IN ('abierto', 'en_proceso') {where_condition}
        GROUP BY a.id, ua.nombre, ua.apellido, a.matricula, c.nombre, rr.id, 
                 rr.tipo_riesgo, rr.nivel_riesgo, rr.descripcion, rr.acciones_recomendadas,
                 rr.estado, rr.fecha_reporte
        ORDER BY 
            CASE rr.nivel_riesgo
                WHEN 'critico' THEN 4
                WHEN 'alto' THEN 3
                WHEN 'medio' THEN 2
                ELSE 1
            END DESC,
            rr.fecha_reporte DESC
        LIMIT 20
        """
        
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontraron estudiantes con riesgo {tipo_riesgo or 'del tipo especificado'}."
            else:
                response = f"Estudiantes con riesgo {tipo_riesgo or 'específico'}:\n\n"
                for i, row in enumerate(result, 1):
                    nivel_emoji = {"critico": "🔴", "alto": "🟠", "medio": "🟡", "bajo": "🟢"}.get(row['nivel_riesgo'], "⚪")
                    response += f"{nivel_emoji} {i}. {row['alumno']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Tipo: {row['tipo_riesgo']} - Nivel: {row['nivel_riesgo']}\n"
                    response += f"   Estado: {row['estado']}\n"
                    if row['promedio_actual']:
                        response += f"   Promedio: {row['promedio_actual']:.2f}\n"
                    response += f"   Fecha reporte: {row['fecha_reporte']}\n"
                    response += f"   Descripción: {row['descripcion'][:100]}...\n\n"
        else:
            response = "No pude obtener estudiantes con el tipo de riesgo especificado."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetSubjectSpecific(Action):
    def name(self) -> Text:
        return "action_get_subject_specific"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        asignatura = tracker.get_slot('asignatura')
        
        where_condition = ""
        params = []
        
        if asignatura:
            where_condition = "AND a.nombre LIKE %s"
            params.append(f"%{asignatura}%")
        
        query = f"""
        SELECT a.nombre as asignatura,
               a.codigo,
               a.cuatrimestre,
               COUNT(ed.id) as total_estudiantes,
               AVG(ed.calificacion_final) as promedio_asignatura,
               COUNT(CASE WHEN ed.estatus = 'aprobado' THEN 1 END) as aprobados,
               COUNT(CASE WHEN ed.estatus = 'reprobado' THEN 1 END) as reprobados,
               COUNT(CASE WHEN ed.estatus = 'extraordinario' THEN 1 END) as extraordinarios,
               ROUND((COUNT(CASE WHEN ed.estatus = 'aprobado' THEN 1 END) * 100.0 / COUNT(ed.id)), 2) as porcentaje_aprobacion,
               MIN(ed.calificacion_final) as calificacion_minima,
               MAX(ed.calificacion_final) as calificacion_maxima,
               CONCAT(up.nombre, ' ', up.apellido) as mejor_profesor,
               MAX(prom_prof.promedio_profesor) as mejor_promedio_profesor
        FROM asignaturas a
        LEFT JOIN evaluacion_detalle ed ON a.id = ed.asignatura_id
        LEFT JOIN (
            SELECT ed2.asignatura_id, ed2.profesor_id, AVG(ed2.calificacion_final) as promedio_profesor
            FROM evaluacion_detalle ed2
            GROUP BY ed2.asignatura_id, ed2.profesor_id
        ) prom_prof ON a.id = prom_prof.asignatura_id
        LEFT JOIN profesores p ON prom_prof.profesor_id = p.id
        LEFT JOIN usuarios up ON p.usuario_id = up.id
        WHERE a.activa = TRUE {where_condition}
        GROUP BY a.id, a.nombre, a.codigo, a.cuatrimestre, up.nombre, up.apellido, prom_prof.promedio_profesor
        HAVING total_estudiantes > 0
        ORDER BY a.nombre, promedio_asignatura DESC
        LIMIT 15
        """
        
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontraron datos para la asignatura {asignatura or 'especificada'}."
            else:
                response = f"Información de la asignatura {asignatura or 'solicitada'}:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['asignatura']} ({row['codigo']})\n"
                    response += f"   Cuatrimestre: {row['cuatrimestre']}\n"
                    response += f"   Promedio general: {row['promedio_asignatura']:.2f}\n"
                    response += f"   Aprobación: {row['porcentaje_aprobacion']}%\n"
                    response += f"   Estudiantes: {row['total_estudiantes']}\n"
                    response += f"   Resultados: {row['aprobados']} aprobados, {row['reprobados']} reprobados\n"
                    if row['extraordinarios'] > 0:
                        response += f"   Extraordinarios: {row['extraordinarios']}\n"
                    response += f"   Rango: {row['calificacion_minima']:.1f} - {row['calificacion_maxima']:.1f}\n"
                    if row['mejor_profesor']:
                        response += f"   Mejor profesor: {row['mejor_profesor']} ({row['mejor_promedio_profesor']:.2f})\n"
                    response += "\n"
        else:
            response = "No pude obtener información de la asignatura."
        
        dispatcher.utter_message(text=response)
        return []
class ActionGetGraduatedStudents(Action):
    def name(self) -> Text:
        return "action_get_graduated_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula, c.nombre as carrera,
               a.cuatrimestre_actual, a.fecha_ingreso
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        WHERE a.estado_alumno = 'egresado'
        ORDER BY c.nombre, u.apellido
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay alumnos egresados registrados."
            else:
                response = f"Alumnos egresados ({len(result)} casos):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"    Último cuatrimestre: {row['cuatrimestre_actual']}\n"
                    response += f"    Fecha ingreso: {row['fecha_ingreso']}\n\n"
        else:
            response = "No pude obtener información de estudiantes egresados."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTemporaryDropoutStudents(Action):
    def name(self) -> Text:
        return "action_get_temporary_dropout_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula, c.nombre as carrera,
               a.cuatrimestre_actual, a.fecha_ingreso
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        WHERE a.estado_alumno = 'baja_temporal'
        ORDER BY c.nombre, u.apellido
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay alumnos con baja temporal."
            else:
                response = f"Alumnos con baja temporal ({len(result)} casos):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"    Último cuatrimestre: {row['cuatrimestre_actual']}\n"
                    response += f"    Fecha ingreso: {row['fecha_ingreso']}\n\n"
        else:
            response = "No pude obtener información de estudiantes con baja temporal."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetDefinitiveDropoutStudents(Action):
    def name(self) -> Text:
        return "action_get_definitive_dropout_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula, c.nombre as carrera,
               a.cuatrimestre_actual, a.fecha_ingreso
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        WHERE a.estado_alumno = 'baja_definitiva'
        ORDER BY c.nombre, u.apellido
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay alumnos con baja definitiva."
            else:
                response = f"Alumnos con baja definitiva ({len(result)} casos):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"    Último cuatrimestre: {row['cuatrimestre_actual']}\n"
                    response += f"    Fecha ingreso: {row['fecha_ingreso']}\n\n"
        else:
            response = "No pude obtener información de estudiantes con baja definitiva."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetActiveStudentsOnly(Action):
    def name(self) -> Text:
        return "action_get_active_students_only"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula, c.nombre as carrera,
               a.cuatrimestre_actual, a.fecha_ingreso
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        WHERE a.estado_alumno = 'activo'
        ORDER BY c.nombre, a.cuatrimestre_actual, u.apellido
        LIMIT 50
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay alumnos activos registrados."
            else:
                response = f"Alumnos activos ({len(result)} casos):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"    Cuatrimestre: {row['cuatrimestre_actual']}\n"
                    response += f"    Fecha ingreso: {row['fecha_ingreso']}\n\n"
        else:
            response = "No pude obtener información de estudiantes activos."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetStudentsBySpecificCareer(Action):
    def name(self) -> Text:
        return "action_get_students_by_specific_career"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        carrera_nombre = tracker.get_slot('carrera')
        
        if not carrera_nombre:
            dispatcher.utter_message(text="Necesito que especifiques de qué carrera quieres consultar los estudiantes.")
            return []
        
        query = """
        SELECT CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
               a.matricula,
               c.nombre as carrera,
               a.cuatrimestre_actual,
               a.fecha_ingreso,
               a.estado_alumno,
               AVG(ed.calificacion_final) as promedio_general
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN evaluacion_detalle ed ON a.id = ed.alumno_id
        WHERE c.nombre LIKE %s AND u.activo = TRUE
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, a.cuatrimestre_actual, a.fecha_ingreso, a.estado_alumno
        ORDER BY a.cuatrimestre_actual, promedio_general DESC
        LIMIT 30
        """
        
        result = execute_query(query, (f"%{carrera_nombre}%",))
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontraron estudiantes en la carrera {carrera_nombre}."
            else:
                response = f"Estudiantes de {carrera_nombre} ({len(result)} casos):\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre_completo']} ({row['matricula']})\n"
                    response += f"   Estado: {row['estado_alumno']}\n"
                    response += f"   Cuatrimestre: {row['cuatrimestre_actual']}\n"
                    if row['promedio_general']:
                        response += f"   Promedio: {row['promedio_general']:.2f}\n"
                    response += f"   Fecha ingreso: {row['fecha_ingreso']}\n\n"
        else:
            response = f"No pude obtener información de estudiantes de {carrera_nombre}."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetCriticalRiskStudents(Action):
    def name(self) -> Text:
        return "action_get_critical_risk_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula,
               COUNT(rr.id) as total_reportes,
               GROUP_CONCAT(DISTINCT rr.tipo_riesgo) as tipos_riesgo,
               c.nombre as carrera, g.codigo as grupo
        FROM reportes_riesgo rr
        JOIN alumnos a ON rr.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN carreras c ON g.carrera_id = c.id
        WHERE rr.estado IN ('abierto', 'en_proceso') AND a.estado_alumno = 'activo'
              AND rr.nivel_riesgo = 'critico'
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo
        ORDER BY total_reportes DESC, u.apellido
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay estudiantes con reportes de riesgo critico activos."
            else:
                response = f"Estudiantes con riesgo critico ({len(result)} casos):\n\n"
                for row in result:
                    response += f"{row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"  Carrera: {row['carrera'] or 'Sin carrera'}\n"
                    response += f"  Grupo: {row['grupo'] or 'Sin grupo'}\n"
                    response += f"  Total reportes criticos: {row['total_reportes']}\n"
                    response += f"  Tipos de riesgo: {row['tipos_riesgo']}\n\n"
        else:
            response = "No pude obtener información de reportes de riesgo critico."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetHighRiskStudentsOnly(Action):
    def name(self) -> Text:
        return "action_get_high_risk_students_only"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula,
               COUNT(rr.id) as total_reportes,
               GROUP_CONCAT(DISTINCT rr.tipo_riesgo) as tipos_riesgo,
               c.nombre as carrera, g.codigo as grupo
        FROM reportes_riesgo rr
        JOIN alumnos a ON rr.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN carreras c ON g.carrera_id = c.id
        WHERE rr.estado IN ('abierto', 'en_proceso') AND a.estado_alumno = 'activo'
              AND rr.nivel_riesgo = 'alto'
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo
        ORDER BY total_reportes DESC, u.apellido
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay estudiantes con reportes de riesgo alto activos."
            else:
                response = f"Estudiantes con riesgo alto ({len(result)} casos):\n\n"
                for row in result:
                    response += f"{row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"  Carrera: {row['carrera'] or 'Sin carrera'}\n"
                    response += f"  Grupo: {row['grupo'] or 'Sin grupo'}\n"
                    response += f"  Total reportes altos: {row['total_reportes']}\n"
                    response += f"  Tipos de riesgo: {row['tipos_riesgo']}\n\n"
        else:
            response = "No pude obtener información de reportes de riesgo alto."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetMediumRiskStudents(Action):
    def name(self) -> Text:
        return "action_get_medium_risk_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula,
               COUNT(rr.id) as total_reportes,
               GROUP_CONCAT(DISTINCT rr.tipo_riesgo) as tipos_riesgo,
               c.nombre as carrera, g.codigo as grupo
        FROM reportes_riesgo rr
        JOIN alumnos a ON rr.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN carreras c ON g.carrera_id = c.id
        WHERE rr.estado IN ('abierto', 'en_proceso') AND a.estado_alumno = 'activo'
              AND rr.nivel_riesgo = 'medio'
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo
        ORDER BY total_reportes DESC, u.apellido
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay estudiantes con reportes de riesgo medio activos."
            else:
                response = f"Estudiantes con riesgo medio ({len(result)} casos):\n\n"
                for row in result:
                    response += f"{row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"  Carrera: {row['carrera'] or 'Sin carrera'}\n"
                    response += f"  Grupo: {row['grupo'] or 'Sin grupo'}\n"
                    response += f"  Total reportes medios: {row['total_reportes']}\n"
                    response += f"  Tipos de riesgo: {row['tipos_riesgo']}\n\n"
        else:
            response = "No pude obtener información de reportes de riesgo medio."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetFailingStudentsOnly(Action):
    def name(self) -> Text:
        return "action_get_failing_students_only"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT DISTINCT u.nombre, u.apellido, a.matricula,
               asig.nombre as asignatura, ed.calificacion_final,
               c.nombre as carrera, g.codigo as grupo
        FROM evaluacion_detalle ed
        JOIN alumnos a ON ed.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN asignaturas asig ON ed.asignatura_id = asig.id
        JOIN grupos g ON ed.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        WHERE ed.estatus = 'reprobado' AND a.estado_alumno = 'activo'
        ORDER BY ed.calificacion_final ASC, u.apellido
        LIMIT 30
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay alumnos reprobados."
            else:
                response = f"Alumnos reprobados ({len(result)} casos):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"    Grupo: {row['grupo']}\n"
                    response += f"    Asignatura: {row['asignatura']}\n"
                    response += f"    Calificación: {row['calificacion_final']}\n\n"
        else:
            response = "No pude obtener información de estudiantes reprobados."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetLowGradeStudentsSpecific(Action):
    def name(self) -> Text:
        return "action_get_low_grade_students_specific"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT DISTINCT u.nombre, u.apellido, a.matricula,
               asig.nombre as asignatura, ed.calificacion_final,
               c.nombre as carrera, g.codigo as grupo
        FROM evaluacion_detalle ed
        JOIN alumnos a ON ed.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN asignaturas asig ON ed.asignatura_id = asig.id
        JOIN grupos g ON ed.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        WHERE ed.calificacion_final < 8 AND ed.calificacion_final IS NOT NULL
              AND a.estado_alumno = 'activo' AND ed.estatus != 'reprobado'
        ORDER BY ed.calificacion_final ASC, u.apellido
        LIMIT 30
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay alumnos con calificaciones menores a 8 que no esten reprobados."
            else:
                response = f"Alumnos con calificaciones bajas ({len(result)} casos):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"    Grupo: {row['grupo']}\n"
                    response += f"    Asignatura: {row['asignatura']}\n"
                    response += f"    Calificación: {row['calificacion_final']}\n\n"
        else:
            response = "No pude obtener información de calificaciones bajas."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetSocioeconomicVulnerableStudents(Action):
    def name(self) -> Text:
        return "action_get_socioeconomic_vulnerable_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula, c.nombre as carrera,
               COUNT(DISTINCT re.pregunta_id) as vulnerabilidades_identificadas,
               GROUP_CONCAT(DISTINCT pe.pregunta SEPARATOR '; ') as tipos_vulnerabilidad
        FROM respuestas_encuesta re
        JOIN alumnos a ON re.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        JOIN preguntas_encuesta pe ON re.pregunta_id = pe.id
        JOIN encuestas e ON pe.encuesta_id = e.id
        WHERE e.tipo_encuesta = 'socioeconomica' AND a.estado_alumno = 'activo'
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre
        ORDER BY vulnerabilidades_identificadas DESC
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron estudiantes con vulnerabilidades socioeconomicas identificadas."
            else:
                response = f"Estudiantes con vulnerabilidad socioeconomica ({len(result)} casos):\n\n"
                for row in result:
                    response += f"{row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"  Carrera: {row['carrera']}\n"
                    response += f"  Vulnerabilidades: {row['vulnerabilidades_identificadas']}\n"
                    if len(row['tipos_vulnerabilidad']) > 200:
                        response += f"  Tipos: {row['tipos_vulnerabilidad'][:200]}...\n\n"
                    else:
                        response += f"  Tipos: {row['tipos_vulnerabilidad']}\n\n"
        else:
            response = "No pude obtener información de estudiantes con vulnerabilidad socioeconomica."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetAcademicVulnerableStudents(Action):
    def name(self) -> Text:
        return "action_get_academic_vulnerable_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT COUNT(*) as total_registros 
        FROM ultima_oportunidad_log
        """
        count_result = execute_query(query)
        
        if count_result and count_result[0]['total_registros'] == 0:
            dispatcher.utter_message(text="No hay registros en la tabla ultima_oportunidad_log.")
            return []
        
        query = """
        SELECT 
            uol.id,
            uol.alumno_id,
            uol.asignatura_id,
            uol.numero_parcial,
            uol.calificacion,
            uol.resultado,
            uol.fecha_uso,
            a.matricula,
            u.nombre,
            u.apellido,
            tutor_u.nombre as tutor_nombre,
            tutor_u.apellido as tutor_apellido,
            tutor_p.numero_empleado as tutor_empleado
        FROM ultima_oportunidad_log uol
        LEFT JOIN alumnos a ON uol.alumno_id = a.id
        LEFT JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id AND g.activo = TRUE
        LEFT JOIN profesores tutor_p ON g.profesor_tutor_id = tutor_p.id
        LEFT JOIN usuarios tutor_u ON tutor_p.usuario_id = tutor_u.id
        ORDER BY uol.fecha_uso DESC
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if len(result) == 0:
                response = "La tabla ultima_oportunidad_log esta vacia."
            else:
                response = f"Alumnos vulnerables academicamente ({len(result)} casos):\n\n"
                for row in result:
                    if row.get('matricula') and row.get('nombre'):
                        response += f"Matricula: {row['matricula']}\n"
                        response += f"Nombre: {row['nombre']} {row['apellido']}\n"
                        response += f"Parcial: {row['numero_parcial']}\n"
                        response += f"Calificacion: {row['calificacion']}\n"
                        if row.get('tutor_nombre') and row.get('tutor_apellido'):
                            response += f"Tutor del grupo: {row['tutor_nombre']} {row['tutor_apellido']}"
                            if row.get('tutor_empleado'):
                                response += f" (matricula: {row['tutor_empleado']})"
                            response += "\n"
                        else:
                            response += "Tutor del grupo: No asignado\n"
                        response += f"Categoria: Academica\n\n"
                    else:
                        response += f"ID Log: {row['id']}\n"
                        response += f"Alumno ID: {row['alumno_id']}\n"
                        response += f"Asignatura ID: {row['asignatura_id']}\n"
                        response += f"Parcial: {row['numero_parcial']}\n"
                        response += f"Calificacion: {row['calificacion']}\n"
                        if row.get('tutor_nombre') and row.get('tutor_apellido'):
                            response += f"Tutor del grupo: {row['tutor_nombre']} {row['tutor_apellido']}"
                            if row.get('tutor_empleado'):
                                response += f" (matricula: {row['tutor_empleado']})"
                            response += "\n"
                        else:
                            response += "Tutor del grupo: No asignado\n"
                        response += f"Categoria: Academica\n\n"
        else:
            response = "No pude obtener informacion de estudiantes con vulnerabilidad academica."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetExcellentStudents(Action):
    def name(self) -> Text:
        return "action_get_excellent_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula, c.nombre as carrera,
               AVG(ed.calificacion_final) as promedio,
               COUNT(ed.id) as materias_evaluadas,
               g.codigo as grupo
        FROM evaluacion_detalle ed
        JOIN alumnos a ON ed.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN grupos g ON ed.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        WHERE ed.calificacion_final IS NOT NULL AND a.estado_alumno = 'activo'
              AND ed.calificacion_final >= 9.5
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo
        HAVING materias_evaluadas >= 3 AND promedio >= 9.5
        ORDER BY promedio DESC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No encontre estudiantes con excelencia academica."
            else:
                response = f"Estudiantes con excelencia academica ({len(result)} casos):\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"   Promedio: {row['promedio']:.2f}\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Grupo: {row['grupo']}\n"
                    response += f"   Materias evaluadas: {row['materias_evaluadas']}\n\n"
        else:
            response = "No pude obtener información de estudiantes con excelencia."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetGoodStudents(Action):
    def name(self) -> Text:
        return "action_get_good_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT u.nombre, u.apellido, a.matricula, c.nombre as carrera,
               AVG(ed.calificacion_final) as promedio,
               COUNT(ed.id) as materias_evaluadas,
               g.codigo as grupo
        FROM evaluacion_detalle ed
        JOIN alumnos a ON ed.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN grupos g ON ed.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        WHERE ed.calificacion_final IS NOT NULL AND a.estado_alumno = 'activo'
              AND ed.calificacion_final >= 8 AND ed.calificacion_final < 9.5
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo
        HAVING materias_evaluadas >= 3 AND promedio >= 8 AND promedio < 9.5
        ORDER BY promedio DESC
        LIMIT 20
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No encontre estudiantes con buen rendimiento academico."
            else:
                response = f"Estudiantes con buen rendimiento ({len(result)} casos):\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"   Promedio: {row['promedio']:.2f}\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Grupo: {row['grupo']}\n"
                    response += f"   Materias evaluadas: {row['materias_evaluadas']}\n\n"
        else:
            response = "No pude obtener información de estudiantes con buen rendimiento."
        
        dispatcher.utter_message(text=response)
        return []
    
class ActionGetVulnerableStudents(Action):
    def name(self) -> Text:
        return "action_get_vulnerable_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        response = ""
        
        # Académicos desde ultima_oportunidad_log
        query = """
        SELECT COUNT(*) as total_registros 
        FROM ultima_oportunidad_log
        """
        count_result = execute_query(query)
        
        if count_result and count_result[0]['total_registros'] == 0:
            response += "No hay registros en la tabla ultima_oportunidad_log.\n\n"
        else:
            query = """
            SELECT 
                uol.id,
                uol.alumno_id,
                uol.asignatura_id,
                uol.numero_parcial,
                uol.calificacion,
                uol.resultado,
                uol.fecha_uso,
                a.matricula,
                u.nombre,
                u.apellido,
                tutor_u.nombre as tutor_nombre,
                tutor_u.apellido as tutor_apellido,
                tutor_p.numero_empleado as tutor_empleado
            FROM ultima_oportunidad_log uol
            LEFT JOIN alumnos a ON uol.alumno_id = a.id
            LEFT JOIN usuarios u ON a.usuario_id = u.id
            LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
            LEFT JOIN grupos g ON ag.grupo_id = g.id AND g.activo = TRUE
            LEFT JOIN profesores tutor_p ON g.profesor_tutor_id = tutor_p.id
            LEFT JOIN usuarios tutor_u ON tutor_p.usuario_id = tutor_u.id
            ORDER BY uol.fecha_uso DESC
            """
            result = execute_query(query)
            
            if result and not isinstance(result, dict):
                if len(result) == 0:
                    response += "La tabla ultima_oportunidad_log esta vacia.\n\n"
                else:
                    response += f"Alumnos vulnerables academicamente ({len(result)} casos):\n\n"
                    for row in result:
                        if row.get('matricula') and row.get('nombre'):
                            response += f"Matricula: {row['matricula']}\n"
                            response += f"Nombre: {row['nombre']} {row['apellido']}\n"
                            response += f"Parcial: {row['numero_parcial']}\n"
                            response += f"Calificacion: {row['calificacion']}\n"
                            if row.get('tutor_nombre') and row.get('tutor_apellido'):
                                response += f"Tutor del grupo: {row['tutor_nombre']} {row['tutor_apellido']}"
                                if row.get('tutor_empleado'):
                                    response += f" (matricula: {row['tutor_empleado']})"
                                response += "\n"
                            else:
                                response += "Tutor del grupo: No asignado\n"
                            response += f"Categoria: Academica\n\n"
                        else:
                            response += f"ID Log: {row['id']}\n"
                            response += f"Alumno ID: {row['alumno_id']}\n"
                            response += f"Asignatura ID: {row['asignatura_id']}\n"
                            response += f"Parcial: {row['numero_parcial']}\n"
                            response += f"Calificacion: {row['calificacion']}\n"
                            if row.get('tutor_nombre') and row.get('tutor_apellido'):
                                response += f"Tutor del grupo: {row['tutor_nombre']} {row['tutor_apellido']}"
                                if row.get('tutor_empleado'):
                                    response += f" (matricula: {row['tutor_empleado']})"
                                response += "\n"
                            else:
                                response += "Tutor del grupo: No asignado\n"
                            response += f"Categoria: Academica\n\n"

        # Económicos desde reportes_riesgo
        query_economicos = """
        SELECT 
            a.matricula,
            u.nombre,
            u.apellido,
            rr.descripcion AS motivo,
            tutor_u.nombre AS tutor_nombre,
            tutor_u.apellido AS tutor_apellido,
            tutor_p.numero_empleado AS tutor_empleado
        FROM reportes_riesgo rr
        JOIN alumnos a ON rr.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id AND g.activo = TRUE
        LEFT JOIN profesores tutor_p ON g.profesor_tutor_id = tutor_p.id
        LEFT JOIN usuarios tutor_u ON tutor_p.usuario_id = tutor_u.id
        WHERE rr.tipo_riesgo = 'economico'
        ORDER BY rr.nivel_riesgo DESC
        """
        economicos = execute_query(query_economicos)
        
        if economicos and not isinstance(economicos, dict):
            if len(economicos) > 0:
                response += f"Alumnos vulnerables economicamente ({len(economicos)} casos):\n\n"
                for e in economicos:
                    response += f"Matricula: {e['matricula']}\n"
                    response += f"Nombre: {e['nombre']} {e['apellido']}\n"
                    response += f"Motivo: {e['motivo']}\n"
                    if e.get('tutor_nombre') and e.get('tutor_apellido'):
                        response += f"Tutor del grupo: {e['tutor_nombre']} {e['tutor_apellido']}"
                        if e.get('tutor_empleado'):
                            response += f" (matricula: {e['tutor_empleado']})"
                        response += "\n"
                    else:
                        response += "Tutor del grupo: No asignado\n"
                    response += f"Categoria: Economica\n\n"
            else:
                response += "No hay registros economicos en la base de datos.\n\n"
        else:
            response += "No hay registros economicos en la base de datos.\n\n"

        # Familiares desde reportes_riesgo
        query_familiares = """
        SELECT 
            a.matricula,
            u.nombre,
            u.apellido,
            rr.descripcion AS motivo,
            tutor_u.nombre AS tutor_nombre,
            tutor_u.apellido AS tutor_apellido,
            tutor_p.numero_empleado AS tutor_empleado
        FROM reportes_riesgo rr
        JOIN alumnos a ON rr.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id AND g.activo = TRUE
        LEFT JOIN profesores tutor_p ON g.profesor_tutor_id = tutor_p.id
        LEFT JOIN usuarios tutor_u ON tutor_p.usuario_id = tutor_u.id
        WHERE rr.tipo_riesgo = 'familiar'
        ORDER BY rr.nivel_riesgo DESC
        """
        familiares = execute_query(query_familiares)
        
        if familiares and not isinstance(familiares, dict):
            if len(familiares) > 0:
                response += f"Alumnos vulnerables familiarmente ({len(familiares)} casos):\n\n"
                for f in familiares:
                    response += f"Matricula: {f['matricula']}\n"
                    response += f"Nombre: {f['nombre']} {f['apellido']}\n"
                    response += f"Motivo: {f['motivo']}\n"
                    if f.get('tutor_nombre') and f.get('tutor_apellido'):
                        response += f"Tutor del grupo: {f['tutor_nombre']} {f['tutor_apellido']}"
                        if f.get('tutor_empleado'):
                            response += f" (matricula: {f['tutor_empleado']})"
                        response += "\n"
                    else:
                        response += "Tutor del grupo: No asignado\n"
                    response += f"Categoria: Familiar\n\n"
            else:
                response += "No hay registros familiares en la base de datos.\n\n"
        else:
            response += "No hay registros familiares en la base de datos.\n\n"
        
        if not response.strip():
            response = "No se encontraron estudiantes vulnerables."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetEconomicVulnerableStudents(Action):
    def name(self) -> Text:
        return "action_get_economic_vulnerable_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            a.matricula,
            u.nombre,
            u.apellido,
            rr.descripcion AS motivo,
            tutor_u.nombre AS tutor_nombre,
            tutor_u.apellido AS tutor_apellido,
            tutor_p.numero_empleado AS tutor_empleado
        FROM reportes_riesgo rr
        JOIN alumnos a ON rr.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id AND g.activo = TRUE
        LEFT JOIN profesores tutor_p ON g.profesor_tutor_id = tutor_p.id
        LEFT JOIN usuarios tutor_u ON tutor_p.usuario_id = tutor_u.id
        WHERE rr.tipo_riesgo = 'economico'
        ORDER BY rr.nivel_riesgo DESC
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if len(result) == 0:
                response = "No se encontraron estudiantes con vulnerabilidad economica."
            else:
                response = f"Alumnos vulnerables economicamente ({len(result)} casos):\n\n"
                for row in result:
                    response += f"Matricula: {row['matricula']}\n"
                    response += f"Nombre: {row['nombre']} {row['apellido']}\n"
                    response += f"Motivo: {row['motivo']}\n"
                    if row.get('tutor_nombre') and row.get('tutor_apellido'):
                        response += f"Tutor del grupo: {row['tutor_nombre']} {row['tutor_apellido']}"
                        if row.get('tutor_empleado'):
                            response += f" (matricula: {row['tutor_empleado']})"
                        response += "\n"
                    else:
                        response += "Tutor del grupo: No asignado\n"
                    response += f"Categoria: Economica\n\n"
        else:
            response = "No pude obtener informacion de estudiantes con vulnerabilidad economica."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetFamilyVulnerableStudents(Action):
    def name(self) -> Text:
        return "action_get_family_vulnerable_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            a.matricula,
            u.nombre,
            u.apellido,
            rr.descripcion AS motivo,
            tutor_u.nombre AS tutor_nombre,
            tutor_u.apellido AS tutor_apellido,
            tutor_p.numero_empleado AS tutor_empleado
        FROM reportes_riesgo rr
        JOIN alumnos a ON rr.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id AND g.activo = TRUE
        LEFT JOIN profesores tutor_p ON g.profesor_tutor_id = tutor_p.id
        LEFT JOIN usuarios tutor_u ON tutor_p.usuario_id = tutor_u.id
        WHERE rr.tipo_riesgo = 'familiar'
        ORDER BY rr.nivel_riesgo DESC
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if len(result) == 0:
                response = "No se encontraron estudiantes con vulnerabilidad familiar."
            else:
                response = f"Alumnos vulnerables familiarmente ({len(result)} casos):\n\n"
                for row in result:
                    response += f"Matricula: {row['matricula']}\n"
                    response += f"Nombre: {row['nombre']} {row['apellido']}\n"
                    response += f"Motivo: {row['motivo']}\n"
                    if row.get('tutor_nombre') and row.get('tutor_apellido'):
                        response += f"Tutor del grupo: {row['tutor_nombre']} {row['tutor_apellido']}"
                        if row.get('tutor_empleado'):
                            response += f" (matricula: {row['tutor_empleado']})"
                        response += "\n"
                    else:
                        response += "Tutor del grupo: No asignado\n"
                    response += f"Categoria: Familiar\n\n"
        else:
            response = "No pude obtener informacion de estudiantes con vulnerabilidad familiar."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetAcademicRiskStudents(Action):
    def name(self) -> Text:
        return "action_get_academic_risk_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT CONCAT(ua.nombre, ' ', ua.apellido) as alumno,
               a.matricula,
               c.nombre as carrera,
               rr.descripcion,
               rr.observaciones,
               rr.acciones_recomendadas,
               rr.fecha_reporte,
               rr.estado,
               rr.nivel_riesgo
        FROM alumnos a
        JOIN usuarios ua ON a.usuario_id = ua.id
        JOIN carreras c ON a.carrera_id = c.id
        JOIN reportes_riesgo rr ON a.id = rr.alumno_id
        WHERE a.estado_alumno = 'activo' 
          AND rr.tipo_riesgo = 'academico'
          AND rr.estado IN ('abierto', 'en_proceso')
        ORDER BY 
            CASE rr.nivel_riesgo
                WHEN 'critico' THEN 4
                WHEN 'alto' THEN 3
                WHEN 'medio' THEN 2
                ELSE 1
            END DESC,
            rr.fecha_reporte DESC
        LIMIT 20
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron estudiantes con riesgo academico."
            else:
                response = f"Estudiantes con riesgo academico ({len(result)} casos):\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['alumno']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Nivel de riesgo: {row['nivel_riesgo']}\n"
                    response += f"   Descripcion: {row['descripcion']}\n"
                    response += f"   Observaciones: {row['observaciones']}\n"
                    response += f"   Acciones recomendadas: {row['acciones_recomendadas']}\n"
                    response += f"   Fecha reporte: {row['fecha_reporte']}\n"
                    response += f"   Estado: {row['estado']}\n\n"
        else:
            response = "No pude obtener informacion de estudiantes con riesgo academico."
        
        dispatcher.utter_message(text=response)
        return []


   
class ActionGetIndiceReprobacionCarrera(Action):
    def name(self) -> Text:
        return "action_get_indice_reprobacion_carrera"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        carrera_nombre = tracker.get_slot('carrera')
        
        where_condition = "WHERE cal.estatus IN ('reprobado', 'aprobado', 'cursando')"
        params = []
        
        if carrera_nombre:
            where_condition += " AND c.nombre LIKE %s"
            params.append(f"%{carrera_nombre}%")
        
        query = f"""
        SELECT 
            c.nombre as carrera,
            COUNT(DISTINCT cal.alumno_id) as total_alumnos,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as materias_reprobadas,
            COUNT(cal.id) as total_calificaciones,
            ROUND(
                (COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2
            ) as porcentaje_reprobacion
        FROM calificaciones cal
        JOIN alumnos a ON cal.alumno_id = a.id
        JOIN carreras c ON a.carrera_id = c.id
        {where_condition}
        GROUP BY c.id, c.nombre
        ORDER BY porcentaje_reprobacion DESC
        """
        
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos de reprobación por carrera."
            else:
                response = "Índice de reprobación por carrera:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['carrera']}\n"
                    response += f"   Total alumnos: {row['total_alumnos']}\n"
                    response += f"   Materias reprobadas: {row['materias_reprobadas']}\n"
                    response += f"   Porcentaje de reprobación: {row['porcentaje_reprobacion']}%\n\n"
        else:
            response = "No pude obtener el índice de reprobación por carrera."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetIndiceReprobacionGrupo(Action):
    def name(self) -> Text:
        return "action_get_indice_reprobacion_grupo"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        grupo_codigo = tracker.get_slot('grupo')
        
        where_condition = "WHERE cal.estatus IN ('reprobado', 'aprobado', 'cursando')"
        params = []
        
        if grupo_codigo:
            where_condition += " AND g.codigo LIKE %s"
            params.append(f"%{grupo_codigo}%")
        
        query = f"""
        SELECT 
            g.codigo as grupo,
            c.nombre as carrera,
            COUNT(DISTINCT cal.alumno_id) as total_alumnos,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as materias_reprobadas,
            COUNT(cal.id) as total_calificaciones,
            ROUND(
                (COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2
            ) as porcentaje_reprobacion
        FROM calificaciones cal
        JOIN grupos g ON cal.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        {where_condition}
        GROUP BY g.id, g.codigo, c.nombre
        ORDER BY porcentaje_reprobacion DESC
        """
        
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos de reprobación por grupo."
            else:
                response = "Índice de reprobación por grupo:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. Grupo {row['grupo']} ({row['carrera']})\n"
                    response += f"   Total alumnos: {row['total_alumnos']}\n"
                    response += f"   Materias reprobadas: {row['materias_reprobadas']}\n"
                    response += f"   Porcentaje de reprobación: {row['porcentaje_reprobacion']}%\n\n"
        else:
            response = "No pude obtener el índice de reprobación por grupo."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetIndiceReprobacionAlumno(Action):
    def name(self) -> Text:
        return "action_get_indice_reprobacion_alumno"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        nombre_estudiante = tracker.get_slot('nombre_estudiante')
        
        where_condition = "WHERE cal.estatus IN ('reprobado', 'aprobado', 'cursando')"
        params = []
        
        if nombre_estudiante:
            where_condition += " AND (CONCAT(u.nombre, ' ', u.apellido) LIKE %s OR u.nombre LIKE %s OR u.apellido LIKE %s)"
            params.extend([f"%{nombre_estudiante}%", f"%{nombre_estudiante}%", f"%{nombre_estudiante}%"])
        
        query = f"""
        SELECT 
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            c.nombre as carrera,
            g.codigo as grupo,
            COUNT(cal.id) as total_materias,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as materias_reprobadas,
            ROUND(
                (COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2
            ) as porcentaje_reprobacion
        FROM calificaciones cal
        JOIN alumnos a ON cal.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        JOIN grupos g ON cal.grupo_id = g.id
        {where_condition}
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo
        HAVING COUNT(cal.id) > 0
        ORDER BY porcentaje_reprobacion DESC, materias_reprobadas DESC
        LIMIT 20
        """
        
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron alumnos con datos de reprobación."
            else:
                response = "Índice de reprobación por alumno:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre_completo']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Grupo: {row['grupo']}\n"
                    response += f"   Total materias: {row['total_materias']}\n"
                    response += f"   Materias reprobadas: {row['materias_reprobadas']}\n"
                    response += f"   Porcentaje de reprobación: {row['porcentaje_reprobacion']}%\n\n"
        else:
            response = "No pude obtener el índice de reprobación por alumno."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetIndiceReprobacionAsignatura(Action):
    def name(self) -> Text:
        return "action_get_indice_reprobacion_asignatura"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        asignatura_nombre = tracker.get_slot('asignatura')
        
        where_condition = "WHERE cal.estatus IN ('reprobado', 'aprobado', 'cursando')"
        params = []
        
        if asignatura_nombre:
            where_condition += " AND asig.nombre LIKE %s"
            params.append(f"%{asignatura_nombre}%")
        
        query = f"""
        SELECT 
            asig.nombre as asignatura,
            asig.codigo as codigo_asignatura,
            c.nombre as carrera,
            asig.cuatrimestre,
            COUNT(cal.id) as total_calificaciones,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as reprobaciones,
            ROUND(
                (COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2
            ) as porcentaje_reprobacion
        FROM calificaciones cal
        JOIN asignaturas asig ON cal.asignatura_id = asig.id
        JOIN carreras c ON asig.carrera_id = c.id
        {where_condition}
        GROUP BY asig.id, asig.nombre, asig.codigo, c.nombre, asig.cuatrimestre
        HAVING COUNT(cal.id) >= 3
        ORDER BY porcentaje_reprobacion DESC
        LIMIT 15
        """
        
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos de reprobación por asignatura."
            else:
                response = "Índice de reprobación por asignatura:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['asignatura']} ({row['codigo_asignatura']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Cuatrimestre: {row['cuatrimestre']}\n"
                    response += f"   Total evaluaciones: {row['total_calificaciones']}\n"
                    response += f"   Reprobaciones: {row['reprobaciones']}\n"
                    response += f"   Porcentaje de reprobación: {row['porcentaje_reprobacion']}%\n\n"
        else:
            response = "No pude obtener el índice de reprobación por asignatura."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetIndiceReprobacionGeneral(Action):
    def name(self) -> Text:
        return "action_get_indice_reprobacion_general"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            COUNT(DISTINCT cal.alumno_id) as total_alumnos,
            COUNT(cal.id) as total_calificaciones,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as total_reprobaciones,
            ROUND(
                (COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2
            ) as porcentaje_reprobacion_general
        FROM calificaciones cal
        WHERE cal.estatus IN ('reprobado', 'aprobado', 'cursando')
        """
        
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos generales de reprobación."
            else:
                row = result[0]
                response = "Índice de reprobación general de la institución:\n\n"
                response += f"Total de alumnos: {row['total_alumnos']}\n"
                response += f"Total de calificaciones: {row['total_calificaciones']}\n"
                response += f"Total de reprobaciones: {row['total_reprobaciones']}\n"
                response += f"Porcentaje de reprobación general: {row['porcentaje_reprobacion_general']}%\n"
        else:
            response = "No pude obtener el índice de reprobación general."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetDatosEstudiante(Action):
    def name(self) -> Text:
        return "action_get_datos_estudiante"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        nombre_estudiante = tracker.get_slot('nombre_estudiante')
        
        if not nombre_estudiante:
            dispatcher.utter_message(text="Por favor especifica el nombre del estudiante.")
            return []
        
        query = """
        SELECT 
            u.nombre,
            u.apellido,
            u.correo,
            a.matricula,
            c.nombre as carrera,
            g.codigo as grupo,
            a.cuatrimestre_actual,
            a.fecha_ingreso,
            a.telefono,
            a.direccion,
            a.fecha_nacimiento,
            a.tutor_nombre,
            a.tutor_telefono,
            a.estado_alumno,
            a.promedio_general,
            a.ultima_oportunidad_usada
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = 1
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        WHERE CONCAT(u.nombre, ' ', u.apellido) LIKE %s
           OR u.nombre LIKE %s 
           OR u.apellido LIKE %s
           OR a.matricula LIKE %s
        """
        
        params = [f"%{nombre_estudiante}%", f"%{nombre_estudiante}%", f"%{nombre_estudiante}%", f"%{nombre_estudiante}%"]
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontró información del estudiante {nombre_estudiante}."
            else:
                response = "Datos del estudiante:\n\n"
                for row in result:
                    response += f"Nombre: {row['nombre']} {row['apellido']}\n"
                    response += f"Matrícula: {row['matricula']}\n"
                    response += f"Correo: {row['correo']}\n"
                    response += f"Carrera: {row['carrera']}\n"
                    if row['grupo']:
                        response += f"Grupo: {row['grupo']}\n"
                    response += f"Cuatrimestre actual: {row['cuatrimestre_actual']}\n"
                    response += f"Fecha de ingreso: {row['fecha_ingreso']}\n"
                    if row['telefono']:
                        response += f"Teléfono: {row['telefono']}\n"
                    if row['tutor_nombre']:
                        response += f"Tutor: {row['tutor_nombre']}\n"
                    response += f"Estado: {row['estado_alumno']}\n"
                    response += f"Promedio general: {row['promedio_general']}\n"
                    response += f"Última oportunidad usada: {'Sí' if row['ultima_oportunidad_usada'] else 'No'}\n\n"
        else:
            response = "No pude obtener los datos del estudiante."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetCalificacionesEstudiante(Action):
    def name(self) -> Text:
        return "action_get_calificaciones_estudiante"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        nombre_estudiante = tracker.get_slot('nombre_estudiante')
        
        if not nombre_estudiante:
            dispatcher.utter_message(text="Por favor especifica el nombre del estudiante.")
            return []
        
        query = """
        SELECT 
            asig.nombre as asignatura,
            asig.codigo,
            cal.calificacion_final,
            cal.estatus,
            cal.ciclo_escolar,
            g.codigo as grupo,
            CONCAT(up.nombre, ' ', up.apellido) as profesor
        FROM calificaciones cal
        JOIN alumnos a ON cal.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN asignaturas asig ON cal.asignatura_id = asig.id
        JOIN grupos g ON cal.grupo_id = g.id
        JOIN profesores p ON cal.profesor_id = p.id
        JOIN usuarios up ON p.usuario_id = up.id
        WHERE CONCAT(u.nombre, ' ', u.apellido) LIKE %s
           OR u.nombre LIKE %s 
           OR u.apellido LIKE %s
        ORDER BY cal.ciclo_escolar DESC, asig.cuatrimestre
        """
        
        params = [f"%{nombre_estudiante}%", f"%{nombre_estudiante}%", f"%{nombre_estudiante}%"]
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontraron calificaciones para {nombre_estudiante}."
            else:
                response = f"Calificaciones de {nombre_estudiante}:\n\n"
                for row in result:
                    response += f"Asignatura: {row['asignatura']} ({row['codigo']})\n"
                    response += f"Calificación: {row['calificacion_final']}\n"
                    response += f"Estatus: {row['estatus']}\n"
                    response += f"Ciclo escolar: {row['ciclo_escolar']}\n"
                    response += f"Grupo: {row['grupo']}\n"
                    response += f"Profesor: {row['profesor']}\n\n"
        else:
            response = "No pude obtener las calificaciones del estudiante."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetEstudiantesBajoRendimiento(Action):
    def name(self) -> Text:
        return "action_get_estudiantes_bajo_rendimiento"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        numero = tracker.get_slot('numero')
        limite_promedio = numero if numero else 7.0
        
        query = """
        SELECT 
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            c.nombre as carrera,
            g.codigo as grupo,
            a.promedio_general,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as materias_reprobadas
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = 1
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE a.promedio_general < %s AND a.promedio_general > 0
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo, a.promedio_general
        ORDER BY a.promedio_general ASC
        LIMIT 20
        """
        
        result = execute_query(query, [limite_promedio])
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontraron estudiantes con promedio menor a {limite_promedio}."
            else:
                response = f"Estudiantes con promedio menor a {limite_promedio}:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre_completo']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    if row['grupo']:
                        response += f"   Grupo: {row['grupo']}\n"
                    response += f"   Promedio: {row['promedio_general']}\n"
                    response += f"   Materias reprobadas: {row['materias_reprobadas']}\n\n"
        else:
            response = "No pude obtener la información de estudiantes con bajo rendimiento."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetEstudiantesAltoRendimiento(Action):
    def name(self) -> Text:
        return "action_get_estudiantes_alto_rendimiento"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        numero = tracker.get_slot('numero')
        limite_promedio = numero if numero else 9.0
        
        query = """
        SELECT 
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            c.nombre as carrera,
            g.codigo as grupo,
            a.promedio_general,
            COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) as materias_aprobadas
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = 1
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE a.promedio_general >= %s
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo, a.promedio_general
        ORDER BY a.promedio_general DESC
        LIMIT 15
        """
        
        result = execute_query(query, [limite_promedio])
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontraron estudiantes con promedio mayor o igual a {limite_promedio}."
            else:
                response = f"Estudiantes con promedio mayor o igual a {limite_promedio}:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre_completo']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    if row['grupo']:
                        response += f"   Grupo: {row['grupo']}\n"
                    response += f"   Promedio: {row['promedio_general']}\n"
                    response += f"   Materias aprobadas: {row['materias_aprobadas']}\n\n"
        else:
            response = "No pude obtener la información de estudiantes con alto rendimiento."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetMejoresPromediosCarrera(Action):
    def name(self) -> Text:
        return "action_get_mejores_promedios_carrera"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            c.nombre as carrera,
            MAX(a.promedio_general) as mejor_promedio,
            CONCAT(u.nombre, ' ', u.apellido) as mejor_estudiante,
            a.matricula
        FROM carreras c
        JOIN alumnos a ON c.id = a.carrera_id
        JOIN usuarios u ON a.usuario_id = u.id
        WHERE a.promedio_general = (
            SELECT MAX(a2.promedio_general) 
            FROM alumnos a2 
            WHERE a2.carrera_id = c.id AND a2.promedio_general > 0
        )
        GROUP BY c.id, c.nombre, u.nombre, u.apellido, a.matricula, a.promedio_general
        ORDER BY mejor_promedio DESC
        """
        
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos de mejores promedios por carrera."
            else:
                response = "Mejores promedios por carrera:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['carrera']}\n"
                    response += f"   Mejor promedio: {row['mejor_promedio']}\n"
                    response += f"   Estudiante: {row['mejor_estudiante']} ({row['matricula']})\n\n"
        else:
            response = "No pude obtener los mejores promedios por carrera."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetListaGrupos(Action):
    def name(self) -> Text:
        return "action_get_lista_grupos"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            g.codigo as grupo,
            c.nombre as carrera,
            g.cuatrimestre,
            g.ciclo_escolar,
            g.periodo,
            COUNT(ag.alumno_id) as total_alumnos,
            CONCAT(up.nombre, ' ', up.apellido) as tutor
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = 1
        LEFT JOIN profesores p ON g.profesor_tutor_id = p.id
        LEFT JOIN usuarios up ON p.usuario_id = up.id
        WHERE g.activo = 1
        GROUP BY g.id, g.codigo, c.nombre, g.cuatrimestre, g.ciclo_escolar, g.periodo, up.nombre, up.apellido
        ORDER BY c.nombre, g.cuatrimestre
        """
        
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron grupos activos."
            else:
                response = "Lista de grupos activos:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. Grupo {row['grupo']}\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Cuatrimestre: {row['cuatrimestre']}\n"
                    response += f"   Ciclo escolar: {row['ciclo_escolar']}\n"
                    response += f"   Período: {row['periodo']}\n"
                    response += f"   Total alumnos: {row['total_alumnos']}\n"
                    if row['tutor']:
                        response += f"   Tutor: {row['tutor']}\n"
                    response += "\n"
        else:
            response = "No pude obtener la lista de grupos."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetMateriasComplejas(Action):
    def name(self) -> Text:
        return "action_get_materias_complejas"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            asig.nombre as asignatura,
            c.nombre as carrera,
            asig.cuatrimestre,
            COUNT(cal.id) as total_evaluaciones,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as reprobaciones,
            ROUND(
                (COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2
            ) as porcentaje_reprobacion
        FROM asignaturas asig
        JOIN calificaciones cal ON asig.id = cal.asignatura_id
        JOIN carreras c ON asig.carrera_id = c.id
        WHERE cal.estatus IN ('reprobado', 'aprobado', 'cursando')
        GROUP BY asig.id, asig.nombre, c.nombre, asig.cuatrimestre
        HAVING COUNT(cal.id) >= 3
        ORDER BY porcentaje_reprobacion DESC
        LIMIT
        15
        """
        
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron materias con datos de reprobación."
            else:
                response = "Materias más complejas (mayor índice de reprobación):\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['asignatura']}\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Cuatrimestre: {row['cuatrimestre']}\n"
                    response += f"   Total evaluaciones: {row['total_evaluaciones']}\n"
                    response += f"   Reprobaciones: {row['reprobaciones']}\n"
                    response += f"   Porcentaje de reprobación: {row['porcentaje_reprobacion']}%\n\n"
        else:
            response = "No pude obtener las materias más complejas."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetEstudiantesConMasReprobaciones(Action):
    def name(self) -> Text:
        return "action_get_estudiantes_con_mas_reprobaciones"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            c.nombre as carrera,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as materias_reprobadas,
            a.promedio_general
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE cal.estatus IN ('reprobado', 'aprobado', 'cursando')
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, a.promedio_general
        HAVING COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) > 2
        ORDER BY materias_reprobadas DESC
        LIMIT 20
        """
        
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron estudiantes con más de 2 materias reprobadas."
            else:
                response = "Estudiantes con más reprobaciones:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre_completo']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Materias reprobadas: {row['materias_reprobadas']}\n"
                    response += f"   Promedio general: {row['promedio_general']}\n\n"
        else:
            response = "No pude obtener los estudiantes con más reprobaciones."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetComparativaCarreras(Action):
    def name(self) -> Text:
        return "action_get_comparativa_carreras"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            c.nombre as carrera,
            COUNT(DISTINCT a.id) as total_alumnos,
            AVG(a.promedio_general) as promedio_carrera,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as total_reprobaciones,
            COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) as total_aprobaciones,
            ROUND(
                (COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / 
                 COUNT(CASE WHEN cal.estatus IN ('reprobado', 'aprobado') THEN 1 END)), 2
            ) as porcentaje_reprobacion
        FROM carreras c
        LEFT JOIN alumnos a ON c.id = a.carrera_id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE cal.estatus IN ('reprobado', 'aprobado', 'cursando') OR cal.estatus IS NULL
        GROUP BY c.id, c.nombre
        ORDER BY porcentaje_reprobacion DESC
        """
        
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos para comparativa de carreras."
            else:
                response = "Comparativa de rendimiento por carrera:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['carrera']}\n"
                    response += f"   Total alumnos: {row['total_alumnos']}\n"
                    if row['promedio_carrera']:
                        response += f"   Promedio de carrera: {row['promedio_carrera']:.2f}\n"
                    response += f"   Total reprobaciones: {row['total_reprobaciones']}\n"
                    response += f"   Total aprobaciones: {row['total_aprobaciones']}\n"
                    if row['porcentaje_reprobacion']:
                        response += f"   Porcentaje de reprobación: {row['porcentaje_reprobacion']}%\n"
                    response += "\n"
        else:
            response = "No pude obtener la comparativa de carreras."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTop10MejoresEstudiantes(Action):
    def name(self) -> Text:
        return "action_get_top10_mejores_estudiantes"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            c.nombre as carrera,
            g.codigo as grupo,
            a.promedio_general,
            COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) as materias_aprobadas,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as materias_reprobadas
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = 1
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE a.promedio_general > 0
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo, a.promedio_general
        ORDER BY a.promedio_general DESC
        LIMIT 10
        """
        
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron estudiantes con promedio registrado."
            else:
                response = "Top 10 mejores estudiantes:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre_completo']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    if row['grupo']:
                        response += f"   Grupo: {row['grupo']}\n"
                    response += f"   Promedio: {row['promedio_general']}\n"
                    response += f"   Materias aprobadas: {row['materias_aprobadas']}\n"
                    response += f"   Materias reprobadas: {row['materias_reprobadas']}\n\n"
        else:
            response = "No pude obtener el top 10 de mejores estudiantes."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetAlumnosRiesgoAcademico(Action):
    def name(self) -> Text:
        return "action_get_alumnos_riesgo_academico"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            c.nombre as carrera,
            g.codigo as grupo,
            a.promedio_general,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as materias_reprobadas,
            COUNT(cal.id) as total_materias,
            ROUND(
                (COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2
            ) as porcentaje_reprobacion,
            a.ultima_oportunidad_usada
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = 1
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE a.estado_alumno = 'activo'
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, g.codigo, a.promedio_general, a.ultima_oportunidad_usada
        HAVING (a.promedio_general < 7.0 AND a.promedio_general > 0) 
           OR COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) >= 2
           OR (COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)) >= 30
        ORDER BY porcentaje_reprobacion DESC, materias_reprobadas DESC
        LIMIT 25
        """
        
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron alumnos en riesgo académico."
            else:
                response = "Alumnos en riesgo académico:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre_completo']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    if row['grupo']:
                        response += f"   Grupo: {row['grupo']}\n"
                    response += f"   Promedio: {row['promedio_general']}\n"
                    response += f"   Materias reprobadas: {row['materias_reprobadas']}/{row['total_materias']}\n"
                    response += f"   Porcentaje reprobación: {row['porcentaje_reprobacion']}%\n"
                    response += f"   Última oportunidad usada: {'Sí' if row['ultima_oportunidad_usada'] else 'No'}\n\n"
        else:
            response = "No pude obtener los alumnos en riesgo académico."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetRankingGruposReprobacion(Action):
    def name(self) -> Text:
        return "action_get_ranking_grupos_reprobacion"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            g.codigo as grupo,
            c.nombre as carrera,
            g.cuatrimestre,
            COUNT(DISTINCT ag.alumno_id) as total_alumnos,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as total_reprobaciones,
            COUNT(CASE WHEN cal.estatus IN ('reprobado', 'aprobado') THEN 1 END) as total_evaluaciones,
            ROUND(
                (COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / 
                 NULLIF(COUNT(CASE WHEN cal.estatus IN ('reprobado', 'aprobado') THEN 1 END), 0)), 2
            ) as porcentaje_reprobacion,
            AVG(a.promedio_general) as promedio_grupo
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = 1
        LEFT JOIN alumnos a ON ag.alumno_id = a.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id AND cal.grupo_id = g.id
        WHERE g.activo = 1
        GROUP BY g.id, g.codigo, c.nombre, g.cuatrimestre
        HAVING COUNT(DISTINCT ag.alumno_id) > 0
        ORDER BY porcentaje_reprobacion DESC
        LIMIT 15
        """
        
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos de grupos para ranking."
            else:
                response = "Ranking de grupos por índice de reprobación:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. Grupo {row['grupo']} ({row['carrera']})\n"
                    response += f"   Cuatrimestre: {row['cuatrimestre']}\n"
                    response += f"   Total alumnos: {row['total_alumnos']}\n"
                    response += f"   Total reprobaciones: {row['total_reprobaciones']}\n"
                    if row['porcentaje_reprobacion']:
                        response += f"   Porcentaje reprobación: {row['porcentaje_reprobacion']}%\n"
                    if row['promedio_grupo']:
                        response += f"   Promedio del grupo: {row['promedio_grupo']:.2f}\n"
                    response += "\n"
        else:
            response = "No pude obtener el ranking de grupos por reprobación."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetEstudiantesPorGrupo(Action):
    def name(self) -> Text:
        return "action_get_estudiantes_por_grupo"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        grupo_codigo = tracker.get_slot('grupo')
        
        if not grupo_codigo:
            dispatcher.utter_message(text="Por favor especifica el código del grupo.")
            return []
        
        query = """
        SELECT 
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            a.promedio_general,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as materias_reprobadas,
            COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) as materias_aprobadas,
            a.estado_alumno
        FROM grupos g
        JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = 1
        JOIN alumnos a ON ag.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id AND cal.grupo_id = g.id
        WHERE g.codigo LIKE %s
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, a.promedio_general, a.estado_alumno
        ORDER BY a.promedio_general DESC
        """
        
        result = execute_query(query, [f"%{grupo_codigo}%"])
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontraron estudiantes en el grupo {grupo_codigo}."
            else:
                response = f"Estudiantes del grupo {grupo_codigo}:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre_completo']} ({row['matricula']})\n"
                    response += f"   Promedio: {row['promedio_general']}\n"
                    response += f"   Materias aprobadas: {row['materias_aprobadas']}\n"
                    response += f"   Materias reprobadas: {row['materias_reprobadas']}\n"
                    response += f"   Estado: {row['estado_alumno']}\n\n"
        else:
            response = f"No pude obtener los estudiantes del grupo {grupo_codigo}."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetPromediosPorCuatrimestre(Action):
    def name(self) -> Text:
        return "action_get_promedios_por_cuatrimestre"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            g.cuatrimestre,
            c.nombre as carrera,
            COUNT(DISTINCT a.id) as total_alumnos,
            AVG(a.promedio_general) as promedio_cuatrimestre,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as total_reprobaciones,
            ROUND(
                (COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / 
                 NULLIF(COUNT(CASE WHEN cal.estatus IN ('reprobado', 'aprobado') THEN 1 END), 0)), 2
            ) as porcentaje_reprobacion
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = 1
        LEFT JOIN alumnos a ON ag.alumno_id = a.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id AND cal.grupo_id = g.id
        WHERE g.activo = 1 AND a.promedio_general > 0
        GROUP BY g.cuatrimestre, c.id, c.nombre
        ORDER BY c.nombre, g.cuatrimestre
        """
        
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron datos de promedios por cuatrimestre."
            else:
                response = "Promedios por cuatrimestre y carrera:\n\n"
                current_carrera = ""
                for row in result:
                    if row['carrera'] != current_carrera:
                        current_carrera = row['carrera']
                        response += f"\n{current_carrera}:\n"
                    
                    response += f"  Cuatrimestre {row['cuatrimestre']}:\n"
                    response += f"    Total alumnos: {row['total_alumnos']}\n"
                    if row['promedio_cuatrimestre']:
                        response += f"    Promedio: {row['promedio_cuatrimestre']:.2f}\n"
                    response += f"    Reprobaciones: {row['total_reprobaciones']}\n"
                    if row['porcentaje_reprobacion']:
                        response += f"    % Reprobación: {row['porcentaje_reprobacion']}%\n"
                    response += "\n"
        else:
            response = "No pude obtener los promedios por cuatrimestre."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetHistorialAcademico(Action):
    def name(self) -> Text:
        return "action_get_historial_academico"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        nombre_estudiante = tracker.get_slot('nombre_estudiante')
        
        if not nombre_estudiante:
            dispatcher.utter_message(text="Por favor especifica el nombre del estudiante.")
            return []
        
        query = """
        SELECT 
            u.nombre,
            u.apellido,
            a.matricula,
            c.nombre as carrera,
            a.cuatrimestre_actual,
            a.fecha_ingreso,
            a.promedio_general,
            a.estado_alumno,
            COUNT(cal.id) as total_materias_cursadas,
            COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) as materias_aprobadas,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as materias_reprobadas,
            COUNT(CASE WHEN cal.estatus = 'cursando' THEN 1 END) as materias_cursando
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE CONCAT(u.nombre, ' ', u.apellido) LIKE %s
           OR u.nombre LIKE %s 
           OR u.apellido LIKE %s
           OR a.matricula LIKE %s
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, a.cuatrimestre_actual, 
                 a.fecha_ingreso, a.promedio_general, a.estado_alumno
        """
        
        params = [f"%{nombre_estudiante}%", f"%{nombre_estudiante}%", f"%{nombre_estudiante}%", f"%{nombre_estudiante}%"]
        result = execute_query(query, params)
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontró historial académico para {nombre_estudiante}."
            else:
                response = "Historial académico:\n\n"
                for row in result:
                    response += f"Estudiante: {row['nombre']} {row['apellido']}\n"
                    response += f"Matrícula: {row['matricula']}\n"
                    response += f"Carrera: {row['carrera']}\n"
                    response += f"Cuatrimestre actual: {row['cuatrimestre_actual']}\n"
                    response += f"Fecha de ingreso: {row['fecha_ingreso']}\n"
                    response += f"Estado: {row['estado_alumno']}\n"
                    response += f"Promedio general: {row['promedio_general']}\n\n"
                    response += f"Resumen académico:\n"
                    response += f"  Total materias cursadas: {row['total_materias_cursadas']}\n"
                    response += f"  Materias aprobadas: {row['materias_aprobadas']}\n"
                    response += f"  Materias reprobadas: {row['materias_reprobadas']}\n"
                    response += f"  Materias cursando: {row['materias_cursando']}\n\n"
        else:
            response = "No pude obtener el historial académico del estudiante."
        
        dispatcher.utter_message(text=response)
        return []
    
class ActionGetStudentByMatricula(Action):
    def name(self) -> Text:
        return "action_get_student_by_matricula"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        matricula = next(tracker.get_latest_entity_values("matricula"), None)
        
        if not matricula:
            dispatcher.utter_message(text="Necesito la matrícula del estudiante para consultar su información.")
            return []
        
        query = """
        SELECT a.matricula, u.nombre, u.apellido, u.correo,
               a.cuatrimestre_actual, a.fecha_ingreso, a.telefono,
               a.estado_alumno, c.nombre as carrera,
               g.codigo as grupo_actual,
               a.tutor_nombre, a.tutor_telefono,
               COUNT(rr.id) as reportes_riesgo
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN reportes_riesgo rr ON a.id = rr.alumno_id AND rr.estado IN ('abierto', 'en_proceso')
        WHERE a.matricula = %s
        GROUP BY a.id
        """
        result = execute_query(query, (matricula,))
        
        if result and not isinstance(result, dict) and result:
            data = result[0]
            response = f"Información del estudiante {data['nombre']} {data['apellido']}:\n\n"
            response += f"Matrícula: {data['matricula']}\n"
            response += f"Correo: {data['correo']}\n"
            response += f"Estado: {data['estado_alumno']}\n"
            response += f"Carrera: {data['carrera']}\n"
            response += f"Cuatrimestre: {data['cuatrimestre_actual']}\n"
            response += f"Grupo: {data['grupo_actual'] or 'Sin asignar'}\n"
            response += f"Fecha ingreso: {data['fecha_ingreso']}\n"
            if data['telefono']:
                response += f"Teléfono: {data['telefono']}\n"
            if data['tutor_nombre']:
                response += f"Tutor: {data['tutor_nombre']}\n"
            if data['tutor_telefono']:
                response += f"Teléfono tutor: {data['tutor_telefono']}\n"
            if data['reportes_riesgo'] > 0:
                response += f"Reportes de riesgo activos: {data['reportes_riesgo']}\n"
        else:
            response = f"No encontré información para la matrícula: {matricula}."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTeacherByNumeroEmpleado(Action):
    def name(self) -> Text:
        return "action_get_teacher_by_numero_empleado"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        numero_empleado = next(tracker.get_latest_entity_values("numero_empleado"), None)
        
        if not numero_empleado:
            dispatcher.utter_message(text="Necesito el número de empleado del profesor para consultar su información.")
            return []
        
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido, u.correo,
               p.titulo_academico, p.especialidad, p.experiencia_años,
               c.nombre as carrera, p.fecha_contratacion,
               p.telefono, p.extension, p.cedula_profesional,
               COUNT(DISTINCT pag.grupo_id) as grupos_asignados,
               COUNT(DISTINCT pag.asignatura_id) as asignaturas,
               COUNT(DISTINCT g.id) as grupos_tutoria
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN carreras c ON p.carrera_id = c.id
        LEFT JOIN profesor_asignatura_grupo pag ON p.id = pag.profesor_id AND pag.activo = TRUE
        LEFT JOIN grupos g ON p.id = g.profesor_tutor_id AND g.activo = TRUE
        WHERE p.numero_empleado = %s AND p.activo = TRUE
        GROUP BY p.id
        """
        result = execute_query(query, (numero_empleado,))
        
        if result and not isinstance(result, dict) and result:
            data = result[0]
            response = f"Información del profesor {data['nombre']} {data['apellido']}:\n\n"
            response += f"Número empleado: {data['numero_empleado']}\n"
            response += f"Correo: {data['correo']}\n"
            response += f"Carrera: {data['carrera']}\n"
            response += f"Título académico: {data['titulo_academico'] or 'No especificado'}\n"
            response += f"Especialidad: {data['especialidad'] or 'No especificada'}\n"
            response += f"Experiencia: {data['experiencia_años'] or 0} años\n"
            response += f"Fecha contratación: {data['fecha_contratacion']}\n"
            if data['telefono']:
                response += f"Teléfono: {data['telefono']}\n"
            if data['extension']:
                response += f"Extensión: {data['extension']}\n"
            if data['cedula_profesional']:
                response += f"Cédula profesional: {data['cedula_profesional']}\n"
            response += f"Grupos asignados: {data['grupos_asignados']}\n"
            response += f"Asignaturas: {data['asignaturas']}\n"
            response += f"Grupos en tutoría: {data['grupos_tutoria']}\n"
        else:
            response = f"No encontré información para el empleado: {numero_empleado}."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetStudentsByGroup(Action):
    def name(self) -> Text:
        return "action_get_students_by_group"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        grupo = next(tracker.get_latest_entity_values("grupo"), None)
        
        if not grupo:
            dispatcher.utter_message(text="Necesito el código del grupo para consultar sus estudiantes.")
            return []
        
        query = """
        SELECT u.nombre, u.apellido, a.matricula, a.promedio_general,
               COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as materias_reprobadas,
               a.estado_alumno
        FROM grupos g
        JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        JOIN alumnos a ON ag.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE g.codigo = %s
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, a.promedio_general, a.estado_alumno
        ORDER BY u.apellido, u.nombre
        """
        result = execute_query(query, (grupo,))
        
        if result and not isinstance(result, dict):
            if not result:
                response = f"No se encontraron estudiantes en el grupo {grupo}."
            else:
                response = f"Estudiantes del grupo {grupo} ({len(result)} alumnos):\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                    response += f"   Estado: {row['estado_alumno']}\n"
                    response += f"   Promedio: {row['promedio_general']}\n"
                    response += f"   Materias reprobadas: {row['materias_reprobadas']}\n\n"
        else:
            response = f"No pude obtener los estudiantes del grupo {grupo}."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTeachersWithTutoring(Action):
    def name(self) -> Text:
        return "action_get_teachers_with_tutoring"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido, g.codigo as grupo,
               c.nombre as carrera, g.cuatrimestre,
               COUNT(ag.alumno_id) as total_alumnos
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN grupos g ON p.id = g.profesor_tutor_id
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE p.activo = TRUE AND g.activo = TRUE
        GROUP BY p.id, g.id
        ORDER BY c.nombre, g.codigo
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay profesores asignados como tutores actualmente."
            else:
                response = f"Profesores con tutoría ({len(result)} asignaciones):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['numero_empleado']})\n"
                    response += f"    Grupo: {row['grupo']} - {row['cuatrimestre']} cuatrimestre\n"
                    response += f"    Alumnos: {row['total_alumnos']}\n\n"
        else:
            response = "No pude obtener información de profesores tutores."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTeachersWithoutTutoring(Action):
    def name(self) -> Text:
        return "action_get_teachers_without_tutoring"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido, c.nombre as carrera,
               p.titulo_academico, p.especialidad
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN carreras c ON p.carrera_id = c.id
        LEFT JOIN grupos g ON p.id = g.profesor_tutor_id AND g.activo = TRUE
        WHERE p.activo = TRUE AND g.id IS NULL
        ORDER BY c.nombre, u.apellido
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "Todos los profesores activos tienen grupos de tutoría asignados."
            else:
                response = f"Profesores sin tutoría ({len(result)} profesores):\n\n"
                current_career = None
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                    
                    response += f"  {row['nombre']} {row['apellido']} ({row['numero_empleado']})\n"
                    response += f"    Título: {row['titulo_academico'] or 'No especificado'}\n"
                    response += f"    Especialidad: {row['especialidad'] or 'No especificada'}\n\n"
        else:
            response = "No pude obtener información de profesores sin tutoría."
        
        dispatcher.utter_message(text=response)
        return []
    
class ActionGetStudentsLastChance(Action):
    def name(self) -> Text:
        return "action_get_students_last_chance"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            c.nombre as carrera,
            g.codigo as grupo,
            a.cuatrimestre_actual,
            a.promedio_general,
            uol.fecha_uso as fecha_ultima_oportunidad,
            uol.calificacion as calificacion_ultima_oportunidad,
            uol.resultado,
            asig.nombre as asignatura_ultima_oportunidad
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = 1
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN ultima_oportunidad_log uol ON a.id = uol.alumno_id
        LEFT JOIN asignaturas asig ON uol.asignatura_id = asig.id
        WHERE a.ultima_oportunidad_usada = 1
        ORDER BY uol.fecha_uso DESC
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron estudiantes que hayan usado su última oportunidad."
            else:
                response = f"Estudiantes que han usado su última oportunidad ({len(result)} casos):\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre_completo']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    if row['grupo']:
                        response += f"   Grupo: {row['grupo']}\n"
                    response += f"   Cuatrimestre: {row['cuatrimestre_actual']}\n"
                    response += f"   Promedio general: {row['promedio_general']}\n"
                    if row['asignatura_ultima_oportunidad']:
                        response += f"   Asignatura última oportunidad: {row['asignatura_ultima_oportunidad']}\n"
                    response += f"   Calificación obtenida: {row['calificacion_ultima_oportunidad']}\n"
                    response += f"   Resultado: {row['resultado']}\n"
                    response += f"   Fecha de uso: {row['fecha_ultima_oportunidad']}\n\n"
        else:
            response = "No pude obtener información de estudiantes en última oportunidad."
        
        dispatcher.utter_message(text=response)
        return []
    
class ActionGetStudentsFailedSubjects(Action):
    def name(self) -> Text:
        return "action_get_students_failed_subjects"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            c.nombre as carrera,
            asig.nombre as asignatura,
            cal.calificacion_final,
            g.codigo as grupo
        FROM calificaciones cal
        JOIN alumnos a ON cal.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN asignaturas asig ON cal.asignatura_id = asig.id
        JOIN grupos g ON cal.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        WHERE cal.estatus = 'reprobado' AND a.estado_alumno = 'activo'
        ORDER BY u.apellido, u.nombre, asig.nombre
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No hay estudiantes reprobados actualmente."
            else:
                response = f"Estudiantes reprobados y sus asignaturas ({len(result)} casos):\n\n"
                current_student = None
                for row in result:
                    student_key = f"{row['nombre_completo']} ({row['matricula']})"
                    if current_student != student_key:
                        current_student = student_key
                        response += f"{student_key}\n"
                        response += f"  Carrera: {row['carrera']}\n"
                        response += f"  Grupo: {row['grupo']}\n"
                        response += "  Materias reprobadas:\n"
                    
                    response += f"    - {row['asignatura']}: {row['calificacion_final']}\n"
        else:
            response = "No pude obtener información de estudiantes reprobados."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetBestGroupAverage(Action):
    def name(self) -> Text:
        return "action_get_best_group_average"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            g.codigo as grupo,
            c.nombre as carrera,
            COUNT(a.id) as total_alumnos,
            AVG(a.promedio_general) as promedio_grupo
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = 1
        LEFT JOIN alumnos a ON ag.alumno_id = a.id AND a.promedio_general > 0
        WHERE g.activo = 1
        GROUP BY g.id, g.codigo, c.nombre
        HAVING COUNT(a.id) > 0
        ORDER BY promedio_grupo DESC
        LIMIT 10
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron grupos con promedios calculados."
            else:
                response = "Grupos con mejor promedio académico:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. Grupo {row['grupo']} ({row['carrera']})\n"
                    response += f"   Promedio: {row['promedio_grupo']:.2f}\n"
                    response += f"   Total alumnos: {row['total_alumnos']}\n\n"
        else:
            response = "No pude obtener información de promedios por grupo."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetWorstGroupFailure(Action):
    def name(self) -> Text:
        return "action_get_worst_group_failure"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            g.codigo as grupo,
            c.nombre as carrera,
            COUNT(DISTINCT a.id) as total_alumnos,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as total_reprobaciones,
            ROUND((COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / 
                   COUNT(CASE WHEN cal.estatus IN ('aprobado', 'reprobado') THEN 1 END)), 2) as porcentaje_reprobacion
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = 1
        LEFT JOIN alumnos a ON ag.alumno_id = a.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id AND cal.grupo_id = g.id
        WHERE g.activo = 1
        GROUP BY g.id, g.codigo, c.nombre
        HAVING COUNT(DISTINCT a.id) > 0 AND COUNT(CASE WHEN cal.estatus IN ('aprobado', 'reprobado') THEN 1 END) > 0
        ORDER BY porcentaje_reprobacion DESC
        LIMIT 10
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron grupos con datos de reprobación."
            else:
                response = "Grupos con mayor índice de reprobación:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. Grupo {row['grupo']} ({row['carrera']})\n"
                    response += f"   Porcentaje reprobación: {row['porcentaje_reprobacion']}%\n"
                    response += f"   Total reprobaciones: {row['total_reprobaciones']}\n"
                    response += f"   Total alumnos: {row['total_alumnos']}\n\n"
        else:
            response = "No pude obtener información de reprobación por grupo."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetLowestGradeStudents(Action):
    def name(self) -> Text:
        return "action_get_lowest_grade_students"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            c.nombre as carrera,
            a.promedio_general,
            MIN(cal.calificacion_final) as calificacion_minima
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE a.estado_alumno = 'activo' AND a.promedio_general > 0
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, a.promedio_general
        ORDER BY a.promedio_general ASC
        LIMIT 15
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron estudiantes con calificaciones registradas."
            else:
                response = "Estudiantes con las calificaciones más bajas:\n\n"
                for i, row in enumerate(result, 1):
                    response += f"{i}. {row['nombre_completo']} ({row['matricula']})\n"
                    response += f"   Carrera: {row['carrera']}\n"
                    response += f"   Promedio general: {row['promedio_general']}\n"
                    if row['calificacion_minima']:
                        response += f"   Calificación más baja: {row['calificacion_minima']}\n"
                    response += "\n"
        else:
            response = "No pude obtener información de estudiantes con calificaciones bajas."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetStudentAcademicProgress(Action):
    def name(self) -> Text:
        return "action_get_student_academic_progress"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        matricula = next(tracker.get_latest_entity_values("matricula"), None)
        
        if not matricula:
            dispatcher.utter_message(text="Necesito la matrícula del estudiante.")
            return []
        
        query = """
        SELECT 
            CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
            a.matricula,
            c.nombre as carrera,
            a.cuatrimestre_actual,
            a.promedio_general,
            COUNT(cal.id) as total_materias,
            COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) as materias_aprobadas,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as materias_reprobadas,
            COUNT(CASE WHEN cal.estatus = 'cursando' THEN 1 END) as materias_cursando
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE a.matricula = %s
        GROUP BY a.id, u.nombre, u.apellido, a.matricula, c.nombre, a.cuatrimestre_actual, a.promedio_general
        """
        result = execute_query(query, (matricula,))
        
        if result and not isinstance(result, dict) and result:
            row = result[0]
            response = f"Progreso académico de {row['nombre_completo']} ({row['matricula']}):\n\n"
            response += f"Carrera: {row['carrera']}\n"
            response += f"Cuatrimestre actual: {row['cuatrimestre_actual']}\n"
            response += f"Promedio general: {row['promedio_general']}\n"
            response += f"Total materias: {row['total_materias']}\n"
            response += f"Materias aprobadas: {row['materias_aprobadas']}\n"
            response += f"Materias reprobadas: {row['materias_reprobadas']}\n"
            response += f"Materias cursando: {row['materias_cursando']}\n"
        else:
            response = f"No se encontró información para la matrícula {matricula}."
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetAllSubjects(Action):
    def name(self) -> Text:
        return "action_get_all_subjects"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        query = """
        SELECT 
            asig.nombre as asignatura,
            asig.codigo,
            c.nombre as carrera,
            asig.cuatrimestre,
            asig.horas_teoricas,
            asig.horas_practicas
        FROM asignaturas asig
        JOIN carreras c ON asig.carrera_id = c.id
        WHERE asig.activa = 1
        ORDER BY c.nombre, asig.cuatrimestre, asig.nombre
        """
        result = execute_query(query)
        
        if result and not isinstance(result, dict):
            if not result:
                response = "No se encontraron asignaturas activas."
            else:
                response = f"Asignaturas del sistema ({len(result)} materias):\n\n"
                current_career = None
                current_cuatrimestre = None
                
                for row in result:
                    if current_career != row['carrera']:
                        current_career = row['carrera']
                        response += f"{current_career}:\n"
                        current_cuatrimestre = None
                    
                    if current_cuatrimestre != row['cuatrimestre']:
                        current_cuatrimestre = row['cuatrimestre']
                        response += f"  Cuatrimestre {row['cuatrimestre']}:\n"
                    
                    response += f"    - {row['asignatura']} ({row['codigo']})\n"
                    response += f"      Horas: {row['horas_teoricas']}T + {row['horas_practicas']}P\n"
        else:
            response = "No pude obtener información de las asignaturas."
        
        dispatcher.utter_message(text=response)
        return []    

class ActionGetGroupDetailedInfo(Action):
    def name(self) -> Text:
        return "action_get_group_detailed_info"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        grupo = next(tracker.get_latest_entity_values("grupo"), None)
        
        if not grupo:
            dispatcher.utter_message(text="Necesito el código del grupo para consultar su información detallada.")
            return []
        
        query_grupo = """
        SELECT g.codigo, g.cuatrimestre, g.ciclo_escolar, c.nombre as carrera,
               COUNT(ag.alumno_id) as cantidad_alumnos,
               CONCAT(ut.nombre, ' ', ut.apellido) as tutor_nombre,
               p.numero_empleado as tutor_matricula
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN profesores p ON g.profesor_tutor_id = p.id
        LEFT JOIN usuarios ut ON p.usuario_id = ut.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE g.codigo = %s AND g.activo = TRUE
        GROUP BY g.id
        """
        
        grupo_info = execute_query(query_grupo, (grupo,))
        
        if not grupo_info or isinstance(grupo_info, dict) or not grupo_info:
            dispatcher.utter_message(text=f"No encontré el grupo {grupo}.")
            return []
        
        info_grupo = grupo_info[0]
        
        query_alumnos = """
        SELECT u.nombre, u.apellido, a.matricula,
               COUNT(cal.id) as total_materias,
               COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) as aprobadas,
               COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as reprobadas,
               ROUND((COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2) as indice_aprobacion,
               ROUND((COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2) as indice_reprobacion
        FROM grupos g
        JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        JOIN alumnos a ON ag.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE g.codigo = %s
        GROUP BY a.id, u.nombre, u.apellido, a.matricula
        ORDER BY u.apellido, u.nombre
        """
        
        alumnos = execute_query(query_alumnos, (grupo,))
        
        query_asignaturas = """
        SELECT DISTINCT asig.nombre as asignatura
        FROM grupos g
        JOIN profesor_asignatura_grupo pag ON g.id = pag.grupo_id AND pag.activo = TRUE
        JOIN asignaturas asig ON pag.asignatura_id = asig.id
        WHERE g.codigo = %s
        ORDER BY asig.nombre
        """
        
        asignaturas = execute_query(query_asignaturas, (grupo,))
        
        query_calificaciones = """
        SELECT CONCAT(u.nombre, ' ', u.apellido) as alumno,
               a.matricula,
               asig.nombre as asignatura,
               cal.calificacion_final,
               cal.estatus
        FROM grupos g
        JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        JOIN alumnos a ON ag.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN calificaciones cal ON a.id = cal.alumno_id AND cal.grupo_id = g.id
        JOIN asignaturas asig ON cal.asignatura_id = asig.id
        WHERE g.codigo = %s
        ORDER BY u.apellido, u.nombre, asig.nombre
        """
        
        calificaciones = execute_query(query_calificaciones, (grupo,))
        
        total_materias_grupo = len(asignaturas) if asignaturas and not isinstance(asignaturas, dict) else 0
        total_calificaciones = len(calificaciones) if calificaciones and not isinstance(calificaciones, dict) else 0
        aprobadas_grupo = len([c for c in calificaciones if c['estatus'] == 'aprobado']) if calificaciones and not isinstance(calificaciones, dict) else 0
        reprobadas_grupo = len([c for c in calificaciones if c['estatus'] == 'reprobado']) if calificaciones and not isinstance(calificaciones, dict) else 0
        
        indice_aprobacion_grupo = round((aprobadas_grupo * 100.0 / total_calificaciones), 2) if total_calificaciones > 0 else 0
        indice_reprobacion_grupo = round((reprobadas_grupo * 100.0 / total_calificaciones), 2) if total_calificaciones > 0 else 0
        
        response = f"Información detallada del grupo {info_grupo['codigo']}:\n\n"
        response += f"Carrera: {info_grupo['carrera']}\n"
        response += f"Cuatrimestre: {info_grupo['cuatrimestre']}\n"
        response += f"Ciclo escolar: {info_grupo['ciclo_escolar']}\n"
        response += f"Cantidad de alumnos: {info_grupo['cantidad_alumnos']}\n"
        response += f"Tutor: {info_grupo['tutor_nombre'] or 'Sin asignar'}\n"
        response += f"Matricula del tutor: {info_grupo['tutor_matricula'] or 'Sin asignar'}\n"
        response += f"Indice de aprobacion del grupo: {indice_aprobacion_grupo}%\n"
        response += f"Indice de reprobacion del grupo: {indice_reprobacion_grupo}%\n\n"
        
        if asignaturas and not isinstance(asignaturas, dict):
            response += f"Asignaturas ({len(asignaturas)}):\n"
            for asig in asignaturas:
                response += f"  - {asig['asignatura']}\n"
            response += "\n"
        
        if alumnos and not isinstance(alumnos, dict):
            response += "Alumnos del grupo:\n"
            for i, alumno in enumerate(alumnos, 1):
                response += f"{i}. {alumno['nombre']} {alumno['apellido']} ({alumno['matricula']})\n"
                response += f"   Total materias: {alumno['total_materias']}\n"
                response += f"   Aprobadas: {alumno['aprobadas']}\n"
                response += f"   Reprobadas: {alumno['reprobadas']}\n"
                response += f"   Indice aprobacion: {alumno['indice_aprobacion']}%\n"
                response += f"   Indice reprobacion: {alumno['indice_reprobacion']}%\n\n"
        
        if calificaciones and not isinstance(calificaciones, dict):
            response += "Calificaciones por alumno y asignatura:\n"
            current_student = None
            for cal in calificaciones:
                student_key = f"{cal['alumno']} ({cal['matricula']})"
                if current_student != student_key:
                    current_student = student_key
                    response += f"\n{student_key}:\n"
                response += f"  - {cal['asignatura']}: {cal['calificacion_final']} ({cal['estatus']})\n"
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetTeacherDetailedInfo(Action):
    def name(self) -> Text:
        return "action_get_teacher_detailed_info"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        numero_empleado = next(tracker.get_latest_entity_values("numero_empleado"), None)
        
        if not numero_empleado:
            dispatcher.utter_message(text="Necesito el número de empleado del profesor para consultar su información detallada.")
            return []
        
        query_profesor = """
        SELECT p.numero_empleado, u.nombre, u.apellido, u.correo,
               p.titulo_academico, p.especialidad, p.experiencia_años,
               c.nombre as carrera, p.fecha_contratacion
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN carreras c ON p.carrera_id = c.id
        WHERE p.numero_empleado = %s AND p.activo = TRUE
        """
        
        profesor_info = execute_query(query_profesor, (numero_empleado,))
        
        if not profesor_info or isinstance(profesor_info, dict) or not profesor_info:
            dispatcher.utter_message(text=f"No encontré información para el profesor {numero_empleado}.")
            return []
        
        info_profesor = profesor_info[0]
        
        query_reprobados = """
        SELECT CONCAT(ua.nombre, ' ', ua.apellido) as alumno_nombre,
               a.matricula,
               g.codigo as grupo,
               asig.nombre as asignatura,
               cal.calificacion_final
        FROM profesores p
        JOIN calificaciones cal ON p.id = cal.profesor_id
        JOIN alumnos a ON cal.alumno_id = a.id
        JOIN usuarios ua ON a.usuario_id = ua.id
        JOIN grupos g ON cal.grupo_id = g.id
        JOIN asignaturas asig ON cal.asignatura_id = asig.id
        WHERE p.numero_empleado = %s AND cal.estatus = 'reprobado'
        ORDER BY g.codigo, ua.apellido
        """
        
        reprobados = execute_query(query_reprobados, (numero_empleado,))
        
        query_asignaturas = """
        SELECT DISTINCT asig.nombre as asignatura, g.codigo as grupo
        FROM profesores p
        JOIN profesor_asignatura_grupo pag ON p.id = pag.profesor_id AND pag.activo = TRUE
        JOIN asignaturas asig ON pag.asignatura_id = asig.id
        JOIN grupos g ON pag.grupo_id = g.id
        WHERE p.numero_empleado = %s
        ORDER BY asig.nombre
        """
        
        asignaturas = execute_query(query_asignaturas, (numero_empleado,))
        
        query_grupos = """
        SELECT DISTINCT g.codigo as grupo, c.nombre as carrera,
               CONCAT(ut.nombre, ' ', ut.apellido) as tutor_nombre,
               pt.numero_empleado as tutor_matricula
        FROM profesores p
        JOIN profesor_asignatura_grupo pag ON p.id = pag.profesor_id AND pag.activo = TRUE
        JOIN grupos g ON pag.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN profesores pt ON g.profesor_tutor_id = pt.id
        LEFT JOIN usuarios ut ON pt.usuario_id = ut.id
        WHERE p.numero_empleado = %s
        ORDER BY c.nombre, g.codigo
        """
        
        grupos = execute_query(query_grupos, (numero_empleado,))
        
        query_indices = """
        SELECT 
            COUNT(cal.id) as total_calificaciones,
            COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) as aprobados,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as reprobados,
            ROUND((COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2) as indice_aprobacion,
            ROUND((COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2) as indice_reprobacion
        FROM profesores p
        JOIN calificaciones cal ON p.id = cal.profesor_id
        WHERE p.numero_empleado = %s
        """
        
        indices = execute_query(query_indices, (numero_empleado,))
        
        response = f"Información detallada del profesor {info_profesor['nombre']} {info_profesor['apellido']}:\n\n"
        response += f"Numero de empleado: {info_profesor['numero_empleado']}\n"
        response += f"Correo: {info_profesor['correo']}\n"
        response += f"Carrera: {info_profesor['carrera']}\n"
        response += f"Titulo academico: {info_profesor['titulo_academico'] or 'No especificado'}\n"
        response += f"Especialidad: {info_profesor['especialidad'] or 'No especificada'}\n"
        response += f"Experiencia: {info_profesor['experiencia_años'] or 0} años\n"
        response += f"Fecha contratacion: {info_profesor['fecha_contratacion']}\n\n"
        
        if indices and not isinstance(indices, dict) and indices:
            ind = indices[0]
            response += f"Indices academicos:\n"
            response += f"Total evaluaciones: {ind['total_calificaciones']}\n"
            response += f"Aprobados: {ind['aprobados']}\n"
            response += f"Reprobados: {ind['reprobados']}\n"
            response += f"Indice de aprobacion: {ind['indice_aprobacion']}%\n"
            response += f"Indice de reprobacion: {ind['indice_reprobacion']}%\n\n"
        
        if asignaturas and not isinstance(asignaturas, dict):
            response += f"Asignaturas que imparte ({len(asignaturas)}):\n"
            for asig in asignaturas:
                response += f"  - {asig['asignatura']} (Grupo {asig['grupo']})\n"
            response += "\n"
        
        if grupos and not isinstance(grupos, dict):
            response += f"Grupos a los que da clases ({len(grupos)}):\n"
            for grupo in grupos:
                response += f"  - Grupo {grupo['grupo']} ({grupo['carrera']})\n"
                response += f"    Tutor: {grupo['tutor_nombre'] or 'Sin tutor'}\n"
                response += f"    Matricula tutor: {grupo['tutor_matricula'] or 'Sin matricula'}\n"
            response += "\n"
        
        if reprobados and not isinstance(reprobados, dict):
            response += f"Alumnos reprobados ({len(reprobados)}):\n"
            for rep in reprobados:
                response += f"  - {rep['alumno_nombre']} ({rep['matricula']}) - Grupo {rep['grupo']}\n"
                response += f"    Asignatura: {rep['asignatura']} - Calificacion: {rep['calificacion_final']}\n"
        
        dispatcher.utter_message(text=response)
        return []

class ActionGetStudentDetailedInfo(Action):
    def name(self) -> Text:
        return "action_get_student_detailed_info"

    def run(self, dispatcher: CollectingDispatcher, tracker: Tracker, domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        matricula = next(tracker.get_latest_entity_values("matricula"), None)
        
        if not matricula:
            dispatcher.utter_message(text="Necesito la matricula del estudiante para consultar su información detallada.")
            return []
        
        query_alumno = """
        SELECT a.matricula, u.nombre, u.apellido, u.correo,
               a.cuatrimestre_actual, a.fecha_ingreso, a.telefono,
               a.estado_alumno, c.nombre as carrera,
               g.codigo as grupo_actual,
               a.tutor_nombre, a.tutor_telefono,
               CONCAT(pt.nombre, ' ', pt.apellido) as tutor_grupo,
               pt_prof.numero_empleado as tutor_empleado
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        LEFT JOIN profesores pt_prof ON g.profesor_tutor_id = pt_prof.id
        LEFT JOIN usuarios pt ON pt_prof.usuario_id = pt.id
        WHERE a.matricula = %s
        LIMIT 1
        """
        
        alumno_info = execute_query(query_alumno, (matricula,))
        
        if not alumno_info or isinstance(alumno_info, dict) or not alumno_info:
            dispatcher.utter_message(text=f"No encontré información para el estudiante con matricula {matricula}.")
            return []
        
        info_alumno = alumno_info[0]
        
        query_asignaturas = """
        SELECT DISTINCT asig.nombre as asignatura
        FROM alumnos a
        JOIN calificaciones cal ON a.id = cal.alumno_id
        JOIN asignaturas asig ON cal.asignatura_id = asig.id
        WHERE a.matricula = %s
        ORDER BY asig.nombre
        """
        
        asignaturas = execute_query(query_asignaturas, (matricula,))
        
        query_calificaciones = """
        SELECT 
            asig.nombre as asignatura,
            cal.calificacion_final,
            cal.estatus,
            cal.ciclo_escolar,
            g.codigo as grupo,
            CONCAT(up.nombre, ' ', up.apellido) as profesor
        FROM alumnos a
        JOIN calificaciones cal ON a.id = cal.alumno_id
        JOIN asignaturas asig ON cal.asignatura_id = asig.id
        JOIN grupos g ON cal.grupo_id = g.id
        JOIN profesores p ON cal.profesor_id = p.id
        JOIN usuarios up ON p.usuario_id = up.id
        WHERE a.matricula = %s
        ORDER BY cal.ciclo_escolar DESC, asig.nombre
        """
        
        calificaciones = execute_query(query_calificaciones, (matricula,))
        
        query_indices = """
        SELECT 
            COUNT(cal.id) as total_materias,
            COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) as aprobadas,
            COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) as reprobadas,
            COUNT(CASE WHEN cal.estatus = 'cursando' THEN 1 END) as cursando,
            ROUND((COUNT(CASE WHEN cal.estatus = 'aprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2) as indice_aprobacion,
            ROUND((COUNT(CASE WHEN cal.estatus = 'reprobado' THEN 1 END) * 100.0 / COUNT(cal.id)), 2) as indice_reprobacion
        FROM alumnos a
        JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE a.matricula = %s
        """
        
        indices = execute_query(query_indices, (matricula,))
        
        response = f"Información detallada del estudiante {info_alumno['nombre']} {info_alumno['apellido']}:\n\n"
        response += f"Matricula: {info_alumno['matricula']}\n"
        response += f"Correo: {info_alumno['correo']}\n"
        response += f"Estado: {info_alumno['estado_alumno']}\n"
        response += f"Carrera: {info_alumno['carrera']}\n"
        response += f"Cuatrimestre: {info_alumno['cuatrimestre_actual']}\n"
        response += f"Grupo: {info_alumno['grupo_actual'] or 'Sin asignar'}\n"
        response += f"Fecha ingreso: {info_alumno['fecha_ingreso']}\n"
        
        if info_alumno['telefono']:
            response += f"Telefono: {info_alumno['telefono']}\n"
        
        if info_alumno['tutor_grupo']:
            response += f"Tutor del grupo: {info_alumno['tutor_grupo']}\n"
            response += f"Matricula del tutor: {info_alumno['tutor_empleado']}\n"
        else:
            response += f"Tutor del grupo: Sin asignar\n"
        
        if info_alumno['tutor_nombre']:
            response += f"Tutor personal: {info_alumno['tutor_nombre']}\n"
        if info_alumno['tutor_telefono']:
            response += f"Telefono tutor personal: {info_alumno['tutor_telefono']}\n"
        
        response += "\n"
        
        if indices and not isinstance(indices, dict) and indices:
            ind = indices[0]
            response += f"Indices academicos:\n"
            response += f"Total materias: {ind['total_materias']}\n"
            response += f"Aprobadas: {ind['aprobadas']}\n"
            response += f"Reprobadas: {ind['reprobadas']}\n"
            response += f"Cursando: {ind['cursando']}\n"
            response += f"Indice de aprobacion: {ind['indice_aprobacion']}%\n"
            response += f"Indice de reprobacion: {ind['indice_reprobacion']}%\n\n"
        
        if asignaturas and not isinstance(asignaturas, dict):
            response += f"Asignaturas ({len(asignaturas)}):\n"
            for asig in asignaturas:
                response += f"  - {asig['asignatura']}\n"
            response += "\n"
        
        if calificaciones and not isinstance(calificaciones, dict):
            response += f"Calificaciones y asignaturas ({len(calificaciones)} registros):\n"
            current_cycle = None
            for cal in calificaciones:
                if current_cycle != cal['ciclo_escolar']:
                    current_cycle = cal['ciclo_escolar']
                    response += f"\n{current_cycle}:\n"
                
                response += f"  - {cal['asignatura']}: {cal['calificacion_final']} ({cal['estatus']})\n"
                response += f"    Grupo: {cal['grupo']} - Profesor: {cal['profesor']}\n"
        
        dispatcher.utter_message(text=response)
        return []