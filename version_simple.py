import mysql.connector
import json
import re
from flask import Flask, request, jsonify
from flask_cors import CORS

app = Flask(__name__)
CORS(app)

config = {
    'host': 'bluebyte.space',
    'user': 'bluebyte_angel',
    'password': 'orbitalsoft',
    'database': 'bluebyte_dtai_web',
    'port': 3306
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
        print(f"Error en consulta: {str(e)}")
        return {"error": f"Error al consultar la base de datos: {str(e)}"}

def detect_intent(message):
    message = message.lower()
    
    if any(word in message for word in ['hola', 'buenos dias', 'buenas tardes', 'hey', 'saludos']):
        return 'greeting'
    elif any(word in message for word in ['adios', 'hasta luego', 'bye', 'nos vemos', 'chao']):
        return 'goodbye'
    elif any(phrase in message for phrase in ['cuantos alumnos hay actualmente por carrera', 'alumnos por carrera y cuatrimestre', 'estudiantes por carrera cuatrimestre', 'cuantos alumnos hay por carrera']):
        return 'students_by_career_semester'
    elif any(phrase in message for phrase in ['que alumnos tienen estado inactivo', 'alumnos inactivos', 'estudiantes inactivos', 'alumnos con estado inactivo']):
        return 'inactive_students'
    elif any(phrase in message for phrase in ['cuales son los alumnos con calificacion menor a 8', 'alumnos con calificacion menor', 'estudiantes con calificaciones bajas', 'alumnos reprobados', 'calificaciones menores a 8']):
        return 'low_grade_students'
    elif any(phrase in message for phrase in ['que alumnos obtuvieron las mejores calificaciones en este cuatrimestre', 'mejores calificaciones este cuatrimestre', 'mejores estudiantes', 'alumnos destacados']):
        return 'top_students_current'
    elif any(phrase in message for phrase in ['mejores calificaciones cuatrimestre', 'mejores estudiantes cuatrimestre', 'obtuvieron las mejores calificaciones en este cuatrimestre']) and re.search(r'\b\d+\b', message):
        return 'top_students_semester'
    elif any(phrase in message for phrase in ['cuales son los alumnos con mas de 2 reportes', 'mas de 2 reportes', 'alumnos con reportes de riesgo', 'multiples reportes riesgo']):
        return 'high_risk_students'
    elif any(phrase in message for phrase in ['cuales son los alumnos que no estan asignados', 'no estan asignados grupo', 'sin grupo asignado', 'alumnos sin grupo']):
        return 'students_without_group'
    elif any(phrase in message for phrase in ['cuantos profesores estan activos por carrera', 'profesores activos por carrera', 'cuantos profesores por carrera', 'docentes por carrera']):
        return 'teachers_by_career'
    elif any(phrase in message for phrase in ['que profesores estan asignados como tutores', 'profesores tutores', 'tutores de grupo', 'profesores asignados tutores']):
        return 'teacher_tutors'
    elif any(phrase in message for phrase in ['que profesores tienen mas grupos', 'profesores mas grupos', 'profesores mas asignaturas', 'docentes con mas carga']):
        return 'teachers_most_load'
    elif any(phrase in message for phrase in ['que profesores han registrado mas calificaciones', 'profesores calificaciones bajas', 'profesores con reprobados', 'docentes menor igual 7']):
        return 'teachers_low_grades'
    elif any(phrase in message for phrase in ['dame toda la informacion de este profesor', 'informacion profesor', 'datos profesor', 'todo sobre profesor']) and re.search(r'\b(?=.*[a-zA-Z])(?=.*\d)[a-zA-Z0-9]{3,10}\b', message):
        return 'teacher_complete_info'
    elif any(phrase in message for phrase in ['dame toda la informacion de este alumno', 'informacion alumno', 'datos alumno', 'todo sobre alumno']) and re.search(r'\b\d{10}\b', message):
        return 'student_complete_info'
    elif any(phrase in message for phrase in ['que aulas se usan con mas frecuencia', 'aulas mas frecuencia', 'aulas mas usadas', 'salones mas utilizados']):
        return 'most_used_classrooms'
    elif any(phrase in message for phrase in ['que grupos hay', 'cuales son los grupos', 'lista grupos']):
        return 'all_groups'
    elif any(phrase in message for phrase in ['cuantos grupos activos hay por carrera', 'grupos activos por carrera', 'cuantos grupos por carrera', 'grupos por carrera cuatrimestre']):
        return 'groups_by_career_semester'
    elif any(phrase in message for phrase in ['que grupos no tienen tutor asignado', 'grupos sin tutor', 'grupos no tienen tutor', 'sin tutor asignado']):
        return 'groups_without_tutor'
    elif any(phrase in message for phrase in ['quien es el tutor de este grupo', 'tutor de grupo', 'quien es tutor']) and re.search(r'\b[a-zA-Z]{3,4}\d{2}\b', message):
        return 'group_tutor'
    elif any(phrase in message for phrase in ['que grupos tienen este tutor', 'grupos de tutor', 'grupos tiene tutor']) and (re.search(r'\bEMP\d+\b', message) or len([w for w in message.split() if len(w) > 3]) > 2):
        return 'tutor_groups'
    elif any(phrase in message for phrase in ['que grupos tienen mayor numero de reportes', 'grupos mayor reportes', 'grupos mas reportes riesgo', 'grupos con mas riesgo']):
        return 'groups_most_risk'
    elif any(phrase in message for phrase in ['que noticias han sido mas vistas', 'noticias mas vistas', 'noticias populares', 'noticias con mas vistas']):
        return 'most_viewed_news'
    elif any(phrase in message for phrase in ['que grupos tienen promedio final mas bajo', 'grupos promedio bajo', 'grupos calificaciones bajas', 'grupos peor rendimiento']):
        return 'groups_lowest_average'
    elif any(phrase in message for phrase in ['que asignaturas tienen calificaciones aun sin registrar', 'asignaturas sin calificaciones', 'materias sin registrar', 'calificaciones sin registrar']):
        return 'subjects_no_grades'
    elif any(phrase in message for phrase in ['cuales son las categorias del foro', 'categorias foro', 'categorias del foro', 'que categorias foro']):
        return 'forum_categories'
    elif any(phrase in message for phrase in ['cuantas publicaciones hay por categoria', 'publicaciones por categoria', 'posts por categoria foro', 'cuantas publicaciones categoria']):
        return 'posts_by_category'
    elif any(phrase in message for phrase in ['dame todos los grupos y su numero de alumnos', 'todos grupos numero alumnos', 'grupos con alumnos', 'grupos y estudiantes']):
        return 'all_groups_student_count'
    elif any(phrase in message for phrase in ['que grupos son los que tienen menos alumnos', 'grupos menos alumnos', 'grupos con pocos estudiantes', 'grupos menor ocupacion']):
        return 'groups_least_students'
    elif any(phrase in message for phrase in ['cual es la calificacion promedio por grupo', 'calificacion promedio grupo', 'promedio por grupo', 'rendimiento grupos']):
        return 'group_average'
    elif any(phrase in message for phrase in ['cuantos alumnos aprobaron en ordinario', 'aprobaron ordinario', 'sep-dic 2024', 'aprobacion ordinario']):
        return 'students_passed_ordinary'
    elif any(phrase in message for phrase in ['que profesores estan inactivos actualmente', 'profesores inactivos', 'docentes inactivos', 'profesores no activos']):
        return 'inactive_teachers'
    elif any(phrase in message for phrase in ['cuantos profesores hay por carrera', 'cuantos profesores carrera', 'profesores por cada carrera', 'docentes carrera']):
        return 'teachers_count_by_career'
    elif any(phrase in message for phrase in ['que profesor tiene mas asignaturas asignadas', 'profesor mas asignaturas', 'docente mas materias', 'mayor carga academica']):
        return 'teacher_most_subjects'
    elif any(phrase in message for phrase in ['que carrera tiene mas profesores asignados', 'carrera mas profesores', 'carrera mayor docentes', 'carreras con profesores']):
        return 'career_most_teachers'
    elif any(phrase in message for phrase in ['cuantos alumnos hay por cada cuatrimestre', 'alumnos por cuatrimestre carrera', 'estudiantes cuatrimestre cada carrera']):
        return 'students_by_semester_career'
    elif any(phrase in message for phrase in ['cuantos grupos se han creado por carrera', 'grupos creados carrera', 'cuantos grupos carrera', 'grupos por cada carrera']):
        return 'groups_created_by_career'
    elif any(phrase in message for phrase in ['dame todos los grupos ordenados por carreras', 'grupos ordenados carreras', 'todos grupos por carrera', 'listado grupos carrera']):
        return 'all_groups_by_career'
    elif any(phrase in message for phrase in ['cuantos alumnos hay en total en el sistema', 'total estudiantes sistema', 'cuantos alumnos total', 'total alumnos sistema']):
        return 'total_students_system'
    elif any(phrase in message for phrase in ['cuantos profesores activos hay actualmente', 'profesores activos total', 'cuantos profesores activos', 'total docentes activos']):
        return 'total_active_teachers'
    elif any(phrase in message for phrase in ['estadisticas generales', 'resumen sistema', 'dashboard general']):
        return 'general_system_stats'
    elif any(phrase in message for phrase in ['recomendaciones', 'que recomiendas', 'sugerencias']):
        return 'recommendations'
    elif any(phrase in message for phrase in ['horarios de clases', 'horario del grupo', 'que horarios tienen', 'calendario de clases']):
        return 'class_schedules'
    elif any(phrase in message for phrase in ['asignaturas por carrera', 'materias de la carrera', 'que materias hay']):
        return 'subjects_by_career'
    elif any(phrase in message for phrase in ['profesores sin asignar', 'docentes disponibles', 'profesores libres']):
        return 'available_teachers'
    elif any(phrase in message for phrase in ['estadisticas de asistencia', 'quien falta mas', 'ausencias por grupo']):
        return 'attendance_stats'
    elif any(phrase in message for phrase in ['calificaciones por periodo', 'notas del ciclo', 'rendimiento del semestre']):
        return 'grades_by_period'
    elif any(phrase in message for phrase in ['alumnos destacados historicamente', 'mejores promedios historicos', 'honor roll']):
        return 'historical_top_students'
    elif any(phrase in message for phrase in ['profesores nuevos', 'docentes recientes', 'contrataciones recientes']):
        return 'recent_teachers'
    elif any(phrase in message for phrase in ['grupos con bajo rendimiento', 'grupos problematicos', 'rendimiento por grupo']):
        return 'underperforming_groups'
    elif any(phrase in message for phrase in ['materias mas reprobadas', 'asignaturas dificiles', 'indices de reprobacion']):
        return 'most_failed_subjects'
    elif any(phrase in message for phrase in ['distribución de edades', 'edades de estudiantes', 'demografia estudiantil']):
        return 'student_demographics'
    elif any(phrase in message for phrase in ['capacidad de aulas', 'ocupacion de salones', 'espacios disponibles']):
        return 'classroom_capacity'
    elif any(phrase in message for phrase in ['eventos proximos', 'calendario escolar', 'fechas importantes']):
        return 'upcoming_events'
    elif any(phrase in message for phrase in ['grupos mas reportes riesgo', 'grupos con mayor numero de reportes']):
        return 'groups_most_risk'
    elif any(phrase in message for phrase in ['grupos promedio final mas bajo', 'grupos con promedio bajo']):
        return 'groups_lowest_average'
    elif any(phrase in message for phrase in ['cuantos alumnos hay actualmente por carrera y cuatrimestre', ]):
        return 'students_by_career_semester'
    elif any(phrase in message for phrase in ['que alumnos tienen estado inactivo', 'alumnos estado inactivo', 'estudiantes inactivos']):
        return 'inactive_students'
    elif any(phrase in message for phrase in ['que alumnos obtuvieron las mejores calificaciones en este cuatrimestre', 'mejores calificaciones cuatrimestre actual', 'top estudiantes cuatrimestre']):
        return 'top_students_current'
    elif any(phrase in message for phrase in ['que alumnos obtuvieron las mejores calificaciones en este cuatrimestre 3', 'mejores calificaciones cuatrimestre 3', 'top estudiantes 3']) and re.search(r'\b3\b', message):
        return 'top_students_semester'
    elif any(phrase in message for phrase in ['cuales son los alumnos con mas de 2 reportes de riesgo', 'alumnos multiples reportes riesgo', 'estudiantes alto riesgo']):
        return 'high_risk_students'
    elif any(phrase in message for phrase in ['cuales son los alumnos que no estan asignados a ningun grupo', 'alumnos sin grupo', 'estudiantes sin asignar']):
        return 'students_without_group'
    elif any(phrase in message for phrase in ['cuantos profesores estan activos por carrera', 'profesores activos carrera', 'docentes por carrera']):
        return 'teachers_by_career'
    elif any(phrase in message for phrase in ['que profesores tienen mas grupos o asignaturas', 'profesores mayor carga', 'docentes mas asignaciones']):
        return 'teachers_most_load'
    elif any(phrase in message for phrase in ['que profesores han registrado mas calificaciones con resultado menor', 'profesores calificaciones bajas', 'docentes reprobacion']):
        return 'teachers_low_grades'
    elif any(phrase in message for phrase in ['dame informacion de este profesor', 'informacion completa profesor']) and re.search(r'\b(?=.*[a-zA-Z])(?=.*\d)[a-zA-Z0-9]{3,10}\b', message):
        return 'teacher_complete_info'
    elif any(phrase in message for phrase in ['que aulas se usan con mas frecuencia', 'aulas frecuentes', 'salones mas usados']):
        return 'most_used_classrooms'
    elif any(phrase in message for phrase in ['cuantos grupos activos hay por carrera y cuatrimestre', 'grupos carrera cuatrimestre', 'distribucion grupos']):
        return 'groups_by_career_semester'
    elif any(phrase in message for phrase in ['que grupos tienen este tutor', 'grupos del tutor']) and (re.search(r'\bEMP\d+\b', message) or len([w for w in message.split() if len(w) > 3]) > 2):
        return 'tutor_groups'
    elif any(phrase in message for phrase in ['que grupos tienen mayor numero de reportes de riesgo', 'grupos riesgo alto', 'grupos problematicos']):
        return 'groups_most_risk'
    elif any(phrase in message for phrase in ['que noticias han sido mas vistas', 'noticias populares', 'noticias top']):
        return 'most_viewed_news'
    elif any(phrase in message for phrase in ['que grupos tienen promedio final mas bajo', 'grupos bajo rendimiento', 'grupos peor promedio']):
        return 'groups_lowest_average'
    elif any(phrase in message for phrase in ['que asignaturas tienen calificaciones aun sin registrar', 'materias sin calificar', 'asignaturas pendientes']):
        return 'subjects_no_grades'
    elif any(phrase in message for phrase in ['cuales son las categorias del foro', 'categorias foro', 'secciones foro']):
        return 'forum_categories'
    elif any(phrase in message for phrase in ['cuantas publicaciones hay por categoria de foro', 'posts por categoria', 'publicaciones categoria']):
        return 'posts_by_category'
    elif any(phrase in message for phrase in ['cuantos alumnos aprobaron en ordinario en el cuatrimestre', 'aprobacion ordinario', 'sep-dic 2024']):
        return 'students_passed_ordinary'
    elif any(phrase in message for phrase in ['que profesores estan inactivos actualmente', 'profesores inactivos', 'docentes no activos']):
        return 'inactive_teachers'
    elif any(phrase in message for phrase in ['cuantos profesores hay por carrera', 'profesores cada carrera', 'conteo docentes carrera']):
        return 'teachers_count_by_career'
    elif any(phrase in message for phrase in ['que profesor tiene mas asignaturas asignadas', 'profesor mas materias', 'docente mayor carga']):
        return 'teacher_most_subjects'
    elif any(phrase in message for phrase in ['que carrera tiene mas profesores asignados', 'carrera mas docentes', 'programa mas profesores']):
        return 'career_most_teachers'
    elif any(phrase in message for phrase in ['cuantos alumnos hay por cada cuatrimestre en una carrera', 'estudiantes por cuatrimestre', 'distribucion cuatrimestral']):
        return 'students_by_semester_career'
    elif any(phrase in message for phrase in ['cuantos grupos se han creado por carrera', 'grupos creados carrera', 'total grupos carrera']):
        return 'groups_created_by_career'
    elif any(phrase in message for phrase in ['cuantos alumnos hay en total en el sistema', 'total estudiantes', 'cantidad total alumnos']):
        return 'total_students_system'
    elif any(phrase in message for phrase in ['cuantos profesores activos hay actualmente', 'total profesores activos', 'docentes activos total']):
        return 'total_active_teachers'
    
    
    elif any(word in message for word in ['adios', 'hasta luego', 'bye', 'nos vemos', 'chao']):
        return 'goodbye'
    elif any(phrase in message for phrase in ['cuantos alumnos hay actualmente por carrera y cuatrimestre', 'cuantos alumnos hay actualmente por carrera', 'distribucion alumnos carrera cuatrimestre']):
        return 'students_by_career_semester'
    elif any(phrase in message for phrase in ['que alumnos tienen estado inactivo', 'alumnos tienen estado inactivo']):
        return 'inactive_students'
    elif any(phrase in message for phrase in ['que alumnos obtuvieron las mejores calificaciones en este cuatrimestre', 'mejores calificaciones en este cuatrimestre', 'mejores calificaciones este cuatrimestre']) and not re.search(r'\b\d+\b', message):
        return 'top_students_current'
    elif any(phrase in message for phrase in ['que alumnos obtuvieron las mejores calificaciones en este cuatrimestre 3', 'mejores calificaciones cuatrimestre 3']) and re.search(r'\b3\b', message):
        return 'top_students_semester'
    elif any(phrase in message for phrase in ['cuales son los alumnos con mas de 2 reportes de riesgo activos', 'alumnos con mas de 2 reportes']):
        return 'high_risk_students'
    elif any(phrase in message for phrase in ['cuales son los alumnos que no estan asignados a ningun grupo', 'alumnos que no estan asignados']):
        return 'students_without_group'
    elif any(phrase in message for phrase in ['cuantos profesores estan activos por carrera', 'profesores estan activos por carrera']):
        return 'teachers_by_career'
    elif any(phrase in message for phrase in ['que profesores tienen mas grupos o asignaturas asignadas', 'profesores tienen mas grupos']):
        return 'teachers_most_load'
    elif any(phrase in message for phrase in ['que profesores han registrado mas calificaciones con resultado menor o igual a 7', 'profesores han registrado mas calificaciones']):
        return 'teachers_low_grades'
    elif any(phrase in message for phrase in ['dame toda la informacion de este profesor', 'toda la informacion de este profesor']) and re.search(r'\b(?=.*[a-zA-Z])(?=.*\d)[a-zA-Z0-9]{3,10}\b', message):
        return 'teacher_complete_info'
    elif any(phrase in message for phrase in ['que aulas se usan con mas frecuencia durante la semana', 'aulas se usan con mas frecuencia']):
        return 'most_used_classrooms'
    elif any(phrase in message for phrase in ['cuantos grupos activos hay por carrera y cuatrimestre', 'grupos activos hay por carrera']):
        return 'groups_by_career_semester'
    elif any(phrase in message for phrase in ['que grupos tienen este tutor', 'grupos tienen este tutor']) and (re.search(r'\bEMP\d+\b', message) or len([w for w in message.split() if len(w) > 3 and w not in ['que', 'grupos', 'tienen', 'este', 'tutor']]) >= 2):
        return 'tutor_groups'
    elif any(phrase in message for phrase in ['que grupos tienen mayor numero de reportes de riesgo por alumno', 'grupos tienen mayor numero de reportes']):
        return 'groups_most_risk'
    elif any(phrase in message for phrase in ['que noticias han sido mas vistas por los usuarios', 'noticias han sido mas vistas']):
        return 'most_viewed_news'
    elif any(phrase in message for phrase in ['que grupos tienen promedio final mas bajo', 'grupos tienen promedio final mas bajo']):
        return 'groups_lowest_average'
    elif any(phrase in message for phrase in ['que asignaturas tienen calificaciones aun sin registrar', 'asignaturas tienen calificaciones aun sin registrar']):
        return 'subjects_no_grades'
    elif any(phrase in message for phrase in ['cuales son las categorias del foro', 'categorias del foro']):
        return 'forum_categories'
    elif any(phrase in message for phrase in ['cuantas publicaciones hay por categoria de foro', 'publicaciones hay por categoria de foro']):
        return 'posts_by_category'
    elif any(phrase in message for phrase in ['cuantos alumnos aprobaron en ordinario en el cuatrimestre', 'alumnos aprobaron en ordinario']):
        return 'students_passed_ordinary'
    elif any(phrase in message for phrase in ['que profesores estan inactivos actualmente', 'profesores estan inactivos actualmente']):
        return 'inactive_teachers'
    elif any(phrase in message for phrase in ['cuantos profesores hay por carrera', 'profesores hay por carrera']):
        return 'teachers_count_by_career'
    elif any(phrase in message for phrase in ['que profesor tiene mas asignaturas asignadas', 'profesor tiene mas asignaturas']):
        return 'teacher_most_subjects'
    elif any(phrase in message for phrase in ['que carrera tiene mas profesores asignados', 'carrera tiene mas profesores']):
        return 'career_most_teachers'
    elif any(phrase in message for phrase in ['cuantos alumnos hay por cada cuatrimestre en una carrera', 'alumnos hay por cada cuatrimestre']):
        return 'students_by_semester_career'
    elif any(phrase in message for phrase in ['cuantos grupos se han creado por carrera', 'grupos se han creado por carrera']):
        return 'groups_created_by_career'
    elif any(phrase in message for phrase in ['cuantos alumnos hay en total en el sistema', 'alumnos hay en total en el sistema']):
        return 'total_students_system'
    elif any(phrase in message for phrase in ['cuantos profesores activos hay actualmente', 'profesores activos hay actualmente']):
        return 'total_active_teachers'
    elif any(phrase in message for phrase in ['estadisticas generales', 'resumen sistema', 'dashboard general']):
        return 'general_system_stats'
    elif any(phrase in message for phrase in ['recomendaciones', 'que recomiendas', 'sugerencias']):
        return 'recommendations'
    elif any(phrase in message for phrase in ['posts mas populares del foro', 'publicaciones populares foro', 'posts con mas vistas foro']):
        return 'most_popular_forum_posts'
    elif any(phrase in message for phrase in ['estadisticas del foro', 'estadisticas generales foro', 'resumen del foro']):
        return 'forum_general_stats'
    else:
        return 'unknown'
   
def extract_entities(message, intent):
    entities = {}
    
    if intent == 'teacher_complete_info':
        emp_match = re.search(r'\b(?=.*[A-Za-z])(?=.*\d)[A-Za-z\d]+\b', message)
        if emp_match:
            entities['numero_empleado'] = emp_match.group()
    
    if intent == 'student_complete_info':
        matricula_match = re.search(r'\b\d{10}\b', message)
        if matricula_match:
            entities['matricula'] = matricula_match.group()
    
    if intent == 'group_tutor':
        grupo_match = re.search(r'\b[a-zA-Z]{3,4}\d{2}\b', message)
        if grupo_match:
            entities['grupo'] = grupo_match.group()
    
    if intent == 'tutor_groups':
        emp_match = re.search(r'\b(?=.*[A-Za-z])(?=.*\d)[A-Za-z\d]+\b', message)
        if emp_match:
            entities['numero_empleado'] = emp_match.group()
        else:
            words = [w for w in message.split() if len(w) > 3 and w not in ['grupos', 'tiene', 'tutor', 'este', 'cual', 'cuales']]
            if words:
                entities['nombre_tutor'] = ' '.join(words[:2])
    
    if intent == 'top_students_semester':
        cuatrimestre_match = re.search(r'\b(\d+)\b', message)
        if cuatrimestre_match:
            entities['cuatrimestre'] = cuatrimestre_match.group()
    if intent == 'class_schedules':
        grupo_match = re.search(r'\b[a-zA-Z]{3,4}\d{2}\b', message)
        if grupo_match:
            entities['grupo'] = grupo_match.group()

    if intent == 'subjects_by_career':
        carrera_words = ['sistemas', 'industrial', 'mecatronica', 'gastronomia', 'turismo']
        for word in carrera_words:
            if word in message:
                entities['carrera'] = word
                break
    return entities

def generate_recommendations():
    query_stats = """
    SELECT 
        (SELECT COUNT(*) FROM alumnos WHERE estado_alumno = 'activo') as estudiantes_activos,
        (SELECT COUNT(*) FROM profesores WHERE activo = TRUE) as profesores_activos,
        (SELECT COUNT(*) FROM reportes_riesgo WHERE estado IN ('abierto', 'en_proceso')) as reportes_riesgo,
        (SELECT COUNT(*) FROM solicitudes_ayuda WHERE estado = 'pendiente') as ayudas_pendientes,
        (SELECT AVG(promedio_general) FROM alumnos WHERE estado_alumno = 'activo') as promedio_general
    """
    
    query_low_performance = """
    SELECT c.nombre as carrera, COUNT(*) as estudiantes_bajo_rendimiento
    FROM calificaciones cal
    JOIN alumnos a ON cal.alumno_id = a.id
    JOIN grupos g ON cal.grupo_id = g.id
    JOIN carreras c ON g.carrera_id = c.id
    WHERE cal.calificacion_final < 7 AND cal.calificacion_final IS NOT NULL
    GROUP BY c.id, c.nombre
    ORDER BY estudiantes_bajo_rendimiento DESC
    LIMIT 3
    """
    
    query_high_risk = """
    SELECT COUNT(*) as total_alto_riesgo
    FROM (
        SELECT a.id
        FROM alumnos a
        JOIN reportes_riesgo rr ON a.id = rr.alumno_id
        WHERE rr.estado IN ('abierto', 'en_proceso')
        GROUP BY a.id
        HAVING COUNT(rr.id) > 2
    ) as alto_riesgo
    """
    
    stats = execute_query(query_stats)
    low_performance = execute_query(query_low_performance)
    high_risk = execute_query(query_high_risk)
    
    recommendations = []
    
    if stats and not isinstance(stats, dict) and stats:
        data = stats[0]
        
        if data['reportes_riesgo'] > 0:
            recommendations.append(f"Se detectaron {data['reportes_riesgo']} reportes de riesgo activos. Recomiendo revisar y dar seguimiento prioritario a estos casos.")
        
        if data['ayudas_pendientes'] > 0:
            recommendations.append(f"Hay {data['ayudas_pendientes']} solicitudes de ayuda pendientes. Sugiero asignar personal para atender estas solicitudes.")
        
        ratio = data['estudiantes_activos'] / data['profesores_activos'] if data['profesores_activos'] > 0 else 0
        if ratio > 30:
            recommendations.append(f"El ratio estudiante-profesor es de {ratio:.1f}:1, lo cual es alto. Considere contratar más profesores para mejorar la atención personalizada.")
        
        if data['promedio_general'] and data['promedio_general'] < 7.5:
            recommendations.append(f"El promedio general del sistema es {data['promedio_general']:.2f}. Recomiendo implementar programas de apoyo académico y tutorías.")
    
    if low_performance and not isinstance(low_performance, dict):
        recommendations.append("Carreras que requieren atención especial por bajo rendimiento:")
        for row in low_performance:
            recommendations.append(f"  - {row['carrera']}: {row['estudiantes_bajo_rendimiento']} estudiantes con calificaciones menores a 7")
    
    if high_risk and not isinstance(high_risk, dict) and high_risk[0]['total_alto_riesgo'] > 0:
        recommendations.append(f"Hay {high_risk[0]['total_alto_riesgo']} estudiantes con múltiples reportes de riesgo que requieren intervención inmediata.")
    
    if not recommendations:
        recommendations.append("El sistema muestra indicadores saludables en general. Continúe monitoreando el rendimiento académico y mantenga los programas de apoyo estudiantil.")
    
    return recommendations

def generate_response(intent, entities, message):
    if intent == 'greeting':
        return "Hola, soy tu asistente académico inteligente. Puedo ayudarte con consultas sobre estudiantes, profesores, grupos, calificaciones y estadísticas del sistema educativo. ¿Qué información necesitas?"
    
    elif intent == 'goodbye':
        return "Ha sido un placer ayudarte con la información académica. Que tengas un excelente día."
    
    elif intent == 'unknown':
        return "No tengo programada esa consulta específica en mi sistema. Puedo ayudarte con información sobre estudiantes por carrera, profesores activos, grupos, calificaciones, reportes de riesgo y estadísticas generales. ¿Podrías intentar con una pregunta diferente?"
    elif intent == 'recommendations':
        recommendations = generate_recommendations()
        response = "Basado en el análisis de los datos actuales del sistema, aquí tienes mis recomendaciones:\n\n"
        for i, rec in enumerate(recommendations, 1):
            if rec.startswith("  -"):
                response += f"{rec}\n"
            else:
                response += f"{i}. {rec}\n"
        
        response += "\nEstas recomendaciones están basadas en indicadores clave de rendimiento académico, reportes de riesgo y estadísticas del sistema."
        return response
    
    elif intent == 'students_by_career_semester':
        query = """
        SELECT c.nombre as carrera, a.cuatrimestre_actual,
               COUNT(a.id) as total_alumnos,
               SUM(CASE WHEN a.estado_alumno = 'activo' THEN 1 ELSE 0 END) as activos
        FROM carreras c
        LEFT JOIN alumnos a ON c.id = a.carrera_id
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre, a.cuatrimestre_actual
        HAVING total_alumnos > 0
        ORDER BY c.nombre, a.cuatrimestre_actual
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Te muestro la distribución de alumnos por carrera y cuatrimestre:\n\n"
            current_career = None
            total_general = 0
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  {row['cuatrimestre_actual']} cuatrimestre: {row['total_alumnos']} alumnos ({row['activos']} activos)\n"
                total_general += row['total_alumnos']
            
            response += f"\nEn total tenemos {total_general} estudiantes distribuidos en el sistema."
            return response
        else:
            return "No hay suficientes datos de estudiantes por carrera en la base de datos actualmente."
    
    elif intent == 'inactive_students':
        query = """
        SELECT a.matricula, u.nombre, u.apellido, u.correo, c.nombre as carrera,
               a.cuatrimestre_actual, a.estado_alumno, a.fecha_ingreso,
               a.promedio_general, a.tutor_nombre, a.telefono
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        WHERE a.estado_alumno != 'activo'
        ORDER BY c.nombre, a.estado_alumno, u.apellido
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "Excelente noticia: todos los alumnos registrados están activos actualmente."
            
            response = f"Encontré {len(result)} alumnos con estado inactivo:\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  Matrícula: {row['matricula']}\n"
                response += f"  Nombre: {row['nombre']} {row['apellido']}\n"
                response += f"  Correo: {row['correo']}\n"
                response += f"  Estado: {row['estado_alumno']}\n"
                response += f"  Cuatrimestre: {row['cuatrimestre_actual']}\n"
                response += f"  Promedio: {row['promedio_general']}\n"
                response += f"  Fecha ingreso: {row['fecha_ingreso']}\n"
                if row['tutor_nombre']:
                    response += f"  Tutor: {row['tutor_nombre']}\n"
                if row['telefono']:
                    response += f"  Teléfono: {row['telefono']}\n"
                response += "\n"
            
            return response
        else:
            return "No hay datos suficientes de estudiantes inactivos o hay un problema con la consulta en la base de datos."
    
    elif intent == 'low_grade_students':
        query = """
        SELECT DISTINCT a.matricula, u.nombre, u.apellido, g.codigo as grupo,
               asig.nombre as asignatura, c.calificacion_final, g.cuatrimestre,
               car.nombre as carrera
        FROM calificaciones c
        JOIN alumnos a ON c.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN grupos g ON c.grupo_id = g.id
        JOIN asignaturas asig ON c.asignatura_id = asig.id
        JOIN carreras car ON g.carrera_id = car.id
        WHERE c.calificacion_final < 8 AND c.calificacion_final IS NOT NULL
        ORDER BY g.cuatrimestre, car.nombre, u.apellido, u.nombre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "Excelente desempeño: no hay alumnos con calificaciones menores a 8."
            
            response = f"Encontré {len(result)} registros de alumnos con calificaciones menores a 8:\n\n"
            current_semester = None
            
            for row in result:
                if current_semester != row['cuatrimestre']:
                    current_semester = row['cuatrimestre']
                    response += f"{current_semester} CUATRIMESTRE:\n"
                
                response += f"  {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                response += f"    Grupo: {row['grupo']} | Carrera: {row['carrera']}\n"
                response += f"    Asignatura: {row['asignatura']} | Calificación: {row['calificacion_final']}\n\n"
            
            return response
        else:
            return "No hay suficiente información de calificaciones en la base de datos para esta consulta."
    
    elif intent == 'top_students_current':
        query = """
        SELECT a.matricula, u.nombre, u.apellido, g.codigo as grupo,
               g.cuatrimestre, car.nombre as carrera, AVG(c.calificacion_final) as promedio,
               ROW_NUMBER() OVER (PARTITION BY car.id ORDER BY AVG(c.calificacion_final) DESC) as ranking
        FROM calificaciones c
        JOIN alumnos a ON c.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN grupos g ON c.grupo_id = g.id
        JOIN carreras car ON g.carrera_id = car.id
        WHERE c.calificacion_final IS NOT NULL AND c.ciclo_escolar LIKE '%2024%'
        GROUP BY a.id, a.matricula, u.nombre, u.apellido, g.codigo, g.cuatrimestre, car.nombre, car.id
        HAVING ranking <= 3
        ORDER BY car.nombre, ranking
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No encontré calificaciones registradas para el período actual."
            
            response = "Aquí tienes el top 3 de mejores estudiantes por carrera en el ciclo actual:\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  {row['ranking']}. {row['nombre']} {row['apellido']}\n"
                response += f"     Matrícula: {row['matricula']}\n"
                response += f"     Grupo: {row['grupo']} ({row['cuatrimestre']} cuatrimestre)\n"
                response += f"     Promedio: {row['promedio']:.2f}\n\n"
            
            return response
        else:
            return "No pude obtener la información de mejores estudiantes en este momento."
    
    elif intent == 'top_students_semester':
        cuatrimestre = entities.get('cuatrimestre', '3')
        query = """
        SELECT a.matricula, u.nombre, u.apellido, g.codigo as grupo,
               car.nombre as carrera, AVG(c.calificacion_final) as promedio,
               ROW_NUMBER() OVER (PARTITION BY g.id ORDER BY AVG(c.calificacion_final) DESC) as ranking
        FROM calificaciones c
        JOIN alumnos a ON c.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN grupos g ON c.grupo_id = g.id
        JOIN carreras car ON g.carrera_id = car.id
        WHERE c.calificacion_final IS NOT NULL AND g.cuatrimestre = %s
        GROUP BY a.id, a.matricula, u.nombre, u.apellido, g.codigo, g.id, car.nombre
        HAVING ranking <= 3
        ORDER BY g.codigo, ranking
        """
        result = execute_query(query, (cuatrimestre,))
        if result and not isinstance(result, dict):
            if not result:
                return f"No encontré estudiantes en {cuatrimestre} cuatrimestre."
            
            response = f"Top 3 mejores estudiantes por grupo en {cuatrimestre} cuatrimestre:\n\n"
            current_group = None
            
            for row in result:
                if current_group != row['grupo']:
                    current_group = row['grupo']
                    response += f"Grupo {row['grupo']} ({row['carrera']}):\n"
                
                response += f"  {row['ranking']}. {row['nombre']} {row['apellido']}\n"
                response += f"     Matrícula: {row['matricula']}\n"
                response += f"     Promedio: {row['promedio']:.2f}\n\n"
            
            return response
        else:
            return f"No pude obtener información de {cuatrimestre} cuatrimestre."
    
    elif intent == 'high_risk_students':
        query = """
        SELECT a.matricula, u.nombre, u.apellido, g.codigo as grupo,
               COUNT(rr.id) as total_reportes,
               GROUP_CONCAT(DISTINCT rr.tipo_riesgo) as tipos_riesgo
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        JOIN reportes_riesgo rr ON a.id = rr.alumno_id
        WHERE rr.estado IN ('abierto', 'en_proceso')
        GROUP BY a.id, a.matricula, u.nombre, u.apellido, g.codigo
        HAVING total_reportes > 2
        ORDER BY total_reportes DESC, u.apellido
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "Buenas noticias: no hay estudiantes con más de 2 reportes de riesgo activos."
            
            response = f"Encontré {len(result)} casos críticos de alumnos con más de 2 reportes de riesgo activos:\n\n"
            
            for row in result:
                response += f"Matrícula: {row['matricula']}\n"
                response += f"Nombre: {row['nombre']} {row['apellido']}\n"
                response += f"Grupo: {row['grupo'] or 'Sin asignar'}\n"
                response += f"Total reportes: {row['total_reportes']}\n"
                response += f"Tipos de riesgo: {row['tipos_riesgo']}\n"
                response += "Este caso requiere atención inmediata\n\n"
            
            return response
        else:
            return "No pude obtener información de reportes de riesgo en este momento."
    
    elif intent == 'students_without_group':
        query = """
        SELECT a.matricula, u.nombre, u.apellido, a.cuatrimestre_actual,
               c.nombre as carrera, a.estado_alumno
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
                return "Perfecto: todos los estudiantes activos están asignados a un grupo."
            
            response = f"Encontré {len(result)} alumnos activos sin grupo asignado:\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  Matrícula: {row['matricula']}\n"
                response += f"  Nombre: {row['nombre']} {row['apellido']}\n"
                response += f"  Cuatrimestre: {row['cuatrimestre_actual']}\n"
                response += f"  Estado: {row['estado_alumno']}\n"
                response += f"  NECESITA ASIGNACIÓN DE GRUPO\n\n"
            
            return response
        else:
            return "No pude obtener información de estudiantes sin grupo en este momento."
    
    elif intent == 'teachers_by_career':
        query = """
        SELECT c.nombre as carrera, COUNT(p.id) as total_profesores,
               GROUP_CONCAT(CONCAT(u.nombre, ' ', u.apellido, ' (', p.numero_empleado, ')') SEPARATOR ', ') as profesores
        FROM carreras c
        LEFT JOIN profesores p ON c.id = p.carrera_id AND p.activo = TRUE
        LEFT JOIN usuarios u ON p.usuario_id = u.id
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre
        ORDER BY total_profesores DESC, c.nombre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Te muestro la distribución de profesores activos por carrera:\n\n"
            total_general = 0
            
            for row in result:
                response += f"{row['carrera']}: {row['total_profesores']} profesores\n"
                if row['profesores'] and row['total_profesores'] > 0:
                    response += f"  {row['profesores']}\n"
                response += "\n"
                total_general += row['total_profesores']
            
            response += f"Total en el sistema: {total_general} profesores activos"
            return response
        else:
            return "No hay suficientes datos de profesores por carrera en la base de datos actualmente."
    
    elif intent == 'teacher_tutors':
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
        GROUP BY p.id, p.numero_empleado, u.nombre, u.apellido, g.id, g.codigo, c.nombre, g.cuatrimestre
        ORDER BY c.nombre, g.cuatrimestre, g.codigo
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No hay profesores asignados como tutores de grupo actualmente."
            
            response = f"Te muestro los {len(result)} profesores asignados como tutores de grupo:\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  Profesor {row['nombre']} {row['apellido']} ({row['numero_empleado']})\n"
                response += f"    Grupo: {row['grupo']} - {row['cuatrimestre']} cuatrimestre\n"
                response += f"    Alumnos a cargo: {row['total_alumnos']}\n\n"
            
            return response
        else:
            return "No pude obtener información de profesores tutores en este momento."
    
    elif intent == 'teachers_most_load':
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido,
               COUNT(DISTINCT pag.grupo_id) as total_grupos,
               COUNT(DISTINCT pag.asignatura_id) as total_asignaturas,
               GROUP_CONCAT(DISTINCT g.codigo ORDER BY g.codigo) as grupos,
               GROUP_CONCAT(DISTINCT a.nombre ORDER BY a.nombre) as asignaturas
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN profesor_asignatura_grupo pag ON p.id = pag.profesor_id
        JOIN grupos g ON pag.grupo_id = g.id
        JOIN asignaturas a ON pag.asignatura_id = a.id
        WHERE p.activo = TRUE AND pag.activo = TRUE
        GROUP BY p.id, p.numero_empleado, u.nombre, u.apellido
        ORDER BY (total_grupos + total_asignaturas) DESC, total_grupos DESC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No encontré asignaciones de profesores en este momento."
            
            response = "Aquí tienes los profesores con mayor carga académica:\n\n"
            
            for i, row in enumerate(result, 1):
                carga_total = row['total_grupos'] + row['total_asignaturas']
                response += f"{i}. {row['nombre']} {row['apellido']} ({row['numero_empleado']})\n"
                response += f"   Grupos: {row['total_grupos']} | Asignaturas: {row['total_asignaturas']} | Carga total: {carga_total}\n"
                response += f"   Grupos asignados: {row['grupos']}\n"
                if len(row['asignaturas']) > 100:
                    response += f"   Asignaturas: {row['asignaturas'][:100]}...\n"
                else:
                    response += f"   Asignaturas: {row['asignaturas']}\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de carga académica en este momento."
    
    elif intent == 'teachers_low_grades':
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido,
               COUNT(c.id) as calificaciones_bajas,
               COUNT(DISTINCT a.matricula) as alumnos_afectados,
               GROUP_CONCAT(DISTINCT CONCAT(ua.nombre, ' ', ua.apellido, ' (', a.matricula, ') - ', g.codigo) 
                           ORDER BY ua.apellido SEPARATOR '; ') as detalle_alumnos
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN calificaciones c ON p.id = c.profesor_id
        JOIN alumnos a ON c.alumno_id = a.id
        JOIN usuarios ua ON a.usuario_id = ua.id
        JOIN grupos g ON c.grupo_id = g.id
        WHERE c.calificacion_final <= 7 AND c.calificacion_final IS NOT NULL
        GROUP BY p.id, p.numero_empleado, u.nombre, u.apellido
        ORDER BY calificaciones_bajas DESC, alumnos_afectados DESC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "Excelente: no hay profesores con calificaciones menores o iguales a 7 registradas."
            
            response = "Te muestro los profesores con más calificaciones menores o iguales a 7:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. Profesor {row['nombre']} {row['apellido']} ({row['numero_empleado']})\n"
                response += f"   Calificaciones menores o iguales a 7: {row['calificaciones_bajas']}\n"
                response += f"   Alumnos afectados: {row['alumnos_afectados']}\n"
                if len(row['detalle_alumnos']) > 200:
                    response += f"   Detalle: {row['detalle_alumnos'][:200]}...\n"
                else:
                    response += f"   Detalle: {row['detalle_alumnos']}\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de calificaciones bajas en este momento."
    
    elif intent == 'teacher_complete_info':
        if 'numero_empleado' not in entities:
            return "Para consultar información completa del profesor necesito su número de empleado (por ejemplo: EMP001)."
        
        numero_emp = entities['numero_empleado']
        
        query_basic = """
        SELECT p.numero_empleado, u.nombre, u.apellido, u.correo,
               p.telefono, p.extension, p.fecha_contratacion,
               p.titulo_academico, p.especialidad, p.cedula_profesional,
               p.experiencia_años, p.es_tutor, c.nombre as carrera
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN carreras c ON p.carrera_id = c.id
        WHERE p.numero_empleado = %s AND p.activo = TRUE
        """
        
        query_subjects = """
        SELECT DISTINCT a.nombre as asignatura, g.codigo as grupo,
               g.cuatrimestre, pag.ciclo_escolar
        FROM profesor_asignatura_grupo pag
        JOIN profesores p ON pag.profesor_id = p.id
        JOIN asignaturas a ON pag.asignatura_id = a.id
        JOIN grupos g ON pag.grupo_id = g.id
        WHERE p.numero_empleado = %s AND pag.activo = TRUE
        ORDER BY pag.ciclo_escolar DESC, g.cuatrimestre, a.nombre
        """
        
        query_tutor = """
        SELECT g.codigo as grupo, COUNT(ag.alumno_id) as total_alumnos
        FROM grupos g
        JOIN profesores p ON g.profesor_tutor_id = p.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE p.numero_empleado = %s AND g.activo = TRUE
        GROUP BY g.id, g.codigo
        """
        
        query_grades = """
        SELECT COUNT(*) as total_calificaciones,
               AVG(calificacion_final) as promedio_otorgado,
               COUNT(CASE WHEN calificacion_final < 7 THEN 1 END) as calificaciones_bajas
        FROM calificaciones c
        JOIN profesores p ON c.profesor_id = p.id
        WHERE p.numero_empleado = %s AND c.calificacion_final IS NOT NULL
        """
        
        basic_info = execute_query(query_basic, (numero_emp,))
        subjects_info = execute_query(query_subjects, (numero_emp,))
        tutor_info = execute_query(query_tutor, (numero_emp,))
        grades_info = execute_query(query_grades, (numero_emp,))
        
        if not basic_info or isinstance(basic_info, dict) or not basic_info:
            return f"No encontré información para el profesor {numero_emp}."
        
        data = basic_info[0]
        response = f"INFORMACIÓN COMPLETA DEL PROFESOR\n\n"
        response += f"Datos Personales:\n"
        response += f"  Nombre: {data['nombre']} {data['apellido']}\n"
        response += f"  Número de empleado: {data['numero_empleado']}\n"
        response += f"  Correo: {data['correo']}\n"
        response += f"  Carrera asignada: {data['carrera']}\n"
        
        if data['telefono']:
            response += f"  Teléfono: {data['telefono']}\n"
        if data['extension']:
            response += f"  Extensión: {data['extension']}\n"
        
        response += f"\nDatos Académicos:\n"
        response += f"  Fecha contratación: {data['fecha_contratacion']}\n"
        response += f"  Experiencia: {data['experiencia_años']} años\n"
        
        if data['titulo_academico']:
            response += f"  Título: {data['titulo_academico']}\n"
        if data['especialidad']:
            response += f"  Especialidad: {data['especialidad']}\n"
        if data['cedula_profesional']:
            response += f"  Cédula profesional: {data['cedula_profesional']}\n"
        
        if subjects_info and not isinstance(subjects_info, dict):
            response += f"\nAsignaturas que imparte ({len(subjects_info)}):\n"
            current_cycle = None
            for subj in subjects_info:
                if current_cycle != subj['ciclo_escolar']:
                    current_cycle = subj['ciclo_escolar']
                    response += f"  {current_cycle}:\n"
                response += f"    {subj['asignatura']} - Grupo {subj['grupo']} ({subj['cuatrimestre']} cuatrimestre)\n"
        
        if data['es_tutor'] and tutor_info and not isinstance(tutor_info, dict):
            response += f"\nTutoría de grupos:\n"
            for tutor in tutor_info:
                response += f"  Grupo {tutor['grupo']}: {tutor['total_alumnos']} alumnos\n"
        
        if grades_info and not isinstance(grades_info, dict) and grades_info[0]['total_calificaciones'] > 0:
            grade_data = grades_info[0]
            response += f"\nEstadísticas de calificaciones:\n"
            response += f"  Total calificaciones registradas: {grade_data['total_calificaciones']}\n"
            response += f"  Promedio general otorgado: {grade_data['promedio_otorgado']:.2f}\n"
            response += f"  Calificaciones menores a 7: {grade_data['calificaciones_bajas']}\n"
        
        return response
    
    elif intent == 'student_complete_info':
        if 'matricula' not in entities:
            return "Para consultar información completa del alumno necesito su matrícula (10 dígitos)."
        
        matricula = entities['matricula']
        
        query_basic = """
        SELECT a.matricula, u.nombre, u.apellido, u.correo,
               a.cuatrimestre_actual, a.fecha_ingreso, a.telefono,
               a.estado_alumno, a.promedio_general, c.nombre as carrera,
               a.tutor_nombre, a.tutor_telefono, a.direccion, a.fecha_nacimiento
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        WHERE a.matricula = %s
        """
        
        query_group = """
        SELECT g.codigo as grupo, g.cuatrimestre, g.ciclo_escolar,
               car.nombre as carrera
        FROM alumnos_grupos ag
        JOIN alumnos a ON ag.alumno_id = a.id
        JOIN grupos g ON ag.grupo_id = g.id
        JOIN carreras car ON g.carrera_id = car.id
        WHERE a.matricula = %s AND ag.activo = TRUE
        """
        
        query_grades = """
        SELECT asig.nombre as asignatura, c.calificacion_final,
               c.estatus, c.ciclo_escolar, g.codigo as grupo
        FROM calificaciones c
        JOIN alumnos a ON c.alumno_id = a.id
        JOIN asignaturas asig ON c.asignatura_id = asig.id
        JOIN grupos g ON c.grupo_id = g.id
        WHERE a.matricula = %s AND c.calificacion_final IS NOT NULL
        ORDER BY c.ciclo_escolar DESC, asig.nombre
        LIMIT 10
        """
        
        query_risk = """
        SELECT tipo_riesgo, nivel_riesgo, descripcion, estado,
               DATE_FORMAT(fecha_reporte, '%d/%m/%Y') as fecha
        FROM reportes_riesgo rr
        JOIN alumnos a ON rr.alumno_id = a.id
        WHERE a.matricula = %s AND rr.estado IN ('abierto', 'en_proceso')
        ORDER BY fecha_reporte DESC
        """
        
        basic_info = execute_query(query_basic, (matricula,))
        group_info = execute_query(query_group, (matricula,))
        grades_info = execute_query(query_grades, (matricula,))
        risk_info = execute_query(query_risk, (matricula,))
        
        if not basic_info or isinstance(basic_info, dict) or not basic_info:
            return f"No encontré información para la matrícula {matricula}."
        
        data = basic_info[0]
        response = f"INFORMACIÓN COMPLETA DEL ALUMNO\n\n"
        response += f"Datos Personales:\n"
        response += f"  Nombre: {data['nombre']} {data['apellido']}\n"
        response += f"  Matrícula: {data['matricula']}\n"
        response += f"  Correo: {data['correo']}\n"
        response += f"  Estado: {data['estado_alumno']}\n"
        
        if data['telefono']:
            response += f"  Teléfono: {data['telefono']}\n"
        if data['direccion']:
            response += f"  Dirección: {data['direccion']}\n"
        if data['fecha_nacimiento']:
            response += f"  Fecha nacimiento: {data['fecha_nacimiento']}\n"
        
        response += f"\nDatos Académicos:\n"
        response += f"  Carrera: {data['carrera']}\n"
        response += f"  Cuatrimestre actual: {data['cuatrimestre_actual']}\n"
        response += f"  Fecha ingreso: {data['fecha_ingreso']}\n"
        response += f"  Promedio general: {data['promedio_general']}\n"
        
        if data['tutor_nombre']:
            response += f"  Tutor: {data['tutor_nombre']}\n"
            if data['tutor_telefono']:
                response += f"  Teléfono tutor: {data['tutor_telefono']}\n"
        
        if group_info and not isinstance(group_info, dict):
            response += f"\nGrupo actual:\n"
            for group in group_info:
                response += f"  {group['grupo']} - {group['cuatrimestre']} cuatrimestre\n"
                response += f"  Ciclo: {group['ciclo_escolar']}\n"
        
        if grades_info and not isinstance(grades_info, dict):
            response += f"\nÚltimas calificaciones:\n"
            for grade in grades_info:
                response += f"  {grade['asignatura']}: {grade['calificacion_final']} ({grade['estatus']})\n"
        
        if risk_info and not isinstance(risk_info, dict):
            response += f"\nReportes de riesgo activos ({len(risk_info)}):\n"
            for risk in risk_info:
                response += f"  {risk['tipo_riesgo']} - Nivel {risk['nivel_riesgo']} ({risk['fecha']})\n"
                response += f"    {risk['descripcion']}\n"
        
        return response
    
    elif intent == 'most_used_classrooms':
        query = """
        SELECT h.aula, COUNT(*) as frecuencia_uso,
               GROUP_CONCAT(DISTINCT h.dia_semana ORDER BY h.dia_semana) as dias,
               GROUP_CONCAT(DISTINCT CONCAT(a.nombre, ' (', g.codigo, ')') 
                           ORDER BY a.nombre SEPARATOR '; ') as asignaturas_grupos
        FROM horarios h
        JOIN profesor_asignatura_grupo pag ON h.profesor_asignatura_grupo_id = pag.id
        JOIN grupos g ON pag.grupo_id = g.id
        JOIN asignaturas a ON pag.asignatura_id = a.id
        WHERE h.activo = TRUE AND h.aula IS NOT NULL AND h.aula != ''
        GROUP BY h.aula
        ORDER BY frecuencia_uso DESC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No encontré información de uso de aulas en este momento."
            
            response = "Te muestro las aulas más utilizadas durante la semana:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. Aula {row['aula']}\n"
                response += f"   Frecuencia de uso: {row['frecuencia_uso']} horarios\n"
                response += f"   Días: {row['dias']}\n"
                if len(row['asignaturas_grupos']) > 150:
                    response += f"   Asignaturas: {row['asignaturas_grupos'][:150]}...\n"
                else:
                    response += f"   Asignaturas: {row['asignaturas_grupos']}\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de uso de aulas en este momento."
    elif intent == 'class_schedules':
        query = """
        SELECT h.dia_semana, h.hora_inicio, h.hora_fin, h.aula,
            a.nombre as asignatura, g.codigo as grupo,
            CONCAT(u.nombre, ' ', u.apellido) as profesor,
            c.nombre as carrera
        FROM horarios h
        JOIN profesor_asignatura_grupo pag ON h.profesor_asignatura_grupo_id = pag.id
        JOIN asignaturas a ON pag.asignatura_id = a.id
        JOIN grupos g ON pag.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        JOIN profesores p ON pag.profesor_id = p.id
        JOIN usuarios u ON p.usuario_id = u.id
        WHERE h.activo = TRUE
        ORDER BY c.nombre, g.codigo, h.dia_semana, h.hora_inicio
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No encontré horarios de clases registrados."
            
            response = "Horarios de clases por carrera y grupo:\n\n"
            current_career = None
            current_group = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                if current_group != row['grupo']:
                    current_group = row['grupo']
                    response += f"  Grupo {row['grupo']}:\n"
                
                response += f"    {row['dia_semana']} {row['hora_inicio']}-{row['hora_fin']}\n"
                response += f"      {row['asignatura']} - Prof. {row['profesor']}\n"
                response += f"      Aula: {row['aula']}\n\n"
            
            return response
        else:
            return "No pude obtener información de horarios en este momento."

    elif intent == 'subjects_by_career':
        query = """
        SELECT c.nombre as carrera, a.nombre as asignatura,
            a.creditos, a.horas_teoricas, a.horas_practicas,
            COUNT(DISTINCT pag.grupo_id) as grupos_asignados
        FROM carreras c
        JOIN carrera_asignatura ca ON c.id = ca.carrera_id
        JOIN asignaturas a ON ca.asignatura_id = a.id
        LEFT JOIN profesor_asignatura_grupo pag ON a.id = pag.asignatura_id AND pag.activo = TRUE
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre, a.id, a.nombre, a.creditos, a.horas_teoricas, a.horas_practicas
        ORDER BY c.nombre, a.nombre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Asignaturas disponibles por carrera:\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  {row['asignatura']}\n"
                response += f"    Créditos: {row['creditos']}\n"
                response += f"    Horas teóricas: {row['horas_teoricas']} | Prácticas: {row['horas_practicas']}\n"
                response += f"    Grupos activos: {row['grupos_asignados']}\n\n"
            
            return response
        else:
            return "No pude obtener información de asignaturas en este momento."

    elif intent == 'available_teachers':
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido, c.nombre as carrera,
            p.especialidad, p.experiencia_años,
            COUNT(pag.id) as asignaciones_actuales
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN carreras c ON p.carrera_id = c.id
        LEFT JOIN profesor_asignatura_grupo pag ON p.id = pag.profesor_id AND pag.activo = TRUE
        WHERE p.activo = TRUE
        GROUP BY p.id, p.numero_empleado, u.nombre, u.apellido, c.nombre, p.especialidad, p.experiencia_años
        HAVING asignaciones_actuales < 3
        ORDER BY asignaciones_actuales ASC, p.experiencia_años DESC
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "Todos los profesores activos tienen asignaciones completas."
            
            response = "Profesores disponibles para nuevas asignaciones:\n\n"
            
            for row in result:
                response += f"Prof. {row['nombre']} {row['apellido']} ({row['numero_empleado']})\n"
                response += f"  Carrera: {row['carrera']}\n"
                if row['especialidad']:
                    response += f"  Especialidad: {row['especialidad']}\n"
                response += f"  Experiencia: {row['experiencia_años']} años\n"
                response += f"  Asignaciones actuales: {row['asignaciones_actuales']}\n\n"
            
            return response
        else:
            return "No pude obtener información de profesores disponibles."

    elif intent == 'most_failed_subjects':
        query = """
        SELECT a.nombre as asignatura, 
            COUNT(CASE WHEN c.calificacion_final < 7 THEN 1 END) as reprobados,
            COUNT(c.id) as total_evaluados,
            ROUND((COUNT(CASE WHEN c.calificacion_final < 7 THEN 1 END) * 100.0 / COUNT(c.id)), 2) as porcentaje_reprobacion,
            car.nombre as carrera
        FROM asignaturas a
        JOIN calificaciones c ON a.id = c.asignatura_id
        JOIN grupos g ON c.grupo_id = g.id
        JOIN carreras car ON g.carrera_id = car.id
        WHERE c.calificacion_final IS NOT NULL
        GROUP BY a.id, a.nombre, car.nombre
        HAVING reprobados > 0
        ORDER BY porcentaje_reprobacion DESC, reprobados DESC
        LIMIT 15
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "Excelente: no hay materias con índices significativos de reprobación."
            
            response = "Materias con mayores índices de reprobación:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. {row['asignatura']} ({row['carrera']})\n"
                response += f"   Reprobados: {row['reprobados']} de {row['total_evaluados']} estudiantes\n"
                response += f"   Porcentaje de reprobación: {row['porcentaje_reprobacion']}%\n"
                if row['porcentaje_reprobacion'] > 50:
                    response += f"   MATERIA DE ALTO RIESGO\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener estadísticas de reprobación."

    elif intent == 'student_demographics':
        query = """
        SELECT 
            CASE 
                WHEN TIMESTAMPDIFF(YEAR, a.fecha_nacimiento, CURDATE()) BETWEEN 17 AND 20 THEN '17-20 años'
                WHEN TIMESTAMPDIFF(YEAR, a.fecha_nacimiento, CURDATE()) BETWEEN 21 AND 25 THEN '21-25 años'
                WHEN TIMESTAMPDIFF(YEAR, a.fecha_nacimiento, CURDATE()) BETWEEN 26 AND 30 THEN '26-30 años'
                WHEN TIMESTAMPDIFF(YEAR, a.fecha_nacimiento, CURDATE()) > 30 THEN 'Más de 30 años'
                ELSE 'Sin datos'
            END as rango_edad,
            COUNT(*) as total_estudiantes,
            ROUND(AVG(a.promedio_general), 2) as promedio_grupo_edad
        FROM alumnos a
        WHERE a.estado_alumno = 'activo'
        GROUP BY rango_edad
        ORDER BY total_estudiantes DESC
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Distribución demográfica de estudiantes activos:\n\n"
            total_estudiantes = sum(row['total_estudiantes'] for row in result)
            
            for row in result:
                porcentaje = (row['total_estudiantes'] / total_estudiantes * 100)
                response += f"{row['rango_edad']}: {row['total_estudiantes']} estudiantes ({porcentaje:.1f}%)\n"
                response += f"  Promedio académico del grupo: {row['promedio_grupo_edad']}\n\n"
            
            response += f"Total de estudiantes analizados: {total_estudiantes}"
            return response
        else:
            return "No pude obtener información demográfica."

    elif intent == 'classroom_capacity':
        query = """
        SELECT h.aula, 
            COUNT(DISTINCT CONCAT(h.dia_semana, h.hora_inicio)) as horarios_ocupados,
            ROUND((COUNT(DISTINCT CONCAT(h.dia_semana, h.hora_inicio)) / 45.0) * 100, 2) as porcentaje_ocupacion
        FROM horarios h
        WHERE h.activo = TRUE AND h.aula IS NOT NULL
        GROUP BY h.aula
        ORDER BY porcentaje_ocupacion DESC
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No encontré datos de ocupación de aulas."
            
            response = "Capacidad y ocupación de aulas:\n\n"
            
            for row in result:
                status = ""
                if row['porcentaje_ocupacion'] > 80:
                    status = " - ALTA OCUPACIÓN"
                elif row['porcentaje_ocupacion'] < 30:
                    status = " - DISPONIBLE"
                
                response += f"Aula {row['aula']}: {row['porcentaje_ocupacion']}% ocupada{status}\n"
                response += f"  Horarios ocupados: {row['horarios_ocupados']} de 45 posibles\n\n"
            
            return response
        else:
            return "No pude obtener datos de capacidad de aulas."

    elif intent == 'historical_top_students':
        query = """
        SELECT a.matricula, u.nombre, u.apellido, c.nombre as carrera,
            a.promedio_general, a.cuatrimestre_actual,
            COUNT(CASE WHEN cal.calificacion_final >= 9 THEN 1 END) as materias_excelencia
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id
        WHERE a.estado_alumno = 'activo' AND a.promedio_general >= 9.0
        GROUP BY a.id, a.matricula, u.nombre, u.apellido, c.nombre, a.promedio_general, a.cuatrimestre_actual
        ORDER BY a.promedio_general DESC, materias_excelencia DESC
        LIMIT 20
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No hay estudiantes con promedio superior a 9.0 actualmente."
            
            response = "Estudiantes destacados históricamente (Promedio >= 9.0):\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. {row['nombre']} {row['apellido']} ({row['matricula']})\n"
                response += f"   Carrera: {row['carrera']}\n"
                response += f"   Promedio general: {row['promedio_general']}\n"
                response += f"   Cuatrimestre: {row['cuatrimestre_actual']}\n"
                response += f"   Materias con excelencia: {row['materias_excelencia']}\n\n"
            
            return response
        else:
            return "No pude obtener información de estudiantes destacados."
    elif intent == 'all_groups':
        query = """
        SELECT g.codigo, g.cuatrimestre, g.ciclo_escolar, c.nombre as carrera,
            COUNT(ag.alumno_id) as total_alumnos,
            CONCAT(u.nombre, ' ', u.apellido) as tutor
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        LEFT JOIN profesores p ON g.profesor_tutor_id = p.id
        LEFT JOIN usuarios u ON p.usuario_id = u.id
        WHERE g.activo = TRUE
        GROUP BY g.id, g.codigo, g.cuatrimestre, g.ciclo_escolar, c.nombre, u.nombre, u.apellido
        ORDER BY c.nombre, g.cuatrimestre, g.codigo
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron grupos activos en el sistema."
            
            response = f"Todos los grupos activos ({len(result)} grupos):\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  Grupo: {row['codigo']} - {row['cuatrimestre']} cuatrimestre\n"
                response += f"    Ciclo: {row['ciclo_escolar']}\n"
                response += f"    Alumnos: {row['total_alumnos']}\n"
                if row['tutor']:
                    response += f"    Tutor: {row['tutor']}\n"
                else:
                    response += f"    Sin tutor asignado\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de los grupos."

    elif intent == 'groups_by_career_semester':
        query = """
        SELECT c.nombre as carrera, g.cuatrimestre,
            COUNT(g.id) as total_grupos,
            SUM(CASE WHEN g.activo = TRUE THEN 1 ELSE 0 END) as grupos_activos,
            AVG(subq.alumnos_por_grupo) as promedio_alumnos
        FROM carreras c
        LEFT JOIN grupos g ON c.id = g.carrera_id
        LEFT JOIN (
            SELECT g.id, COUNT(ag.alumno_id) as alumnos_por_grupo
            FROM grupos g
            LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
            GROUP BY g.id
        ) subq ON g.id = subq.id
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre, g.cuatrimestre
        HAVING total_grupos > 0
        ORDER BY c.nombre, g.cuatrimestre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Grupos activos por carrera y cuatrimestre:\n\n"
            current_career = None
            total_general = 0
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  {row['cuatrimestre']} cuatrimestre: {row['grupos_activos']} grupos activos\n"
                if row['promedio_alumnos']:
                    response += f"    Promedio de alumnos por grupo: {row['promedio_alumnos']:.1f}\n"
                total_general += row['grupos_activos']
            
            response += f"\nTotal general: {total_general} grupos activos"
            return response
        else:
            return "No pude obtener información de grupos por carrera."

    elif intent == 'groups_without_tutor':
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
            COUNT(ag.alumno_id) as total_alumnos,
            g.ciclo_escolar
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE g.activo = TRUE AND g.profesor_tutor_id IS NULL
        GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre, g.ciclo_escolar
        ORDER BY c.nombre, g.cuatrimestre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "Todos los grupos activos tienen tutor asignado."
            
            response = f"Grupos sin tutor asignado ({len(result)} grupos):\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  Grupo: {row['codigo']} - {row['cuatrimestre']} cuatrimestre\n"
                response += f"    Ciclo: {row['ciclo_escolar']}\n"
                response += f"    Alumnos: {row['total_alumnos']}\n"
                response += f"    REQUIERE ASIGNACIÓN DE TUTOR\n\n"
            
            return response
        else:
            return "No hay suficiente información de grupos o tutores en la base de datos para responder esta consulta."

    elif intent == 'group_tutor':
        if 'grupo' not in entities:
            return "Para consultar el tutor de un grupo necesito que menciones el código del grupo."
        
        grupo_codigo = entities['grupo']
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
            p.numero_empleado, u.nombre, u.apellido,
            COUNT(ag.alumno_id) as total_alumnos
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN profesores p ON g.profesor_tutor_id = p.id
        LEFT JOIN usuarios u ON p.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE g.codigo = %s AND g.activo = TRUE
        GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre, p.numero_empleado, u.nombre, u.apellido
        """
        result = execute_query(query, (grupo_codigo,))
        if result and not isinstance(result, dict):
            if not result:
                return f"No encontré el grupo {grupo_codigo}."
            
            data = result[0]
            response = f"Información del tutor del grupo {data['codigo']}:\n\n"
            response += f"Grupo: {data['codigo']} - {data['cuatrimestre']} cuatrimestre\n"
            response += f"Carrera: {data['carrera']}\n"
            response += f"Total alumnos: {data['total_alumnos']}\n\n"
            
            if data['numero_empleado']:
                response += f"Tutor asignado:\n"
                response += f"  Nombre: {data['nombre']} {data['apellido']}\n"
                response += f"  Número empleado: {data['numero_empleado']}\n"
            else:
                response += "Este grupo NO TIENE TUTOR ASIGNADO"
            
            return response
        else:
            return f"No pude obtener información del grupo {grupo_codigo}."

    elif intent == 'tutor_groups':
        if 'numero_empleado' in entities:
            numero_emp = entities['numero_empleado']
            query = """
            SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
                COUNT(ag.alumno_id) as total_alumnos,
                u.nombre, u.apellido
            FROM profesores p
            JOIN usuarios u ON p.usuario_id = u.id
            JOIN grupos g ON p.id = g.profesor_tutor_id
            JOIN carreras c ON g.carrera_id = c.id
            LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
            WHERE p.numero_empleado = %s AND g.activo = TRUE
            GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre, u.nombre, u.apellido
            ORDER BY c.nombre, g.cuatrimestre
            """
            result = execute_query(query, (numero_emp,))
            if result and not isinstance(result, dict):
                if not result:
                    return f"El profesor {numero_emp} no tiene grupos asignados como tutor."
                
                data = result[0]
                response = f"Grupos del tutor {data['nombre']} {data['apellido']} ({numero_emp}):\n\n"
                for row in result:
                    response += f"  Grupo: {row['codigo']} - {row['cuatrimestre']} cuatrimestre\n"
                    response += f"    Carrera: {row['carrera']}\n"
                    response += f"    Alumnos: {row['total_alumnos']}\n\n"
                
                return response
        elif 'nombre_tutor' in entities:
            nombre = entities['nombre_tutor']
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
            GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre, u.nombre, u.apellido, p.numero_empleado
            ORDER BY c.nombre, g.cuatrimestre
            """
            result = execute_query(query, (f"%{nombre}%",))
            if result and not isinstance(result, dict):
                if not result:
                    return f"No encontré profesor tutor con el nombre {nombre}."
                
                data = result[0]
                response = f"Grupos del tutor {data['nombre']} {data['apellido']} ({data['numero_empleado']}):\n\n"
                for row in result:
                    response += f"  Grupo: {row['codigo']} - {row['cuatrimestre']} cuatrimestre\n"
                    response += f"    Carrera: {row['carrera']}\n"
                    response += f"    Alumnos: {row['total_alumnos']}\n\n"
                
                return response
        
        return "Para consultar los grupos de un tutor necesito su número de empleado o nombre."

    elif intent == 'groups_most_risk':
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
            COUNT(DISTINCT a.id) as total_alumnos,
            COUNT(rr.id) as total_reportes,
            ROUND(COUNT(rr.id) / COUNT(DISTINCT a.id), 2) as reportes_por_alumno
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        JOIN alumnos a ON ag.alumno_id = a.id
        JOIN reportes_riesgo rr ON a.id = rr.alumno_id AND rr.estado IN ('abierto', 'en_proceso')
        WHERE g.activo = TRUE
        GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre
        ORDER BY total_reportes DESC, reportes_por_alumno DESC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No hay grupos con reportes de riesgo activos."
            
            response = "Grupos con mayor número de reportes de riesgo por alumno:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. Grupo {row['codigo']} - {row['cuatrimestre']} cuatrimestre\n"
                response += f"   Carrera: {row['carrera']}\n"
                response += f"   Total alumnos: {row['total_alumnos']}\n"
                response += f"   Total reportes: {row['total_reportes']}\n"
                response += f"   Reportes por alumno: {row['reportes_por_alumno']}\n\n"
            
            return response
        else:
            return "No pude obtener información de reportes de riesgo por grupo."

    elif intent == 'most_viewed_news':
        query = """
        SELECT n.titulo, n.contenido, n.fecha_publicacion,
            n.vistas, n.categoria,
            CONCAT(u.nombre, ' ', u.apellido) as autor
        FROM noticias n
        LEFT JOIN usuarios u ON n.autor_id = u.id
        WHERE n.activa = TRUE
        ORDER BY n.vistas DESC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron noticias en el sistema."
            
            response = "Noticias más vistas por los usuarios:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. {row['titulo']}\n"
                response += f"   Autor: {row['autor'] or 'No especificado'}\n"
                response += f"   Categoría: {row['categoria']}\n"
                response += f"   Fecha: {row['fecha_publicacion']}\n"
                response += f"   Vistas: {row['vistas']}\n"
                if len(row['contenido']) > 100:
                    response += f"   Resumen: {row['contenido'][:100]}...\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de noticias."

    elif intent == 'groups_lowest_average':
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
            COUNT(DISTINCT a.id) as total_alumnos,
            AVG(cal.calificacion_final) as promedio_grupo
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        LEFT JOIN alumnos a ON ag.alumno_id = a.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id AND cal.calificacion_final IS NOT NULL
        WHERE g.activo = TRUE
        GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre
        HAVING COUNT(cal.id) > 0
        ORDER BY promedio_grupo ASC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron grupos con calificaciones registradas."
            
            response = "Grupos con promedio final más bajo:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. Grupo {row['codigo']} - {row['cuatrimestre']} cuatrimestre\n"
                response += f"   Carrera: {row['carrera']}\n"
                response += f"   Total alumnos: {row['total_alumnos']}\n"
                response += f"   Promedio del grupo: {row['promedio_grupo']:.2f}\n\n"
            
            return response
        else:
            return "No pude obtener información de promedios por grupo."

    elif intent == 'subjects_no_grades':
        query = """
        SELECT a.nombre as asignatura, g.codigo as grupo, g.cuatrimestre,
            car.nombre as carrera,
            CONCAT(u.nombre, ' ', u.apellido) as profesor,
            p.numero_empleado
        FROM profesor_asignatura_grupo pag
        JOIN asignaturas a ON pag.asignatura_id = a.id
        JOIN grupos g ON pag.grupo_id = g.id
        JOIN carreras car ON g.carrera_id = car.id
        JOIN profesores p ON pag.profesor_id = p.id
        JOIN usuarios u ON p.usuario_id = u.id
        LEFT JOIN calificaciones cal ON pag.grupo_id = cal.grupo_id 
                                    AND pag.asignatura_id = cal.asignatura_id
        WHERE pag.activo = TRUE AND g.activo = TRUE
        AND cal.id IS NULL
        ORDER BY car.nombre, g.cuatrimestre, a.nombre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "Todas las asignaturas tienen calificaciones registradas."
            
            response = "Asignaturas con calificaciones aún sin registrar:\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  Asignatura: {row['asignatura']}\n"
                response += f"    Grupo: {row['grupo']} ({row['cuatrimestre']} cuatrimestre)\n"
                response += f"    Profesor: {row['profesor']} ({row['numero_empleado']})\n\n"
            
            return response
        else:
            return "No pude obtener información de asignaturas sin calificaciones."

    elif intent == 'forum_categories':
        query = """
        SELECT nombre, descripcion, activa,
            (SELECT COUNT(*) FROM publicaciones_foro pf WHERE pf.categoria_id = cf.id) as total_publicaciones
        FROM categorias_foro cf
        ORDER BY total_publicaciones DESC, nombre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron categorías del foro."
            
            response = "Categorías disponibles en el foro:\n\n"
            
            for i, row in enumerate(result, 1):
                status = "Activa" if row['activa'] else "Inactiva"
                response += f"{i}. {row['nombre']} ({status})\n"
                response += f"   Descripción: {row['descripcion']}\n"
                response += f"   Total publicaciones: {row['total_publicaciones']}\n\n"
            
            return response
        else:
            return "No pude obtener información de categorías del foro."

    elif intent == 'posts_by_category':
        query = """
        SELECT cf.nombre as categoria, COUNT(pf.id) as total_publicaciones,
            COUNT(CASE WHEN pf.activa = TRUE THEN 1 END) as publicaciones_activas
        FROM categorias_foro cf
        LEFT JOIN publicaciones_foro pf ON cf.id = pf.categoria_id
        WHERE cf.activa = TRUE
        GROUP BY cf.id, cf.nombre
        ORDER BY total_publicaciones DESC
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Publicaciones por categoría de foro:\n\n"
            total_general = 0
            
            for row in result:
                response += f"Categoría: {row['categoria']}\n"
                response += f"  Total publicaciones: {row['total_publicaciones']}\n"
                response += f"  Publicaciones activas: {row['publicaciones_activas']}\n\n"
                total_general += row['total_publicaciones']
            
            response += f"Total general: {total_general} publicaciones"
            return response
        else:
            return "No pude obtener información de publicaciones por categoría."

    elif intent == 'all_groups_student_count':
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
            COUNT(ag.alumno_id) as total_alumnos
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE g.activo = TRUE
        GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre
        ORDER BY c.nombre, g.cuatrimestre, g.codigo
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron grupos activos."
            
            response = f"Todos los grupos con su número de alumnos ({len(result)} grupos):\n\n"
            current_career = None
            total_estudiantes = 0
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  {row['codigo']} - {row['cuatrimestre']} cuatrimestre: {row['total_alumnos']} alumnos\n"
                total_estudiantes += row['total_alumnos']
            
            response += f"\nTotal de estudiantes en grupos: {total_estudiantes}"
            return response
        else:
            return "No pude obtener información de grupos con estudiantes."

    elif intent == 'groups_least_students':
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
            COUNT(ag.alumno_id) as total_alumnos
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE g.activo = TRUE
        GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre
        ORDER BY total_alumnos ASC, c.nombre
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron grupos activos."
            
            response = "Grupos con menos alumnos:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. Grupo {row['codigo']} - {row['cuatrimestre']} cuatrimestre\n"
                response += f"   Carrera: {row['carrera']}\n"
                response += f"   Total alumnos: {row['total_alumnos']}\n\n"
            
            return response
        else:
            return "No pude obtener información de grupos con menor ocupación."

    elif intent == 'group_average':
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
            COUNT(ag.alumno_id) as total_alumnos,
            AVG(cal.calificacion_final) as promedio_grupo
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        LEFT JOIN calificaciones cal ON g.id = cal.grupo_id AND cal.calificacion_final IS NOT NULL
        WHERE g.activo = TRUE
        GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre
        HAVING COUNT(cal.id) > 0
        ORDER BY promedio_grupo DESC, c.nombre, g.cuatrimestre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron grupos con calificaciones registradas."
            
            response = "Calificación promedio por grupo:\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  {row['codigo']} ({row['cuatrimestre']} cuatrimestre): {row['promedio_grupo']:.2f}\n"
                response += f"    Alumnos: {row['total_alumnos']}\n\n"
            
            return response
        else:
            return "No pude obtener información de promedios por grupo."

    elif intent == 'students_passed_ordinary':
        query = """
        SELECT c.nombre as carrera, g.cuatrimestre,
            COUNT(CASE WHEN cal.estatus = 'aprobado' AND cal.calificacion_ordinario IS NOT NULL THEN 1 END) as aprobados_ordinario,
            COUNT(DISTINCT a.id) as total_alumnos
        FROM calificaciones cal
        JOIN alumnos a ON cal.alumno_id = a.id
        JOIN grupos g ON cal.grupo_id = g.id
        JOIN carreras c ON g.carrera_id = c.id
        WHERE cal.ciclo_escolar LIKE '%SEP-DIC 2024%'
        GROUP BY c.id, c.nombre, g.cuatrimestre
        ORDER BY c.nombre, g.cuatrimestre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron evaluaciones para SEP-DIC 2024."
            
            response = "Alumnos que aprobaron en ordinario (SEP-DIC 2024):\n\n"
            current_career = None
            total_aprobados = 0
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                porcentaje = (row['aprobados_ordinario'] / row['total_alumnos']) * 100 if row['total_alumnos'] > 0 else 0
                response += f"  {row['cuatrimestre']} cuatrimestre: {row['aprobados_ordinario']} aprobados de {row['total_alumnos']} alumnos ({porcentaje:.1f}%)\n"
                total_aprobados += row['aprobados_ordinario']
            
            response += f"\nTotal general: {total_aprobados} estudiantes aprobaron en ordinario"
            return response
        else:
            return "No pude obtener información de aprobación en ordinario."

    elif intent == 'inactive_teachers':
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido, u.correo,
            c.nombre as carrera, p.fecha_contratacion,
            p.titulo_academico, p.especialidad
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN carreras c ON p.carrera_id = c.id
        WHERE p.activo = FALSE
        ORDER BY c.nombre, p.fecha_contratacion DESC
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "Todos los profesores registrados están activos."
            
            response = f"Profesores inactivos actualmente ({len(result)} casos):\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  {row['nombre']} {row['apellido']} ({row['numero_empleado']})\n"
                response += f"    Correo: {row['correo']}\n"
                response += f"    Contratación: {row['fecha_contratacion']}\n"
                if row['titulo_academico']:
                    response += f"    Título: {row['titulo_academico']}\n"
                if row['especialidad']:
                    response += f"    Especialidad: {row['especialidad']}\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de profesores inactivos."

    elif intent == 'teachers_count_by_career':
        query = """
        SELECT c.nombre as carrera,
            COUNT(p.id) as total_profesores,
            SUM(CASE WHEN p.activo = TRUE THEN 1 ELSE 0 END) as profesores_activos
        FROM carreras c
        LEFT JOIN profesores p ON c.id = p.carrera_id
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre
        ORDER BY profesores_activos DESC, c.nombre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Profesores por carrera:\n\n"
            total_general = 0
            total_activos = 0
            
            for row in result:
                response += f"{row['carrera']}:\n"
                response += f"  Total: {row['total_profesores']} profesores\n"
                response += f"  Activos: {row['profesores_activos']}\n\n"
                total_general += row['total_profesores']
                total_activos += row['profesores_activos']
            
            response += f"Resumen: {total_activos} profesores activos de {total_general} registrados"
            return response
        else:
            return "No pude obtener información de profesores por carrera."

    elif intent == 'teacher_most_subjects':
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido,
            COUNT(DISTINCT pag.asignatura_id) as total_asignaturas,
            COUNT(DISTINCT pag.grupo_id) as total_grupos,
            GROUP_CONCAT(DISTINCT a.nombre ORDER BY a.nombre SEPARATOR '; ') as asignaturas,
            c.nombre as carrera
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN carreras c ON p.carrera_id = c.id
        JOIN profesor_asignatura_grupo pag ON p.id = pag.profesor_id
        JOIN asignaturas a ON pag.asignatura_id = a.id
        WHERE p.activo = TRUE AND pag.activo = TRUE
        GROUP BY p.id, p.numero_empleado, u.nombre, u.apellido, c.nombre
        ORDER BY total_asignaturas DESC, total_grupos DESC
        LIMIT 5
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron asignaciones de profesores."
       
            response = "Profesores con más asignaturas asignadas:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. {row['nombre']} {row['apellido']} ({row['numero_empleado']})\n"
                response += f"   Carrera: {row['carrera']}\n"
                response += f"   Asignaturas: {row['total_asignaturas']} | Grupos: {row['total_grupos']}\n"
                if len(row['asignaturas']) > 100:
                    response += f"   Materias: {row['asignaturas'][:100]}...\n"
                else:
                    response += f"   Materias: {row['asignaturas']}\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de asignaturas por profesor."

    elif intent == 'career_most_teachers':
        query = """
        SELECT c.nombre as carrera,
                COUNT(p.id) as total_profesores,
                SUM(CASE WHEN p.activo = TRUE THEN 1 ELSE 0 END) as profesores_activos,
                COUNT(DISTINCT pag.asignatura_id) as asignaturas_cubiertas,
                COUNT(DISTINCT g.id) as grupos_atendidos
        FROM carreras c
        LEFT JOIN profesores p ON c.id = p.carrera_id
        LEFT JOIN profesor_asignatura_grupo pag ON p.id = pag.profesor_id AND pag.activo = TRUE
        LEFT JOIN grupos g ON pag.grupo_id = g.id AND g.activo = TRUE
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre
        ORDER BY profesores_activos DESC, total_profesores DESC
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron carreras activas."
            
            response = "Carreras con más profesores asignados:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. {row['carrera']}\n"
                response += f"   Profesores activos: {row['profesores_activos']}\n"
                response += f"   Total profesores: {row['total_profesores']}\n"
                response += f"   Asignaturas cubiertas: {row['asignaturas_cubiertas']}\n"
                response += f"   Grupos atendidos: {row['grupos_atendidos']}\n\n"
            
            return response
        else:
            return "No pude obtener información de profesores por carrera."

    elif intent == 'students_by_semester_career':
        query = """
        SELECT c.nombre as carrera, a.cuatrimestre_actual,
                COUNT(a.id) as total_alumnos,
                SUM(CASE WHEN a.estado_alumno = 'activo' THEN 1 ELSE 0 END) as activos,
                AVG(a.promedio_general) as promedio_cuatrimestre
        FROM carreras c
        LEFT JOIN alumnos a ON c.id = a.carrera_id
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre, a.cuatrimestre_actual
        HAVING total_alumnos > 0
        ORDER BY c.nombre, a.cuatrimestre_actual
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Alumnos por cada cuatrimestre en cada carrera:\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  {row['cuatrimestre_actual']} cuatrimestre: {row['total_alumnos']} alumnos ({row['activos']} activos)\n"
                if row['promedio_cuatrimestre']:
                    response += f"    Promedio del cuatrimestre: {row['promedio_cuatrimestre']:.2f}\n"
            
            return response
        else:
            return "No pude obtener información de alumnos por cuatrimestre."

    elif intent == 'groups_created_by_career':
        query = """
        SELECT c.nombre as carrera,
                COUNT(g.id) as total_grupos,
                SUM(CASE WHEN g.activo = TRUE THEN 1 ELSE 0 END) as grupos_activos,
                COUNT(DISTINCT g.cuatrimestre) as cuatrimestres_con_grupos
        FROM carreras c
        LEFT JOIN grupos g ON c.id = g.carrera_id
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre
        ORDER BY total_grupos DESC, c.nombre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Grupos creados por carrera:\n\n"
            total_grupos_sistema = 0
            total_activos_sistema = 0
            
            for row in result:
                response += f"{row['carrera']}:\n"
                response += f"  Total grupos creados: {row['total_grupos']}\n"
                response += f"  Grupos activos: {row['grupos_activos']}\n"
                response += f"  Cuatrimestres con grupos: {row['cuatrimestres_con_grupos']}\n\n"
                total_grupos_sistema += row['total_grupos']
                total_activos_sistema += row['grupos_activos']
            
            response += f"Total del sistema: {total_activos_sistema} grupos activos de {total_grupos_sistema} creados"
            return response
        else:
            return "No pude obtener información de grupos por carrera."

    elif intent == 'all_groups_by_career':
        query = """
        SELECT c.nombre as carrera, g.codigo, g.cuatrimestre,
                g.ciclo_escolar, g.periodo, g.año,
                COUNT(ag.alumno_id) as total_alumnos,
                CONCAT(u.nombre, ' ', u.apellido) as tutor
        FROM carreras c
        JOIN grupos g ON c.id = g.carrera_id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        LEFT JOIN profesores p ON g.profesor_tutor_id = p.id
        LEFT JOIN usuarios u ON p.usuario_id = u.id
        WHERE c.activa = TRUE AND g.activo = TRUE
        GROUP BY c.id, c.nombre, g.id, g.codigo, g.cuatrimestre, g.ciclo_escolar, g.periodo, g.año, u.nombre, u.apellido
        ORDER BY c.nombre, g.cuatrimestre, g.codigo
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron grupos activos."
            
            response = f"Todos los grupos ordenados por carreras ({len(result)} grupos):\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  {row['codigo']} - {row['cuatrimestre']} cuatrimestre\n"
                response += f"    Período: {row['periodo']} {row['año']}\n"
                response += f"    Alumnos: {row['total_alumnos']}\n"
                if row['tutor']:
                    response += f"    Tutor: {row['tutor']}\n"
                else:
                    response += f"    Sin tutor asignado\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de grupos por carrera."

    elif intent == 'total_students_system':
        query = """
        SELECT COUNT(*) as total_estudiantes,
                SUM(CASE WHEN estado_alumno = 'activo' THEN 1 ELSE 0 END) as activos,
                SUM(CASE WHEN estado_alumno = 'egresado' THEN 1 ELSE 0 END) as egresados,
                SUM(CASE WHEN estado_alumno = 'baja_temporal' THEN 1 ELSE 0 END) as baja_temporal,
                SUM(CASE WHEN estado_alumno = 'baja_definitiva' THEN 1 ELSE 0 END) as baja_definitiva,
                AVG(promedio_general) as promedio_general_sistema,
                COUNT(DISTINCT carrera_id) as carreras_con_estudiantes
        FROM alumnos
        """
        result = execute_query(query)
        if result and not isinstance(result, dict) and result:
            data = result[0]
            response = "ESTADÍSTICAS GENERALES DEL SISTEMA ESTUDIANTIL\n\n"
            response += f"Total de estudiantes registrados: {data['total_estudiantes']}\n\n"
            response += f"Distribución por estado:\n"
            response += f"  Activos: {data['activos']} ({(data['activos']/data['total_estudiantes']*100):.1f}%)\n"
            response += f"  Egresados: {data['egresados']} ({(data['egresados']/data['total_estudiantes']*100):.1f}%)\n"
            response += f"  Baja temporal: {data['baja_temporal']} ({(data['baja_temporal']/data['total_estudiantes']*100):.1f}%)\n"
            response += f"  Baja definitiva: {data['baja_definitiva']} ({(data['baja_definitiva']/data['total_estudiantes']*100):.1f}%)\n\n"
            response += f"Promedio general del sistema: {data['promedio_general_sistema']:.2f}\n"
            response += f"Carreras con estudiantes: {data['carreras_con_estudiantes']}\n"
            
            return response
        else:
            return "No pude obtener las estadísticas generales del sistema."

    elif intent == 'total_active_teachers':
        query = """
        SELECT COUNT(*) as total_profesores,
                SUM(CASE WHEN activo = TRUE THEN 1 ELSE 0 END) as profesores_activos,
                SUM(CASE WHEN es_tutor = TRUE AND activo = TRUE THEN 1 ELSE 0 END) as tutores_activos,
                AVG(experiencia_años) as experiencia_promedio,
                COUNT(DISTINCT carrera_id) as carreras_con_profesores
        FROM profesores
        """
        result = execute_query(query)
        if result and not isinstance(result, dict) and result:
            data = result[0]
            response = "ESTADÍSTICAS GENERALES DEL CUERPO DOCENTE\n\n"
            response += f"Total de profesores registrados: {data['total_profesores']}\n"
            response += f"Profesores activos: {data['profesores_activos']} ({(data['profesores_activos']/data['total_profesores']*100):.1f}%)\n"
            response += f"Tutores activos: {data['tutores_activos']}\n"
            response += f"Experiencia promedio: {data['experiencia_promedio']:.1f} años\n"
            response += f"Carreras con profesores: {data['carreras_con_profesores']}\n"
            
            return response
        else:
            return "No pude obtener las estadísticas del cuerpo docente."

    elif intent == 'general_system_stats':
        query_students = "SELECT COUNT(*) as total, SUM(CASE WHEN estado_alumno = 'activo' THEN 1 ELSE 0 END) as activos FROM alumnos"
        query_teachers = "SELECT COUNT(*) as total, SUM(CASE WHEN activo = TRUE THEN 1 ELSE 0 END) as activos FROM profesores"
        query_groups = "SELECT COUNT(*) as total, SUM(CASE WHEN activo = TRUE THEN 1 ELSE 0 END) as activos FROM grupos"
        query_careers = "SELECT COUNT(*) as total, SUM(CASE WHEN activa = TRUE THEN 1 ELSE 0 END) as activas FROM carreras"
        query_risk = "SELECT COUNT(*) as total FROM reportes_riesgo WHERE estado IN ('abierto', 'en_proceso')"
        query_help = "SELECT COUNT(*) as total FROM solicitudes_ayuda WHERE estado = 'pendiente'"
        
        students = execute_query(query_students)
        teachers = execute_query(query_teachers)
        groups = execute_query(query_groups)
        careers = execute_query(query_careers)
        risk = execute_query(query_risk)
        help_req = execute_query(query_help)
        
        if all(not isinstance(x, dict) and x for x in [students, teachers, groups, careers, risk, help_req]):
            response = "ESTADÍSTICAS GENERALES DEL SISTEMA EDUCATIVO\n\n"
            response += f"ESTUDIANTES:\n"
            response += f"  Total: {students[0]['total']} | Activos: {students[0]['activos']}\n\n"
            response += f"PROFESORES:\n"
            response += f"  Total: {teachers[0]['total']} | Activos: {teachers[0]['activos']}\n\n"
            response += f"GRUPOS:\n"
            response += f"  Total: {groups[0]['total']} | Activos: {groups[0]['activos']}\n\n"
            response += f"CARRERAS:\n"
            response += f"  Total: {careers[0]['total']} | Activas: {careers[0]['activas']}\n\n"
            response += f"SITUACIONES QUE REQUIEREN ATENCIÓN:\n"
            response += f"  Reportes de riesgo activos: {risk[0]['total']}\n"
            response += f"  Solicitudes de ayuda pendientes: {help_req[0]['total']}\n\n"
            
            ratio = students[0]['activos'] / teachers[0]['activos'] if teachers[0]['activos'] > 0 else 0
            response += f"INDICADORES CLAVE:\n"
            response += f"  Ratio estudiante-profesor: {ratio:.1f}:1\n"
            response += f"  Ocupación promedio de grupos: {(students[0]['activos'] / groups[0]['activos']):.1f} estudiantes por grupo\n"
            
            return response
        else:
            return "No pude obtener las estadísticas generales del sistema."
    elif intent == 'students_by_career_semester':
        query = """
        SELECT c.nombre as carrera, a.cuatrimestre_actual,
            COUNT(a.id) as total_alumnos,
            SUM(CASE WHEN a.estado_alumno = 'activo' THEN 1 ELSE 0 END) as activos
        FROM carreras c
        LEFT JOIN alumnos a ON c.id = a.carrera_id
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre, a.cuatrimestre_actual
        HAVING total_alumnos > 0
        ORDER BY c.nombre, a.cuatrimestre_actual
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Distribución actual de alumnos por carrera y cuatrimestre:\n\n"
            current_career = None
            total_general = 0
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  Cuatrimestre {row['cuatrimestre_actual']}: {row['total_alumnos']} alumnos ({row['activos']} activos)\n"
                total_general += row['total_alumnos']
            
            response += f"\nTotal general: {total_general} estudiantes en el sistema"
            return response
        else:
            return "No pude obtener la distribución de estudiantes por carrera y cuatrimestre."

    elif intent == 'inactive_students':
        query = """
        SELECT a.matricula, u.nombre, u.apellido, u.correo, c.nombre as carrera,
            a.cuatrimestre_actual, a.estado_alumno, a.fecha_ingreso,
            a.promedio_general, a.tutor_nombre, a.telefono, a.direccion
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN carreras c ON a.carrera_id = c.id
        WHERE a.estado_alumno != 'activo'
        ORDER BY c.nombre, a.estado_alumno, u.apellido
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "Todos los alumnos registrados están activos actualmente."
            
            response = f"Alumnos con estado inactivo - Información completa ({len(result)} casos):\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  Matrícula: {row['matricula']}\n"
                response += f"  Nombre completo: {row['nombre']} {row['apellido']}\n"
                response += f"  Correo electrónico: {row['correo']}\n"
                response += f"  Estado actual: {row['estado_alumno']}\n"
                response += f"  Cuatrimestre: {row['cuatrimestre_actual']}\n"
                response += f"  Promedio general: {row['promedio_general']}\n"
                response += f"  Fecha de ingreso: {row['fecha_ingreso']}\n"
                if row['tutor_nombre']:
                    response += f"  Tutor asignado: {row['tutor_nombre']}\n"
                if row['telefono']:
                    response += f"  Teléfono: {row['telefono']}\n"
                if row['direccion']:
                    response += f"  Dirección: {row['direccion']}\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de estudiantes inactivos."

    elif intent == 'top_students_current':
        query = """
        SELECT a.matricula, u.nombre, u.apellido, g.codigo as grupo,
            g.cuatrimestre, car.nombre as carrera, AVG(c.calificacion_final) as promedio,
            ROW_NUMBER() OVER (PARTITION BY car.id ORDER BY AVG(c.calificacion_final) DESC) as ranking
        FROM calificaciones c
        JOIN alumnos a ON c.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN grupos g ON c.grupo_id = g.id
        JOIN carreras car ON g.carrera_id = car.id
        WHERE c.calificacion_final IS NOT NULL AND c.ciclo_escolar LIKE '%2024%'
        GROUP BY a.id, a.matricula, u.nombre, u.apellido, g.codigo, g.cuatrimestre, car.nombre, car.id
        HAVING ranking <= 3
        ORDER BY car.nombre, ranking
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No encontré calificaciones registradas para el ciclo actual."
            
            response = "Top 3 mejores estudiantes por carrera en el cuatrimestre actual:\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  {row['ranking']}. Matrícula: {row['matricula']}\n"
                response += f"     Nombre: {row['nombre']} {row['apellido']}\n"
                response += f"     Grupo: {row['grupo']}\n"
                response += f"     Cuatrimestre: {row['cuatrimestre']}\n"
                response += f"     Carrera: {row['carrera']}\n"
                response += f"     Promedio: {row['promedio']:.2f}\n\n"
            
            return response
        else:
            return "No pude obtener información de los mejores estudiantes."

    elif intent == 'top_students_semester':
        cuatrimestre = entities.get('cuatrimestre', '3')
        query = """
        SELECT a.matricula, u.nombre, u.apellido, g.codigo as grupo,
            car.nombre as carrera, AVG(c.calificacion_final) as promedio,
            ROW_NUMBER() OVER (PARTITION BY g.id ORDER BY AVG(c.calificacion_final) DESC) as ranking
        FROM calificaciones c
        JOIN alumnos a ON c.alumno_id = a.id
        JOIN usuarios u ON a.usuario_id = u.id
        JOIN grupos g ON c.grupo_id = g.id
        JOIN carreras car ON g.carrera_id = car.id
        WHERE c.calificacion_final IS NOT NULL AND g.cuatrimestre = %s
        GROUP BY a.id, a.matricula, u.nombre, u.apellido, g.codigo, g.id, car.nombre
        HAVING ranking <= 3
        ORDER BY g.codigo, ranking
        """
        result = execute_query(query, (cuatrimestre,))
        if result and not isinstance(result, dict):
            if not result:
                return f"No encontré estudiantes en {cuatrimestre} cuatrimestre."
            
            response = f"Top 3 mejores estudiantes por grupo en {cuatrimestre} cuatrimestre:\n\n"
            current_group = None
            
            for row in result:
                if current_group != row['grupo']:
                    current_group = row['grupo']
                    response += f"Grupo {row['grupo']} ({row['carrera']}):\n"
                
                response += f"  {row['ranking']}. Matrícula: {row['matricula']}\n"
                response += f"     Nombre: {row['nombre']} {row['apellido']}\n"
                response += f"     Grupo: {row['grupo']}\n"
                response += f"     Cuatrimestre: {cuatrimestre}\n"
                response += f"     Carrera: {row['carrera']}\n"
                response += f"     Promedio: {row['promedio']:.2f}\n\n"
            
            return response
        else:
            return f"No pude obtener información de {cuatrimestre} cuatrimestre."

    elif intent == 'high_risk_students':
        query = """
        SELECT a.matricula, u.nombre, u.apellido, g.codigo as grupo,
            COUNT(rr.id) as total_reportes,
            GROUP_CONCAT(DISTINCT rr.tipo_riesgo) as tipos_riesgo
        FROM alumnos a
        JOIN usuarios u ON a.usuario_id = u.id
        LEFT JOIN alumnos_grupos ag ON a.id = ag.alumno_id AND ag.activo = TRUE
        LEFT JOIN grupos g ON ag.grupo_id = g.id
        JOIN reportes_riesgo rr ON a.id = rr.alumno_id
        WHERE rr.estado IN ('abierto', 'en_proceso')
        GROUP BY a.id, a.matricula, u.nombre, u.apellido, g.codigo
        HAVING total_reportes > 2
        ORDER BY total_reportes DESC, u.apellido
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No hay estudiantes con más de 2 reportes de riesgo activos."
            
            response = f"Alumnos con más de 2 reportes de riesgo activos ({len(result)} casos críticos):\n\n"
            
            for row in result:
                response += f"Matrícula: {row['matricula']}\n"
                response += f"Nombre: {row['nombre']} {row['apellido']}\n"
                response += f"Grupo: {row['grupo'] or 'Sin asignar'}\n"
                response += f"Total reportes activos: {row['total_reportes']}\n"
                response += f"Tipos de riesgo: {row['tipos_riesgo']}\n"
                response += "REQUIERE ATENCIÓN INMEDIATA\n\n"
            
            return response
        else:
            return "No pude obtener información de reportes de riesgo."

    elif intent == 'students_without_group':
        query = """
        SELECT a.matricula, u.nombre, u.apellido, a.cuatrimestre_actual,
            c.nombre as carrera, a.estado_alumno
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
                return "Todos los estudiantes activos están asignados a un grupo."
            
            response = f"Alumnos activos que no están asignados a ningún grupo ({len(result)} casos):\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  Matrícula: {row['matricula']}\n"
                response += f"  Nombre: {row['nombre']} {row['apellido']}\n"
                response += f"  Cuatrimestre: {row['cuatrimestre_actual']}\n"
                response += f"  Estado: {row['estado_alumno']}\n"
                response += f"  NECESITA ASIGNACIÓN DE GRUPO\n\n"
            
            return response
        else:
            return "No pude obtener información de estudiantes sin grupo."

    elif intent == 'teachers_by_career':
        query = """
        SELECT c.nombre as carrera, COUNT(p.id) as total_profesores,
            GROUP_CONCAT(CONCAT(u.nombre, ' ', u.apellido, ' (', p.numero_empleado, ')') SEPARATOR ', ') as profesores
        FROM carreras c
        LEFT JOIN profesores p ON c.id = p.carrera_id AND p.activo = TRUE
        LEFT JOIN usuarios u ON p.usuario_id = u.id
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre
        ORDER BY total_profesores DESC, c.nombre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Profesores activos por carrera con información completa:\n\n"
            total_general = 0
            
            for row in result:
                response += f"{row['carrera']}: {row['total_profesores']} profesores\n"
                if row['profesores'] and row['total_profesores'] > 0:
                    response += f"  Profesores: {row['profesores']}\n"
                response += "\n"
                total_general += row['total_profesores']
            
            response += f"Total en el sistema: {total_general} profesores activos"
            return response
        else:
            return "No pude obtener información de profesores por carrera."

    elif intent == 'teachers_most_load':
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido,
            COUNT(DISTINCT pag.grupo_id) as total_grupos,
            COUNT(DISTINCT pag.asignatura_id) as total_asignaturas,
            GROUP_CONCAT(DISTINCT g.codigo ORDER BY g.codigo) as grupos,
            GROUP_CONCAT(DISTINCT a.nombre ORDER BY a.nombre) as asignaturas
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN profesor_asignatura_grupo pag ON p.id = pag.profesor_id
        JOIN grupos g ON pag.grupo_id = g.id
        JOIN asignaturas a ON pag.asignatura_id = a.id
        WHERE p.activo = TRUE AND pag.activo = TRUE
        GROUP BY p.id, p.numero_empleado, u.nombre, u.apellido
        ORDER BY (total_grupos + total_asignaturas) DESC, total_grupos DESC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No encontré asignaciones de profesores."
            
            response = "Profesores con más grupos o asignaturas asignadas:\n\n"
            
            for i, row in enumerate(result, 1):
                carga_total = row['total_grupos'] + row['total_asignaturas']
                response += f"{i}. Número empleado: {row['numero_empleado']}\n"
                response += f"   Nombre: {row['nombre']} {row['apellido']}\n"
                response += f"   Grupos asignados: {row['total_grupos']}\n"
                response += f"   Asignaturas: {row['total_asignaturas']}\n"
                response += f"   Carga total: {carga_total}\n"
                response += f"   Lista de grupos: {row['grupos']}\n"
                if len(row['asignaturas']) > 100:
                    response += f"   Lista de asignaturas: {row['asignaturas'][:100]}...\n"
                else:
                    response += f"   Lista de asignaturas: {row['asignaturas']}\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de carga académica."

    elif intent == 'teachers_low_grades':
        query = """
        SELECT p.numero_empleado, u.nombre, u.apellido,
            COUNT(c.id) as calificaciones_bajas,
            COUNT(DISTINCT a.matricula) as alumnos_afectados,
            GROUP_CONCAT(DISTINCT CONCAT(ua.nombre, ' ', ua.apellido, ' (', a.matricula, ') - Grupo ', g.codigo, ' - ', g.cuatrimestre, ' cuatrimestre') 
                        ORDER BY ua.apellido SEPARATOR '; ') as detalle_completo
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN calificaciones c ON p.id = c.profesor_id
        JOIN alumnos a ON c.alumno_id = a.id
        JOIN usuarios ua ON a.usuario_id = ua.id
        JOIN grupos g ON c.grupo_id = g.id
        WHERE c.calificacion_final <= 7 AND c.calificacion_final IS NOT NULL
        GROUP BY p.id, p.numero_empleado, u.nombre, u.apellido
        ORDER BY calificaciones_bajas DESC, alumnos_afectados DESC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No hay profesores con calificaciones menores o iguales a 7 registradas."
            
            response = "Profesores que han registrado más calificaciones con resultado menor o igual a 7:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. Número empleado: {row['numero_empleado']}\n"
                response += f"   Nombre: {row['nombre']} {row['apellido']}\n"
                response += f"   Calificaciones <= 7: {row['calificaciones_bajas']}\n"
                response += f"   Alumnos afectados: {row['alumnos_afectados']}\n"
                if len(row['detalle_completo']) > 300:
                    response += f"   Detalle alumnos y grupos: {row['detalle_completo'][:300]}...\n"
                else:
                    response += f"   Detalle alumnos y grupos: {row['detalle_completo']}\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de calificaciones bajas."

    elif intent == 'teacher_complete_info':
        if 'numero_empleado' not in entities:
            return "Para obtener toda la información del profesor necesito su matrícula (número de empleado EMP###)."
        
        numero_emp = entities['numero_empleado']
        
        query_complete = """
        SELECT p.numero_empleado, u.nombre, u.apellido, u.correo,
            p.telefono, p.extension, p.fecha_contratacion,
            p.titulo_academico, p.especialidad, p.cedula_profesional,
            p.experiencia_años, p.es_tutor, c.nombre as carrera,
            p.activo, p.direccion, p.fecha_nacimiento
        FROM profesores p
        JOIN usuarios u ON p.usuario_id = u.id
        JOIN carreras c ON p.carrera_id = c.id
        WHERE p.numero_empleado = %s
        """
        
        query_subjects = """
        SELECT DISTINCT a.nombre as asignatura, g.codigo as grupo,
            g.cuatrimestre, pag.ciclo_escolar
        FROM profesor_asignatura_grupo pag
        JOIN profesores p ON pag.profesor_id = p.id
        JOIN asignaturas a ON pag.asignatura_id = a.id
        JOIN grupos g ON pag.grupo_id = g.id
        WHERE p.numero_empleado = %s AND pag.activo = TRUE
        ORDER BY pag.ciclo_escolar DESC, g.cuatrimestre, a.nombre
        """
        
        query_tutor = """
        SELECT g.codigo as grupo, COUNT(ag.alumno_id) as total_alumnos,
            g.cuatrimestre, car.nombre as carrera
        FROM grupos g
        JOIN profesores p ON g.profesor_tutor_id = p.id
        JOIN carreras car ON g.carrera_id = car.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        WHERE p.numero_empleado = %s AND g.activo = TRUE
        GROUP BY g.id, g.codigo, g.cuatrimestre, car.nombre
        """
        
        query_grades_stats = """
        SELECT COUNT(*) as total_calificaciones,
            AVG(calificacion_final) as promedio_otorgado,
            COUNT(CASE WHEN calificacion_final < 7 THEN 1 END) as calificaciones_bajas,
            COUNT(CASE WHEN calificacion_final >= 9 THEN 1 END) as calificaciones_altas
        FROM calificaciones c
        JOIN profesores p ON c.profesor_id = p.id
        WHERE p.numero_empleado = %s AND c.calificacion_final IS NOT NULL
        """
        
        basic_info = execute_query(query_complete, (numero_emp,))
        subjects_info = execute_query(query_subjects, (numero_emp,))
        tutor_info = execute_query(query_tutor, (numero_emp,))
        grades_stats = execute_query(query_grades_stats, (numero_emp,))
        
        if not basic_info or isinstance(basic_info, dict) or not basic_info:
            return f"No encontré información para el profesor con matrícula {numero_emp}."
        
        data = basic_info[0]
        response = f"INFORMACIÓN COMPLETA DEL PROFESOR - TODO LO RELACIONADO\n\n"
        response += f"DATOS PERSONALES:\n"
        response += f"  Matrícula (Número empleado): {data['numero_empleado']}\n"
        response += f"  Nombre completo: {data['nombre']} {data['apellido']}\n"
        response += f"  Correo electrónico: {data['correo']}\n"
        response += f"  Estado: {'Activo' if data['activo'] else 'Inactivo'}\n"
        
        if data['telefono']:
            response += f"  Teléfono: {data['telefono']}\n"
        if data['extension']:
            response += f"  Extensión: {data['extension']}\n"
        if data['direccion']:
            response += f"  Dirección: {data['direccion']}\n"
        if data['fecha_nacimiento']:
            response += f"  Fecha nacimiento: {data['fecha_nacimiento']}\n"
        
        response += f"\nDATOS ACADÉMICOS Y PROFESIONALES:\n"
        response += f"  Carrera asignada: {data['carrera']}\n"
        response += f"  Fecha contratación: {data['fecha_contratacion']}\n"
        response += f"  Experiencia: {data['experiencia_años']} años\n"
        response += f"  Es tutor de grupo: {'Sí' if data['es_tutor'] else 'No'}\n"
        
        if data['titulo_academico']:
            response += f"  Título académico: {data['titulo_academico']}\n"
        if data['especialidad']:
            response += f"  Especialidad: {data['especialidad']}\n"
        if data['cedula_profesional']:
            response += f"  Cédula profesional: {data['cedula_profesional']}\n"
        
        if subjects_info and not isinstance(subjects_info, dict):
            response += f"\nASIGNATURAS QUE IMPARTE ({len(subjects_info)}):\n"
            current_cycle = None
            for subj in subjects_info:
                if current_cycle != subj['ciclo_escolar']:
                    current_cycle = subj['ciclo_escolar']
                    response += f"  {current_cycle}:\n"
                response += f"    - {subj['asignatura']} | Grupo {subj['grupo']} ({subj['cuatrimestre']} cuatrimestre)\n"
        
        if data['es_tutor'] and tutor_info and not isinstance(tutor_info, dict):
            response += f"\nGRUPOS COMO TUTOR:\n"
            for tutor in tutor_info:
                response += f"  - Grupo {tutor['grupo']} ({tutor['carrera']}) - {tutor['cuatrimestre']} cuatrimestre\n"
                response += f"    Alumnos a cargo: {tutor['total_alumnos']}\n"
        
        if grades_stats and not isinstance(grades_stats, dict) and grades_stats[0]['total_calificaciones'] > 0:
            grade_data = grades_stats[0]
            response += f"\nESTADÍSTICAS DE CALIFICACIONES:\n"
            response += f"  Total calificaciones registradas: {grade_data['total_calificaciones']}\n"
            response += f"  Promedio general otorgado: {grade_data['promedio_otorgado']:.2f}\n"
            response += f"  Calificaciones < 7: {grade_data['calificaciones_bajas']}\n"
            response += f"  Calificaciones >= 9: {grade_data['calificaciones_altas']}\n"
        
        return response

    elif intent == 'most_used_classrooms':
        query = """
        SELECT h.aula, COUNT(*) as frecuencia_uso,
                GROUP_CONCAT(DISTINCT h.dia_semana ORDER BY h.dia_semana) as dias,
                GROUP_CONCAT(DISTINCT CONCAT(ua.nombre, ' ', ua.apellido, ' (', a.matricula, ') - Grupo ', g.codigo, ' - ', g.cuatrimestre, ' cuatrimestre') 
                            ORDER BY ua.apellido SEPARATOR '; ') as alumnos_profesores_info
        FROM horarios h
        JOIN profesor_asignatura_grupo pag ON h.profesor_asignatura_grupo_id = pag.id
        JOIN grupos g ON pag.grupo_id = g.id
        JOIN asignaturas asig ON pag.asignatura_id = asig.id
        JOIN profesores p ON pag.profesor_id = p.id
        JOIN usuarios up ON p.usuario_id = up.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        LEFT JOIN alumnos a ON ag.alumno_id = a.id
        LEFT JOIN usuarios ua ON a.usuario_id = ua.id
        WHERE h.activo = TRUE AND h.aula IS NOT NULL AND h.aula != ''
        GROUP BY h.aula
        ORDER BY frecuencia_uso DESC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No encontré información de uso de aulas."
            
            response = "Aulas que se usan con más frecuencia durante la semana:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. Aula {row['aula']}\n"
                response += f"   Frecuencia de uso: {row['frecuencia_uso']} horarios\n"
                response += f"   Días de uso: {row['dias']}\n"
                if row['alumnos_profesores_info']:
                    if len(row['alumnos_profesores_info']) > 200:
                        response += f"   Información alumnos y grupos: {row['alumnos_profesores_info'][:200]}...\n"
                    else:
                        response += f"   Información alumnos y grupos: {row['alumnos_profesores_info']}\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de uso de aulas."

    elif intent == 'groups_by_career_semester':
        query = """
        SELECT c.nombre as carrera, g.cuatrimestre,
                COUNT(g.id) as total_grupos,
                SUM(CASE WHEN g.activo = TRUE THEN 1 ELSE 0 END) as grupos_activos,
                AVG(subq.alumnos_por_grupo) as promedio_alumnos
        FROM carreras c
        LEFT JOIN grupos g ON c.id = g.carrera_id
        LEFT JOIN (
            SELECT g.id, COUNT(ag.alumno_id) as alumnos_por_grupo
            FROM grupos g
            LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
            GROUP BY g.id
        ) subq ON g.id = subq.id
        WHERE c.activa = TRUE
        GROUP BY c.id, c.nombre, g.cuatrimestre
        HAVING total_grupos > 0
        ORDER BY c.nombre, g.cuatrimestre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Grupos activos por carrera y cuatrimestre:\n\n"
            current_career = None
            total_general = 0
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  Cuatrimestre {row['cuatrimestre']}: {row['grupos_activos']} grupos activos (de {row['total_grupos']} total)\n"
                if row['promedio_alumnos']:
                    response += f"    Promedio de alumnos por grupo: {row['promedio_alumnos']:.1f}\n"
                total_general += row['grupos_activos']
            
            response += f"\nTotal general: {total_general} grupos activos en el sistema"
            return response
        else:
            return "No pude obtener información de grupos por carrera y cuatrimestre."

    elif intent == 'tutor_groups':
        if 'numero_empleado' in entities:
            numero_emp = entities['numero_empleado']
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
            GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre, u.nombre, u.apellido, p.numero_empleado
            ORDER BY c.nombre, g.cuatrimestre
            """
            result = execute_query(query, (numero_emp,))
            if result and not isinstance(result, dict):
                if not result:
                    return f"El profesor con matrícula {numero_emp} no tiene grupos asignados como tutor."
                
                data = result[0]
                response = f"Grupos que tiene este tutor:\n"
                response += f"Profesor: {data['nombre']} {data['apellido']} (Matrícula: {data['numero_empleado']})\n\n"
                for row in result:
                    response += f"  - Grupo: {row['codigo']} ({row['cuatrimestre']} cuatrimestre)\n"
                    response += f"    Carrera: {row['carrera']}\n"
                    response += f"    Alumnos: {row['total_alumnos']}\n\n"
                
                return response
        elif 'nombre_tutor' in entities:
            nombre = entities['nombre_tutor']
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
            GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre, u.nombre, u.apellido, p.numero_empleado
            ORDER BY c.nombre, g.cuatrimestre
            """
            result = execute_query(query, (f"%{nombre}%",))
            if result and not isinstance(result, dict):
                if not result:
                    return f"No encontré profesor tutor con el nombre {nombre}."
                
                data = result[0]
                response = f"Grupos que tiene este tutor:\n"
                response += f"Profesor: {data['nombre']} {data['apellido']} (Matrícula: {data['numero_empleado']})\n\n"
                for row in result:
                    response += f"  - Grupo: {row['codigo']} ({row['cuatrimestre']} cuatrimestre)\n"
                    response += f"    Carrera: {row['carrera']}\n"
                    response += f"    Alumnos: {row['total_alumnos']}\n\n"
                
                return response
        
        return "Para consultar los grupos de un tutor necesito su matrícula (número empleado) o nombre completo."

    elif intent == 'groups_most_risk':
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
                COUNT(DISTINCT a.id) as total_alumnos,
                COUNT(rr.id) as total_reportes,
                ROUND(COUNT(rr.id) / COUNT(DISTINCT a.id), 2) as reportes_por_alumno
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        JOIN alumnos a ON ag.alumno_id = a.id
        JOIN reportes_riesgo rr ON a.id = rr.alumno_id AND rr.estado IN ('abierto', 'en_proceso')
        WHERE g.activo = TRUE
        GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre
        ORDER BY total_reportes DESC, reportes_por_alumno DESC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No hay grupos con reportes de riesgo activos."
            
            response = "Grupos que tienen mayor número de reportes de riesgo por alumno:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. Grupo: {row['codigo']} ({row['cuatrimestre']} cuatrimestre)\n"
                response += f"   Carrera: {row['carrera']}\n"
                response += f"   Total alumnos: {row['total_alumnos']}\n"
                response += f"   Total reportes de riesgo: {row['total_reportes']}\n"
                response += f"   Reportes por alumno: {row['reportes_por_alumno']}\n"
                if row['reportes_por_alumno'] > 1.5:
                    response += f"   GRUPO DE ALTO RIESGO\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de reportes de riesgo por grupo."

    elif intent == 'most_viewed_news':
        query = """
        SELECT n.titulo, n.contenido, n.fecha_publicacion,
                n.vistas, n.categoria,
                CONCAT(u.nombre, ' ', u.apellido) as autor
        FROM noticias n
        LEFT JOIN usuarios u ON n.autor_id = u.id
        WHERE n.activa = TRUE
        ORDER BY n.vistas DESC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron noticias en el sistema."
            
            response = "Noticias que han sido más vistas por los usuarios:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. {row['titulo']}\n"
                response += f"   Autor: {row['autor'] or 'No especificado'}\n"
                response += f"   Categoría: {row['categoria']}\n"
                response += f"   Fecha publicación: {row['fecha_publicacion']}\n"
                response += f"   Total vistas: {row['vistas']}\n"
                if len(row['contenido']) > 150:
                    response += f"   Resumen: {row['contenido'][:150]}...\n"
                else:
                    response += f"   Contenido: {row['contenido']}\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de noticias más vistas."

    elif intent == 'groups_lowest_average':
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera,
                COUNT(DISTINCT a.id) as total_alumnos,
                AVG(cal.calificacion_final) as promedio_grupo,
                COUNT(CASE WHEN cal.calificacion_final < 7 THEN 1 END) as reprobados
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        LEFT JOIN alumnos a ON ag.alumno_id = a.id
        LEFT JOIN calificaciones cal ON a.id = cal.alumno_id AND cal.calificacion_final IS NOT NULL
        WHERE g.activo = TRUE
        GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre
        HAVING COUNT(cal.id) > 0
        ORDER BY promedio_grupo ASC
        LIMIT 10
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron grupos con calificaciones registradas."
            
            response = "Grupos que tienen promedio final más bajo:\n\n"
            
            for i, row in enumerate(result, 1):
                response += f"{i}. Nombre del grupo: {row['codigo']}\n"
                response += f"   Cuatrimestre: {row['cuatrimestre']}\n"
                response += f"   Carrera: {row['carrera']}\n"
                response += f"   Cuántos alumnos tiene: {row['total_alumnos']}\n"
                response += f"   Promedio final del grupo: {row['promedio_grupo']:.2f}\n"
                response += f"   Alumnos reprobados: {row['reprobados']}\n"
                if row['promedio_grupo'] < 7:
                    response += f"   GRUPO REQUIERE ATENCIÓN URGENTE\n"
                response += "\n"
            
            return response
        else:
            return "No pude obtener información de promedios por grupo."

    elif intent == 'subjects_no_grades':
        query = """
        SELECT a.nombre as asignatura, g.codigo as grupo, g.cuatrimestre,
                car.nombre as carrera,
                CONCAT(u.nombre, ' ', u.apellido) as profesor,
                p.numero_empleado
        FROM profesor_asignatura_grupo pag
        JOIN asignaturas a ON pag.asignatura_id = a.id
        JOIN grupos g ON pag.grupo_id = g.id
        JOIN carreras car ON g.carrera_id = car.id
        JOIN profesores p ON pag.profesor_id = p.id
        JOIN usuarios u ON p.usuario_id = u.id
        LEFT JOIN calificaciones cal ON pag.grupo_id = cal.grupo_id 
                                    AND pag.asignatura_id = cal.asignatura_id
        WHERE pag.activo = TRUE AND g.activo = TRUE
        AND cal.id IS NULL
        ORDER BY car.nombre, g.cuatrimestre, a.nombre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "Todas las asignaturas tienen calificaciones registradas."
            
            response = "Asignaturas que tienen calificaciones aún sin registrar:\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    response += f"{current_career}:\n"
                
                response += f"  Nombre de la asignatura: {row['asignatura']}\n"
                response += f"  Profesor que da la materia: {row['profesor']} ({row['numero_empleado']})\n"
                response += f"  Grupo al que le da: {row['grupo']} ({row['cuatrimestre']} cuatrimestre)\n"
                response += f"  REQUIERE REGISTRO DE CALIFICACIONES\n\n"
            
            return response
        else:
            return "No pude obtener información de asignaturas sin calificaciones."

    elif intent == 'forum_categories':
        query = """
        SELECT nombre, descripcion, activa,
                (SELECT COUNT(*) FROM publicaciones_foro pf WHERE pf.categoria_id = cf.id) as total_publicaciones
        FROM categorias_foro cf
        ORDER BY total_publicaciones DESC, nombre
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            if not result:
                return "No se encontraron categorías del foro."
            
            response = "Categorías del foro disponibles:\n\n"
            
            for i, row in enumerate(result, 1):
                status = "Activa" if row['activa'] else "Inactiva"
                response += f"{i}. Categoría: {row['nombre']} ({status})\n"
                response += f"   Descripción: {row['descripcion']}\n"
                response += f"   Total publicaciones: {row['total_publicaciones']}\n\n"
            
            return response
        else:
            return "No pude obtener información de las categorías del foro."

    elif intent == 'posts_by_category':
        query = """
        SELECT cf.nombre as categoria, COUNT(pf.id) as total_publicaciones,
                COUNT(CASE WHEN pf.activa = TRUE THEN 1 END) as publicaciones_activas
        FROM categorias_foro cf
        LEFT JOIN publicaciones_foro pf ON cf.id = pf.categoria_id
        WHERE cf.activa = TRUE
        GROUP BY cf.id, cf.nombre
        ORDER BY total_publicaciones DESC
        """
        result = execute_query(query)
        if result and not isinstance(result, dict):
            response = "Publicaciones por categoría de foro:\n\n"
            total_general = 0
            
            for row in result:
                response += f"Categoría: {row['categoria']}\n"
                response += f"  Total publicaciones: {row['total_publicaciones']}\n"
                response += f"  Publicaciones activas: {row['publicaciones_activas']}\n\n"
                total_general += row['total_publicaciones']
            
            response += f"Total general: {total_general} publicaciones en todas las categorías"
            return response
        else:
            return "No pude obtener información de publicaciones por categoría."
    else:
        return "No tengo suficiente información en la base de datos para responder esa pregunta específica. ¿Podrías intentar con una consulta diferente?"
    

@app.route('/api/chat', methods=['POST'])
def chat():
    data = request.json
    message = data.get('message', '')
    
    intent = detect_intent(message)
    entities = extract_entities(message, intent)
    response = generate_response(intent, entities, message)
    
    return jsonify({
        "response": response,
        "intent": intent,
        "conversational": True
    })

@app.route('/test', methods=['GET'])
def test():
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM usuarios")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()
        return jsonify({"status": "OK", "message": f"Conexión exitosa. {count} usuarios en la base de datos."})
    except Exception as e:
        return jsonify({"status": "ERROR", "message": str(e)})

if __name__ == '__main__':
    print("Chatbot academico completo iniciado en puerto 5001")
    print("Endpoint principal: POST /api/chat")
    print("Prueba conexion: GET /test")
    app.run(debug=True, port=5001)