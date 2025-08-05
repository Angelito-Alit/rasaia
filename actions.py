from typing import Any, Text, Dict, List
from rasa_sdk import Action, Tracker
from rasa_sdk.executor import CollectingDispatcher
import mysql.connector
import os
from datetime import datetime

class DatabaseConnector:
    def __init__(self):
        self.config = {
            'host': 'bluebyte.space',
            'user': 'bluebyte_angel',
            'password': 'orbitalsoft',
            'database': 'bluebyte_dtai_web',
            'port': 3306
        }
    
    def get_connection(self):
        return mysql.connector.connect(**self.config)
    
    def execute_query(self, query, params=None):
        try:
            conn = self.get_connection()
            cursor = conn.cursor(dictionary=True)
            cursor.execute(query, params or ())
            result = cursor.fetchall()
            cursor.close()
            conn.close()
            return result
        except Exception as e:
            print(f"Database error: {e}")
            return None

db = DatabaseConnector()

class ActionGetStudentsCount(Action):
    def name(self) -> Text:
        return "action_get_students_count"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        query = """
        SELECT COUNT(*) as total,
               SUM(CASE WHEN estado_alumno = 'activo' THEN 1 ELSE 0 END) as activos,
               SUM(CASE WHEN estado_alumno = 'egresado' THEN 1 ELSE 0 END) as egresados
        FROM alumnos
        """
        
        result = db.execute_query(query)
        
        if result:
            data = result[0]
            message = f"Total de alumnos: {data['total']}\n"
            message += f"Activos: {data['activos']}\n"
            message += f"Egresados: {data['egresados']}"
        else:
            message = "No se pudo obtener la información de estudiantes."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetStudentsByCareer(Action):
    def name(self) -> Text:
        return "action_get_students_by_career"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        carrera = tracker.get_slot("carrera_nombre")
        
        if not carrera:
            query = """
            SELECT c.nombre, COUNT(a.id) as total_alumnos
            FROM carreras c
            LEFT JOIN alumnos a ON c.id = a.carrera_id AND a.estado_alumno = 'activo'
            WHERE c.activa = TRUE
            GROUP BY c.id, c.nombre
            ORDER BY total_alumnos DESC
            """
            result = db.execute_query(query)
            
            if result:
                message = "Alumnos por carrera:\n"
                for row in result:
                    message += f"{row['nombre']}: {row['total_alumnos']} alumnos\n"
            else:
                message = "No se pudo obtener la información de carreras."
        else:
            query = """
            SELECT c.nombre, COUNT(a.id) as total_alumnos,
                   AVG(a.promedio_general) as promedio_carrera
            FROM carreras c
            LEFT JOIN alumnos a ON c.id = a.carrera_id AND a.estado_alumno = 'activo'
            WHERE c.nombre LIKE %s AND c.activa = TRUE
            GROUP BY c.id, c.nombre
            """
            result = db.execute_query(query, (f"%{carrera}%",))
            
            if result and result[0]['total_alumnos'] > 0:
                data = result[0]
                message = f"Carrera: {data['nombre']}\n"
                message += f"Total de alumnos activos: {data['total_alumnos']}\n"
                if data['promedio_carrera']:
                    message += f"Promedio general de la carrera: {data['promedio_carrera']:.2f}"
            else:
                message = f"No se encontraron alumnos en la carrera '{carrera}' o la carrera no existe."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetStudentAverage(Action):
    def name(self) -> Text:
        return "action_get_student_average"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        matricula = tracker.get_slot("matricula")
        
        if not matricula:
            message = "Por favor proporciona la matrícula del estudiante."
        else:
            query = """
            SELECT u.nombre, u.apellido, a.matricula, a.promedio_general,
                   a.cuatrimestre_actual, c.nombre as carrera, a.estado_alumno
            FROM alumnos a
            JOIN usuarios u ON a.usuario_id = u.id
            JOIN carreras c ON a.carrera_id = c.id
            WHERE a.matricula = %s
            """
            result = db.execute_query(query, (matricula,))
            
            if result:
                data = result[0]
                message = f"Estudiante: {data['nombre']} {data['apellido']}\n"
                message += f"Matrícula: {data['matricula']}\n"
                message += f"Carrera: {data['carrera']}\n"
                message += f"Cuatrimestre actual: {data['cuatrimestre_actual']}\n"
                message += f"Promedio general: {data['promedio_general']}\n"
                message += f"Estado: {data['estado_alumno']}"
                
                if data['promedio_general'] < 7.0:
                    message += "\n\nRecomendación: El promedio está por debajo del mínimo requerido. Se sugiere apoyo académico."
                elif data['promedio_general'] >= 9.0:
                    message += "\n\nExcelente desempeño académico!"
            else:
                message = f"No se encontró información para la matrícula '{matricula}'."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetRiskReports(Action):
    def name(self) -> Text:
        return "action_get_risk_reports"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        query = """
        SELECT estado, nivel_riesgo, tipo_riesgo, COUNT(*) as total
        FROM reportes_riesgo
        GROUP BY estado, nivel_riesgo, tipo_riesgo
        ORDER BY 
            CASE nivel_riesgo 
                WHEN 'critico' THEN 1 
                WHEN 'alto' THEN 2 
                WHEN 'medio' THEN 3 
                WHEN 'bajo' THEN 4 
            END,
            estado
        """
        
        result = db.execute_query(query)
        
        if result:
            message = "Reportes de riesgo:\n\n"
            current_level = None
            
            for row in result:
                if current_level != row['nivel_riesgo']:
                    current_level = row['nivel_riesgo']
                    message += f"NIVEL {current_level.upper()}:\n"
                
                message += f"  {row['tipo_riesgo']} ({row['estado']}): {row['total']}\n"
            
            query_summary = """
            SELECT 
                SUM(CASE WHEN estado = 'abierto' THEN 1 ELSE 0 END) as abiertos,
                SUM(CASE WHEN nivel_riesgo = 'critico' THEN 1 ELSE 0 END) as criticos
            FROM reportes_riesgo
            """
            summary = db.execute_query(query_summary)
            if summary:
                data = summary[0]
                message += f"\nResumen: {data['abiertos']} reportes abiertos, {data['criticos']} casos críticos"
        else:
            message = "No se encontraron reportes de riesgo."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetGroupSchedule(Action):
    def name(self) -> Text:
        return "action_get_group_schedule"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        grupo_codigo = tracker.get_slot("grupo_codigo")
        
        if not grupo_codigo:
            message = "Por favor proporciona el código del grupo."
        else:
            query = """
            SELECT h.dia_semana, h.hora_inicio, h.hora_fin, h.aula,
                   a.nombre as asignatura, h.tipo_clase,
                   CONCAT(u.nombre, ' ', u.apellido) as profesor
            FROM horarios h
            JOIN profesor_asignatura_grupo pag ON h.profesor_asignatura_grupo_id = pag.id
            JOIN grupos g ON pag.grupo_id = g.id
            JOIN asignaturas a ON pag.asignatura_id = a.id
            JOIN profesores p ON pag.profesor_id = p.id
            JOIN usuarios u ON p.usuario_id = u.id
            WHERE g.codigo = %s AND h.activo = TRUE AND g.activo = TRUE
            ORDER BY 
                CASE h.dia_semana
                    WHEN 'lunes' THEN 1
                    WHEN 'martes' THEN 2
                    WHEN 'miercoles' THEN 3
                    WHEN 'jueves' THEN 4
                    WHEN 'viernes' THEN 5
                    WHEN 'sabado' THEN 6
                END,
                h.hora_inicio
            """
            result = db.execute_query(query)
            
            if result:
                message = f"Horario del grupo {grupo_codigo}:\n\n"
                current_day = None
                
                for row in result:
                    if current_day != row['dia_semana']:
                        current_day = row['dia_semana']
                        message += f"{current_day.upper()}:\n"
                    
                    message += f"  {row['hora_inicio']} - {row['hora_fin']} | {row['asignatura']}\n"
                    message += f"    Profesor: {row['profesor']} | Aula: {row['aula']} | Tipo: {row['tipo_clase']}\n"
            else:
                message = f"No se encontró horario para el grupo '{grupo_codigo}' o el grupo no existe."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetTeacherSubjects(Action):
    def name(self) -> Text:
        return "action_get_teacher_subjects"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        numero_empleado = tracker.get_slot("numero_empleado")
        
        if not numero_empleado:
            message = "Por favor proporciona el número de empleado del profesor."
        else:
            query = """
            SELECT DISTINCT a.nombre as asignatura, g.codigo as grupo,
                   g.cuatrimestre, c.nombre as carrera, pag.ciclo_escolar
            FROM profesor_asignatura_grupo pag
            JOIN profesores p ON pag.profesor_id = p.id
            JOIN asignaturas a ON pag.asignatura_id = a.id
            JOIN grupos g ON pag.grupo_id = g.id
            JOIN carreras c ON g.carrera_id = c.id
            JOIN usuarios u ON p.usuario_id = u.id
            WHERE p.numero_empleado = %s AND pag.activo = TRUE
            ORDER BY g.cuatrimestre, a.nombre
            """
            result = db.execute_query(query)
            
            if result:
                query_teacher = """
                SELECT CONCAT(u.nombre, ' ', u.apellido) as nombre_completo,
                       p.titulo_academico, p.especialidad
                FROM profesores p
                JOIN usuarios u ON p.usuario_id = u.id
                WHERE p.numero_empleado = %s
                """
                teacher_info = db.execute_query(query_teacher, (numero_empleado,))
                
                if teacher_info:
                    teacher = teacher_info[0]
                    message = f"Profesor: {teacher['nombre_completo']}\n"
                    if teacher['titulo_academico']:
                        message += f"Título: {teacher['titulo_academico']}\n"
                    if teacher['especialidad']:
                        message += f"Especialidad: {teacher['especialidad']}\n"
                    message += "\nAsignaturas que imparte:\n"
                    
                    for row in result:
                        message += f"• {row['asignatura']} - Grupo {row['grupo']}\n"
                        message += f"  {row['carrera']} | {row['cuatrimestre']}° cuatrimestre | {row['ciclo_escolar']}\n"
                else:
                    message = "Asignaturas:\n"
                    for row in result:
                        message += f"• {row['asignatura']} - Grupo {row['grupo']}\n"
            else:
                message = f"No se encontraron asignaturas para el empleado '{numero_empleado}' o no existe."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetSubjectsBySemester(Action):
    def name(self) -> Text:
        return "action_get_subjects_by_semester"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        cuatrimestre = tracker.get_slot("cuatrimestre")
        
        if not cuatrimestre:
            message = "Por favor especifica el cuatrimestre."
        else:
            try:
                cuatrimestre_num = int(cuatrimestre) if cuatrimestre.isdigit() else self.parse_cuatrimestre(cuatrimestre)
                
                query = """
                SELECT a.nombre, a.codigo, a.horas_teoricas, a.horas_practicas,
                       a.complejidad, c.nombre as carrera
                FROM asignaturas a
                JOIN carreras c ON a.carrera_id = c.id
                WHERE a.cuatrimestre = %s AND a.activa = TRUE
                ORDER BY c.nombre, a.nombre
                """
                result = db.execute_query(query, (cuatrimestre_num,))
                
                if result:
                    message = f"Asignaturas del {cuatrimestre_num}° cuatrimestre:\n\n"
                    current_career = None
                    
                    for row in result:
                        if current_career != row['carrera']:
                            current_career = row['carrera']
                            message += f"{current_career}:\n"
                        
                        total_horas = row['horas_teoricas'] + row['horas_practicas']
                        message += f"• {row['nombre']} ({row['codigo']})\n"
                        message += f"  Horas: {total_horas} ({row['horas_teoricas']}T + {row['horas_practicas']}P)\n"
                        message += f"  Complejidad: {row['complejidad']}/10\n"
                else:
                    message = f"No se encontraron asignaturas para el {cuatrimestre_num}° cuatrimestre."
            except ValueError:
                message = "Formato de cuatrimestre no válido. Usa números del 1 al 9."
        
        dispatcher.utter_message(text=message)
        return []
    
    def parse_cuatrimestre(self, text):
        cuatrimestres = {
            'primer': 1, 'primero': 1, 'segundo': 2, 'tercer': 3, 'tercero': 3,
            'cuarto': 4, 'quinto': 5, 'sexto': 6, 'septimo': 7, 'séptimo': 7,
            'octavo': 8, 'noveno': 9
        }
        return cuatrimestres.get(text.lower(), 1)

class ActionGetStudentGrades(Action):
    def name(self) -> Text:
        return "action_get_student_grades"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        matricula = tracker.get_slot("matricula")
        
        if not matricula:
            message = "Por favor proporciona la matrícula del estudiante."
        else:
            query = """
            SELECT a.nombre as asignatura, c.parcial_1, c.parcial_2, c.parcial_3,
                   c.calificacion_ordinario, c.calificacion_extraordinario,
                   c.calificacion_final, c.estatus, c.ciclo_escolar
            FROM calificaciones c
            JOIN asignaturas a ON c.asignatura_id = a.id
            JOIN alumnos al ON c.alumno_id = al.id
            WHERE al.matricula = %s
            ORDER BY c.ciclo_escolar DESC, a.nombre
            """
            result = db.execute_query(query, (matricula,))
            
            if result:
                message = f"Calificaciones del estudiante {matricula}:\n\n"
                current_cycle = None
                
                for row in result:
                    if current_cycle != row['ciclo_escolar']:
                        current_cycle = row['ciclo_escolar']
                        message += f"CICLO {current_cycle}:\n"
                    
                    message += f"• {row['asignatura']}\n"
                    message += f"  Parciales: {row['parcial_1'] or 'N/A'} | {row['parcial_2'] or 'N/A'} | {row['parcial_3'] or 'N/A'}\n"
                    
                    if row['calificacion_final']:
                        message += f"  Final: {row['calificacion_final']} | Estado: {row['estatus']}\n"
                    elif row['calificacion_ordinario']:
                        message += f"  Ordinario: {row['calificacion_ordinario']} | Estado: {row['estatus']}\n"
                    else:
                        message += f"  Estado: {row['estatus']}\n"
                
                query_stats = """
                SELECT AVG(calificacion_final) as promedio,
                       COUNT(*) as total_materias,
                       SUM(CASE WHEN estatus = 'aprobado' THEN 1 ELSE 0 END) as aprobadas,
                       SUM(CASE WHEN estatus = 'reprobado' THEN 1 ELSE 0 END) as reprobadas
                FROM calificaciones c
                JOIN alumnos al ON c.alumno_id = al.id
                WHERE al.matricula = %s AND calificacion_final IS NOT NULL
                """
                stats = db.execute_query(query_stats, (matricula,))
                
                if stats and stats[0]['total_materias'] > 0:
                    data = stats[0]
                    message += f"\nResumen:\n"
                    message += f"Promedio: {data['promedio']:.2f}\n"
                    message += f"Materias: {data['aprobadas']} aprobadas, {data['reprobadas']} reprobadas"
            else:
                message = f"No se encontraron calificaciones para la matrícula '{matricula}'."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetActiveGroups(Action):
    def name(self) -> Text:
        return "action_get_active_groups"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        query = """
        SELECT g.codigo, g.cuatrimestre, c.nombre as carrera, g.periodo, g.año,
               COUNT(ag.alumno_id) as total_alumnos, g.capacidad_maxima,
               CONCAT(u.nombre, ' ', u.apellido) as tutor
        FROM grupos g
        JOIN carreras c ON g.carrera_id = c.id
        LEFT JOIN alumnos_grupos ag ON g.id = ag.grupo_id AND ag.activo = TRUE
        LEFT JOIN profesores p ON g.profesor_tutor_id = p.id
        LEFT JOIN usuarios u ON p.usuario_id = u.id
        WHERE g.activo = TRUE
        GROUP BY g.id, g.codigo, g.cuatrimestre, c.nombre, g.periodo, g.año, g.capacidad_maxima, u.nombre, u.apellido
        ORDER BY c.nombre, g.cuatrimestre, g.codigo
        """
        
        result = db.execute_query(query)
        
        if result:
            message = "Grupos activos:\n\n"
            current_career = None
            
            for row in result:
                if current_career != row['carrera']:
                    current_career = row['carrera']
                    message += f"{current_career}:\n"
                
                ocupacion = (row['total_alumnos'] / row['capacidad_maxima']) * 100 if row['capacidad_maxima'] > 0 else 0
                
                message += f"• {row['codigo']} - {row['cuatrimestre']}° cuatrimestre\n"
                message += f"  Período: {row['periodo']} {row['año']}\n"
                message += f"  Alumnos: {row['total_alumnos']}/{row['capacidad_maxima']} ({ocupacion:.1f}%)\n"
                if row['tutor']:
                    message += f"  Tutor: {row['tutor']}\n"
        else:
            message = "No se encontraron grupos activos."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetHelpRequests(Action):
    def name(self) -> Text:
        return "action_get_help_requests"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        query = """
        SELECT sa.tipo_problema, sa.urgencia, sa.estado, COUNT(*) as total,
               AVG(DATEDIFF(COALESCE(sa.fecha_respuesta, NOW()), sa.fecha_solicitud)) as dias_promedio
        FROM solicitudes_ayuda sa
        GROUP BY sa.tipo_problema, sa.urgencia, sa.estado
        ORDER BY 
            CASE sa.urgencia WHEN 'alta' THEN 1 WHEN 'media' THEN 2 WHEN 'baja' THEN 3 END,
            sa.estado
        """
        
        result = db.execute_query(query)
        
        if result:
            message = "Solicitudes de ayuda:\n\n"
            
            for row in result:
                message += f"• {row['tipo_problema'].upper()} | Urgencia: {row['urgencia']}\n"
                message += f"  Estado: {row['estado']} | Total: {row['total']}\n"
                if row['dias_promedio']:
                    message += f"  Tiempo promedio: {row['dias_promedio']:.1f} días\n"
                message += "\n"
            
            query_summary = """
            SELECT 
                SUM(CASE WHEN estado = 'pendiente' THEN 1 ELSE 0 END) as pendientes,
                SUM(CASE WHEN urgencia = 'alta' THEN 1 ELSE 0 END) as urgentes
            FROM solicitudes_ayuda
            """
            summary = db.execute_query(query_summary)
            
            if summary:
                data = summary[0]
                message += f"Resumen: {data['pendientes']} pendientes, {data['urgentes']} urgentes"
        else:
            message = "No se encontraron solicitudes de ayuda."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetForumPosts(Action):
    def name(self) -> Text:
        return "action_get_forum_posts"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        query = """
        SELECT fp.titulo, cf.nombre as categoria, fp.vistas, fp.likes,
               CONCAT(u.nombre, ' ', u.apellido) as autor,
               DATE_FORMAT(fp.fecha_creacion, '%d/%m/%Y') as fecha,
               fp.es_fijado, fp.es_cerrado
        FROM foro_posts fp
        JOIN categorias_foro cf ON fp.categoria_id = cf.id
        JOIN usuarios u ON fp.usuario_id = u.id
        WHERE fp.activo = TRUE
        ORDER BY fp.es_fijado DESC, fp.fecha_creacion DESC
        LIMIT 10
        """
        
        result = db.execute_query(query)
        
        if result:
            message = "Últimas publicaciones del foro:\n\n"
            
            for row in result:
                status = ""
                if row['es_fijado']:
                    status += " [FIJADO]"
                if row['es_cerrado']:
                    status += " [CERRADO]"
                
                message += f"• {row['titulo']}{status}\n"
                message += f"  Categoría: {row['categoria']} | Autor: {row['autor']}\n"
                message += f"  Fecha: {row['fecha']} | Vistas: {row['vistas']} | Likes: {row['likes']}\n\n"
            
            query_stats = """
            SELECT cf.nombre, COUNT(fp.id) as total_posts
            FROM categorias_foro cf
            LEFT JOIN foro_posts fp ON cf.id = fp.categoria_id AND fp.activo = TRUE
            WHERE cf.activo = TRUE
            GROUP BY cf.id, cf.nombre
            ORDER BY total_posts DESC
            """
            stats = db.execute_query(query_stats)
            
            if stats:
                message += "Posts por categoría:\n"
                for row in stats:
                    message += f"• {row['nombre']}: {row['total_posts']} posts\n"
        else:
            message = "No se encontraron publicaciones en el foro."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetNews(Action):
    def name(self) -> Text:
        return "action_get_news"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        query = """
        SELECT n.titulo, n.resumen, cn.nombre as categoria,
               CONCAT(u.nombre, ' ', u.apellido) as autor,
               DATE_FORMAT(n.fecha_publicacion, '%d/%m/%Y') as fecha,
               n.vistas, n.es_destacada
        FROM noticias n
        JOIN categorias_noticias cn ON n.categoria_id = cn.id
        JOIN directivos d ON n.autor_id = d.id
        JOIN usuarios u ON d.usuario_id = u.id
        WHERE n.publicada = TRUE
        ORDER BY n.es_destacada DESC, n.fecha_publicacion DESC
        LIMIT 10
        """
        
        result = db.execute_query(query)
        
        if result:
            message = "Últimas noticias:\n\n"
            
            for row in result:
                destacada = " [DESTACADA]" if row['es_destacada'] else ""
                
                message += f"• {row['titulo']}{destacada}\n"
                message += f"  {row['resumen']}\n"
                message += f"  Categoría: {row['categoria']} | Autor: {row['autor']}\n"
                message += f"  Fecha: {row['fecha']} | Vistas: {row['vistas']}\n\n"
        else:
            message = "No se encontraron noticias publicadas."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetSurveys(Action):
    def name(self) -> Text:
        return "action_get_surveys"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        query = """
        SELECT e.titulo, e.tipo_encuesta, 
               CONCAT(u.nombre, ' ', u.apellido) as creador,
               DATE_FORMAT(e.fecha_creacion, '%d/%m/%Y') as fecha,
               COUNT(DISTINCT pe.id) as total_preguntas,
               COUNT(DISTINCT re.alumno_id) as total_respuestas,
               e.activa
        FROM encuestas e
        JOIN profesores p ON e.creado_por = p.id
        JOIN usuarios u ON p.usuario_id = u.id
        LEFT JOIN preguntas_encuesta pe ON e.id = pe.encuesta_id
        LEFT JOIN respuestas_encuesta re ON e.id = re.encuesta_id
        GROUP BY e.id, e.titulo, e.tipo_encuesta, u.nombre, u.apellido, e.fecha_creacion, e.activa
        ORDER BY e.activa DESC, e.fecha_creacion DESC
        """
        
        result = db.execute_query(query)
        
        if result:
            message = "Encuestas disponibles:\n\n"
            
            activas = [row for row in result if row['activa']]
            inactivas = [row for row in result if not row['activa']]
            
            if activas:
                message += "ACTIVAS:\n"
                for row in activas:
                    message += f"• {row['titulo']}\n"
                    message += f"  Tipo: {row['tipo_encuesta']} | Creador: {row['creador']}\n"
                    message += f"  Preguntas: {row['total_preguntas']} | Respuestas: {row['total_respuestas']}\n"
                    message += f"  Fecha: {row['fecha']}\n\n"
            
            if inactivas:
                message += "INACTIVAS:\n"
                for row in inactivas:
                    message += f"• {row['titulo']} | Respuestas: {row['total_respuestas']}\n"
        else:
            message = "No se encontraron encuestas."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetStudentInfo(Action):
    def name(self) -> Text:
        return "action_get_student_info"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        matricula = tracker.get_slot("matricula")
        
        if not matricula:
            message = "Por favor proporciona la matrícula del estudiante."
        else:
            query = """
            SELECT u.nombre, u.apellido, u.correo, a.matricula, a.cuatrimestre_actual,
                   a.fecha_ingreso, a.telefono, a.estado_alumno, a.promedio_general,
                   c.nombre as carrera, a.tutor_nombre, a.tutor_telefono
            FROM alumnos a
            JOIN usuarios u ON a.usuario_id = u.id
            JOIN carreras c ON a.carrera_id = c.id
            WHERE a.matricula = %s
            """
            result = db.execute_query(query, (matricula,))
            
            if result:
                data = result[0]
                message = f"INFORMACIÓN DEL ESTUDIANTE\n\n"
                message += f"Nombre: {data['nombre']} {data['apellido']}\n"
                message += f"Matrícula: {data['matricula']}\n"
                message += f"Correo: {data['correo']}\n"
                message += f"Carrera: {data['carrera']}\n"
                message += f"Cuatrimestre actual: {data['cuatrimestre_actual']}\n"
                message += f"Fecha de ingreso: {data['fecha_ingreso']}\n"
                message += f"Estado: {data['estado_alumno']}\n"
                message += f"Promedio general: {data['promedio_general']}\n"
                
                if data['telefono']:
                    message += f"Teléfono: {data['telefono']}\n"
                
                if data['tutor_nombre']:
                    message += f"Tutor: {data['tutor_nombre']}\n"
                    if data['tutor_telefono']:
                        message += f"Teléfono del tutor: {data['tutor_telefono']}\n"
                
                query_risk = """
                SELECT COUNT(*) as reportes_riesgo
                FROM reportes_riesgo rr
                JOIN alumnos a ON rr.alumno_id = a.id
                WHERE a.matricula = %s AND rr.estado IN ('abierto', 'en_proceso')
                """
                risk = db.execute_query(query_risk, (matricula,))
                
                if risk and risk[0]['reportes_riesgo'] > 0:
                    message += f"Reportes de riesgo activos: {risk[0]['reportes_riesgo']}"
            else:
                message = f"No se encontró información para la matrícula '{matricula}'."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetTeacherInfo(Action):
    def name(self) -> Text:
        return "action_get_teacher_info"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        numero_empleado = tracker.get_slot("numero_empleado")
        
        if not numero_empleado:
            message = "Por favor proporciona el número de empleado del profesor."
        else:
            query = """
            SELECT u.nombre, u.apellido, u.correo, p.numero_empleado,
                   p.telefono, p.extension, p.fecha_contratacion,
                   p.titulo_academico, p.especialidad, p.cedula_profesional,
                   p.experiencia_años, p.es_tutor, c.nombre as carrera
            FROM profesores p
            JOIN usuarios u ON p.usuario_id = u.id
            JOIN carreras c ON p.carrera_id = c.id
            WHERE p.numero_empleado = %s AND p.activo = TRUE
            """
            result = db.execute_query(query, (numero_empleado,))
            
            if result:
                data = result[0]
                message = f"INFORMACIÓN DEL PROFESOR\n\n"
                message += f"Nombre: {data['nombre']} {data['apellido']}\n"
                message += f"Número de empleado: {data['numero_empleado']}\n"
                message += f"Correo: {data['correo']}\n"
                message += f"Carrera: {data['carrera']}\n"
                
                if data['titulo_academico']:
                    message += f"Título académico: {data['titulo_academico']}\n"
                
                if data['especialidad']:
                    message += f"Especialidad: {data['especialidad']}\n"
                
                if data['cedula_profesional']:
                    message += f"Cédula profesional: {data['cedula_profesional']}\n"
                
                message += f"Experiencia: {data['experiencia_años']} años\n"
                message += f"Fecha de contratación: {data['fecha_contratacion']}\n"
                
                if data['telefono']:
                    message += f"Teléfono: {data['telefono']}\n"
                
                if data['extension']:
                    message += f"Extensión: {data['extension']}\n"
                
                if data['es_tutor']:
                    message += "Es tutor de grupo\n"
                
                query_subjects = """
                SELECT COUNT(DISTINCT asignatura_id) as total_materias,
                       COUNT(DISTINCT grupo_id) as total_grupos
                FROM profesor_asignatura_grupo
                WHERE profesor_id = (SELECT id FROM profesores WHERE numero_empleado = %s)
                AND activo = TRUE
                """
                stats = db.execute_query(query_subjects, (numero_empleado,))
                
                if stats:
                    data_stats = stats[0]
                    message += f"Imparte {data_stats['total_materias']} materias en {data_stats['total_grupos']} grupos"
            else:
                message = f"No se encontró información para el empleado '{numero_empleado}'."
        
        dispatcher.utter_message(text=message)
        return []

class ActionGetCareerInfo(Action):
    def name(self) -> Text:
        return "action_get_career_info"

    def run(self, dispatcher: CollectingDispatcher,
            tracker: Tracker,
            domain: Dict[Text, Any]) -> List[Dict[Text, Any]]:
        
        carrera_nombre = tracker.get_slot("carrera_nombre")
        
        if not carrera_nombre:
            query = """
            SELECT nombre, codigo, descripcion, duracion_cuatrimestres,
                   COUNT(a.id) as total_alumnos,
                   COUNT(DISTINCT asig.id) as total_asignaturas
            FROM carreras c
            LEFT JOIN alumnos a ON c.id = a.carrera_id AND a.estado_alumno = 'activo'
            LEFT JOIN asignaturas asig ON c.id = asig.carrera_id AND asig.activa = TRUE
            WHERE c.activa = TRUE
            GROUP BY c.id, nombre, codigo, descripcion, duracion_cuatrimestres
            ORDER BY nombre
            """
            result = db.execute_query(query)
            
            if result:
                message = "Carreras disponibles:\n\n"
                for row in result:
                    message += f"• {row['nombre']} ({row['codigo']})\n"
                    message += f"  Duración: {row['duracion_cuatrimestres']} cuatrimestres\n"
                    message += f"  Alumnos activos: {row['total_alumnos']}\n"
                    message += f"  Asignaturas: {row['total_asignaturas']}\n"
                    if row['descripcion']:
                        message += f"  {row['descripcion'][:100]}...\n"
                    message += "\n"
            else:
                message = "No se encontraron carreras activas."
        else:
            query = """
            SELECT c.nombre, c.codigo, c.descripcion, c.duracion_cuatrimestres,
                   COUNT(DISTINCT a.id) as total_alumnos,
                   COUNT(DISTINCT asig.id) as total_asignaturas,
                   AVG(a.promedio_general) as promedio_carrera
            FROM carreras c
            LEFT JOIN alumnos a ON c.id = a.carrera_id AND a.estado_alumno = 'activo'
            LEFT JOIN asignaturas asig ON c.id = asig.carrera_id AND asig.activa = TRUE
            WHERE c.nombre LIKE %s AND c.activa = TRUE
            GROUP BY c.id, c.nombre, c.codigo, c.descripcion, c.duracion_cuatrimestres
            """
            result = db.execute_query(query, (f"%{carrera_nombre}%",))
            
            if result:
                data = result[0]
                message = f"INFORMACIÓN DE LA CARRERA\n\n"
                message += f"Nombre: {data['nombre']}\n"
                message += f"Código: {data['codigo']}\n"
                message += f"Duración: {data['duracion_cuatrimestres']} cuatrimestres\n"
                message += f"Alumnos activos: {data['total_alumnos']}\n"
                message += f"Total de asignaturas: {data['total_asignaturas']}\n"
                
                if data['promedio_carrera']:
                    message += f"Promedio general de la carrera: {data['promedio_carrera']:.2f}\n"
                
                if data['descripcion']:
                    message += f"\nDescripción:\n{data['descripcion']}"
                
                query_teachers = """
                SELECT COUNT(*) as total_profesores
                FROM profesores p
                JOIN carreras c ON p.carrera_id = c.id
                WHERE c.nombre LIKE %s AND p.activo = TRUE
                """
                teachers = db.execute_query(query_teachers, (f"%{carrera_nombre}%",))
                
                if teachers:
                    message += f"\nProfesores asignados: {teachers[0]['total_profesores']}"
            else:
                message = f"No se encontró información para la carrera '{carrera_nombre}'."
        
        dispatcher.utter_message(text=message)
        return []