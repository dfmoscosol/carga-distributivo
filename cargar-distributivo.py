import pandas as pd
import firebase_admin
from firebase_admin import credentials, auth
from sqlalchemy import create_engine, text
from fuzzywuzzy import process
import logging

# Inicializar la aplicación de Firebase
cred = credentials.Certificate("pentagono-ucuenca-firebase-adminsdk-5v85v-47168a1707.json")
firebase_admin.initialize_app(cred)

# Configurar el logging
logging.basicConfig(filename='facultades_no_encontradas.log', level=logging.INFO)

# Cargar el archivo Excel
df = pd.read_excel('docentes.xlsx', dtype={'identificacion': str})

# Conectar a la base de datos
try:
    engine = create_engine('postgresql://postgres:000111@10.0.2.77:5432/uc_services')
    connection = engine.connect()
    print("Conexión exitosa a la base de datos")
except Exception as e:
    print(f"Error al conectar a la base de datos: {e}")
    raise SystemExit(e)

# Iniciar una transacción
with engine.begin() as connection:
    # Obtener nombres de facultades desde la base de datos
    result = connection.execute(text("SELECT id_facultad, nombre FROM facultad"))
    facultades = {row[1]: row[0] for row in result}
    facultad_nombres = list(facultades.keys())

    # Verificar y crear el periodo lectivo si no existe
    periodo = df.iloc[0]['id_periodo']
    nombre_periodo = df.iloc[0]['nombre_periodo']
    fecha_inicio = df.iloc[0]['fecha_inicio']
    fecha_fin = df.iloc[0]['fecha_fin']

    periodo_existente = connection.execute(text(f"SELECT * FROM periodos_lectivos WHERE id = {periodo}")).fetchone()
    if periodo_existente is None:
        connection.execute(text(f"""
            INSERT INTO periodos_lectivos (id, nombre, fecha_inicio, fecha_fin)
            VALUES ({periodo}, '{nombre_periodo}', '{fecha_inicio}', '{fecha_fin}')
        """))
        print(f"Periodo lectivo {nombre_periodo} insertado en la base de datos")

    # Verificar la existencia de la cuenta del docente, crear en Firebase y completar datos
    for index, row in df.iterrows():
        correo = row['email_institucional']
        cedula = row['identificacion']
        nombres = row['nombres']
        apellidos = row['apellidos']

        docente = connection.execute(text(f"SELECT * FROM docente WHERE correo = '{correo}'")).fetchone()

        if docente is None:
            try:
                user = auth.create_user(
                    email=correo,
                    email_verified=True,
                    password=cedula
                )
                print(f"Usuario creado: {user.uid}")
                connection.execute(text(f"""
                    INSERT INTO docente (uid_firebase, nombres, correo, id_universidad_fk, cedula)
                    VALUES ('{user.uid}', '{nombres} {apellidos}', '{correo}', 1, '{cedula}')
                """))
                print(f"Docente {nombres} {apellidos} insertado en la base de datos")
            except auth.EmailAlreadyExistsError:
                existing_user = auth.get_user_by_email(correo)
                print(f"El usuario ya existe: {existing_user.uid}")
                connection.execute(text(f"""
                    INSERT INTO docente (uid_firebase, nombres, correo, id_universidad_fk, cedula)
                    VALUES ('{existing_user.uid}', '{nombres} {apellidos}', '{correo}', 1, '{cedula}')
                """))
                print(f"Docente {nombres} {apellidos} insertado en la base de datos")
        else:
            if docente[4] is None:  # Acceder a la columna 'cedula' por índice
                connection.execute(text(f"""
                    UPDATE docente SET cedula = '{cedula}'
                    WHERE correo = '{correo}'
                """))
                print(f"Docente {nombres} {apellidos} actualizado con cédula en la base de datos")

    # Llenar la tabla distributivo_docente utilizando fuzzy matching para los nombres de las facultades
    for index, row in df.iterrows():
        correo = row['email_institucional']
        docente = connection.execute(text(f"SELECT uid_firebase FROM docente WHERE correo = '{correo}'")).fetchone()

        if docente is not None:
            facultad_nombre = row['nombre_dependencia']
            facultad_id = None

            mejor_coincidencia = process.extractOne(facultad_nombre, facultad_nombres)
            if mejor_coincidencia[1] >= 95:
                facultad_id = facultades[mejor_coincidencia[0]]

            if facultad_id is not None:
                # Verificar si el registro ya existe en distributivo_docente
                distributivo_existente = connection.execute(text(f"""
                    SELECT * FROM distributivo_docente 
                    WHERE docente_uid_firebase = '{docente[0]}' 
                      AND facultad_id = {facultad_id} 
                      AND periodo_lectivo_id = {periodo}
                """)).fetchone()

                if distributivo_existente is None:
                    connection.execute(text(f"""
                        INSERT INTO distributivo_docente (docente_uid_firebase, facultad_id, periodo_lectivo_id)
                        VALUES ('{docente[0]}', {facultad_id}, {periodo})
                    """))
                    print(f"Distribuitivo docente para {correo} insertado en la base de datos")
                else:
                    print(f"Registro ya existe en distributivo_docente para {correo}")
            else:
                mensaje = f"Facultad no encontrada para el nombre: {facultad_nombre}"
                print(mensaje)
                logging.info(mensaje)
        else:
            mensaje = f"Docente con correo {correo} no encontrado en la base de datos"
            print(mensaje)
            logging.info(mensaje)

# Cerrar la conexión
connection.close()
