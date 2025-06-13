import pandas as pd
# psycopg2 ya no se importa directamente, SQLAlchemy lo maneja por debajo.
from sqlalchemy import create_engine # Para crear el "motor" de conexión a la BD.
from datetime import datetime, timedelta # Para trabajar con fechas y calcular diferencias.

# --- Configuración Principal del Script ---
# Aquí ponemos los datos para que el script sepa a dónde conectarse y dónde guardar las cosas.
# Es como darle la dirección y las llaves de la base de datos.
DB_HOST = "psql-metrodoralakehouse-dev.postgres.database.azure.com"  # Dirección del servidor de la base de datos (ej: localhost o una dirección en la nube)
DB_NAME = "lakehouse"                                              # Nombre específico de la base de datos a la que nos conectaremos
DB_USER = "metrodora_reader_dev"                                   # Nombre de usuario para acceder a la base de datos
DB_PASSWORD = "ContraseñaSegura123*"                               # Contraseña para ese usuario (¡ojo! esto es un ejemplo, en producción se maneja de forma más segura)
OUTPUT_CSV_PATH = "./reparto_ingresos_output.csv"                  # Nombre y ruta del archivo CSV que vamos a generar con el resultado
DEBUG_CSV_PATH = "./reparto_ingresos_debug.csv"                    # Ruta para el fichero de depuración con los datos de entrada y fechas calculadas.
COURSE_SUMMARY_PATH = "./resumen_por_curso.csv"                    # Ruta para el resumen de ingresos totales por curso.

# Construimos la URL de conexión para SQLAlchemy.
# Es como una dirección web súper específica para la base de datos.
# "postgresql+psycopg2://usuario:contraseña@servidor/nombre_basedatos"
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

# --- Funciones del Script ---

def conectar_db():
    """Crea y prueba una conexión a la base de datos PostgreSQL usando SQLAlchemy.
    Devuelve: el objeto 'engine' de SQLAlchemy si la conexión es exitosa, None si falla.
    """
    engine = None  # Preparamos una variable para guardar nuestro motor, inicialmente vacía.
    try:
        # Intentamos crear el motor de SQLAlchemy con la URL que definimos antes.
        # El motor es el encargado de gestionar las conexiones y la comunicación con la BD.
        engine = create_engine(DB_URL)
        
        # Hacemos una prueba rápida para asegurar que la conexión funciona.
        # 'with ... as ...' se asegura de que la conexión se cierre sola al terminar.
        with engine.connect() as connection: # Intenta obtener una conexión del motor.
            print("Conexión a PostgreSQL (SQLAlchemy) exitosa.") # ¡Lo logramos!
    except Exception as error: # Si algo sale mal en el 'try'...
        # Capturamos cualquier error (Exception) y lo mostramos de forma amigable.
        print(f"Error al conectar a PostgreSQL (SQLAlchemy): {error}")
        engine = None # Si hay error, nos aseguramos de que engine no tenga un motor roto.
    return engine # Devolvemos el motor (o None si hubo un error).

def obtener_datos(engine):
    """Obtiene datos de las tablas 'fct_matricula' y 'dim_producto',
    y los une en una sola tabla (DataFrame de Pandas).
    
    Args:
        engine: El motor de SQLAlchemy para conectarse a la base de datos.
        
    Returns:
        Un DataFrame de Pandas con los datos combinados, o un DataFrame vacío si no hay datos.
    """
    print("Obteniendo datos de la base de datos...")
    
    # Definimos las preguntas (queries SQL) que le haremos a la base de datos.
    # Nota: los nombres de tablas y columnas están en minúsculas, común en PostgreSQL.
    query_matriculas = "SELECT cod_matricula, fec_matricula, importe_matricula, id_dim_producto FROM fct_matricula;"
    query_productos = "SELECT id_dim_producto, modalidad, fecha_inicio, fecha_fin, fecha_inicio_reconocimiento, fecha_fin_reconocimiento, meses_duracion FROM dim_producto;"
    
    try:
        # Usamos Pandas para ejecutar las queries a través del motor de SQLAlchemy.
        # Pandas convierte el resultado de cada query directamente en una tabla (DataFrame).
        df_matriculas = pd.read_sql_query(query_matriculas, engine)
        df_productos = pd.read_sql_query(query_productos, engine)
        
        # Unimos (merge) las dos tablas en una sola.
        # Es como hacer un VLOOKUP/BUSCARV de Excel pero más potente.
        # Se unen usando la columna 'id_dim_producto' que debe existir en ambas.
        # 'how="left"' significa: mantén todas las matrículas y añade la info de producto que coincida.
        # Si una matrícula no tiene producto_id o no coincide, sus columnas de producto quedarán vacías (NaN).
        df_completo = pd.merge(df_matriculas, df_productos, on="id_dim_producto", how="left")
        
        print(f"Se obtuvieron {len(df_completo)} registros combinados.")
        return df_completo
    except Exception as e:
        print(f"Error al obtener o combinar datos: {e}")
        return pd.DataFrame() # Devolvemos un DataFrame vacío si hay un error.

def procesar_reparto(df_datos):
    """Procesa cada matrícula para repartir su importe según la modalidad y fechas.
    
    Args:
        df_datos: DataFrame de Pandas con los datos de matrículas y productos combinados.
        
    Returns:
        Una tupla con dos DataFrames: 
        1. El reparto de ingresos detallado por día.
        2. Un DataFrame de depuración con los datos de entrada y fechas calculadas.
    """
    print("Procesando el reparto de ingresos...")
    
    lista_reparto = [] # Creamos una lista vacía para ir guardando cada "trozo" del reparto.
    lista_debug = []   # Lista para guardar los datos de depuración de cada matrícula.

    # Iteramos por cada fila del DataFrame. Cada fila es una matrícula con su info de producto.
    # 'iterrows()' nos da el índice de la fila y la fila misma (como un objeto).
    for index, row in df_datos.iterrows():
        # Extraemos los datos que necesitamos de la fila actual.
        # Es como decir: "de esta fila, dame el valor de la columna 'cod_matricula'".
        cod_matricula = row['cod_matricula']
        id_dim_producto = row['id_dim_producto']
        importe_matricula = row['importe_matricula']
        # Convertimos las fechas de texto a objetos 'datetime' de Python para poder operar con ellas.
        # 'errors="coerce"' hace que si una fecha no es válida, se convierta en 'NaT' (Not a Time) y no pare el script.
        fec_matricula = pd.to_datetime(row['fec_matricula'], errors='coerce')
        modalidad = row['modalidad']
        fecha_inicio_reconocimiento = pd.to_datetime(row['fecha_inicio_reconocimiento'], errors='coerce')
        fecha_fin_reconocimiento = pd.to_datetime(row['fecha_fin_reconocimiento'], errors='coerce')
        fecha_inicio_producto = pd.to_datetime(row['fecha_inicio'], errors='coerce')
        fecha_fin_producto = pd.to_datetime(row['fecha_fin'], errors='coerce')
        meses_duracion = row['meses_duracion'] # Esto podría ser un número o NaT si no está.

        # --- Lógica principal del reparto ---
        
        fecha_inicio_reparto = None # Variable para guardar la fecha de inicio del reparto.
        fecha_fin_reparto = None   # Variable para guardar la fecha de fin del reparto.
        origen_fechas = "N/A"      # Para el debug, saber de dónde salieron las fechas.

        if modalidad == 'ONLINE':
            # Si la modalidad es ONLINE, el importe completo se asigna a la fecha de la matrícula.
            if pd.notna(fec_matricula):
                fecha_inicio_reparto = fec_matricula
                fecha_fin_reparto = fec_matricula
                origen_fechas = "ONLINE: fec_matricula"
            else:
                origen_fechas = "ONLINE: sin fecha de matrícula"
                print(f"Advertencia: Matrícula ONLINE {cod_matricula} sin fecha de matrícula válida. No se puede repartir.")

        else: # Si la modalidad NO es ONLINE (presencial, semipresencial, etc.)
            # Aplicamos las reglas de prioridad para encontrar las fechas de reparto:
            # Regla 1: Usar fechas de reconocimiento si existen.
            # 'pd.notna()' comprueba si un valor no es nulo o NaT (Not a Time).
            if pd.notna(fecha_inicio_reconocimiento) and pd.notna(fecha_fin_reconocimiento):
                fecha_inicio_reparto = fecha_inicio_reconocimiento
                fecha_fin_reparto = fecha_fin_reconocimiento
                origen_fechas = "Reconocimiento"
            # Regla 2: Si no hay fechas de reconocimiento, usar fechas de inicio/fin del producto.
            elif pd.notna(fecha_inicio_producto) and pd.notna(fecha_fin_producto):
                fecha_inicio_reparto = fecha_inicio_producto
                fecha_fin_reparto = fecha_fin_producto
                origen_fechas = "Producto"
            # Regla 3: Si tampoco hay fechas de producto, usar fecha de matrícula + meses de duración.
            elif pd.notna(fec_matricula) and pd.notna(meses_duracion) and meses_duracion > 0:
                fecha_inicio_reparto = fec_matricula
                # timedelta permite sumar días, semanas, etc., a una fecha.
                # Hacemos una aproximación de 30 días por mes.
                fecha_fin_reparto = fec_matricula + timedelta(days=int(meses_duracion * 30))
                origen_fechas = "Matrícula + Meses"
            else:
                # Si ninguna regla anterior funciona, se usará el fallback más adelante.
                origen_fechas = "Fallback a fec_matricula"
        
        # Guardamos la información de depuración para esta matrícula ANTES de hacer el reparto.
        debug_info = {
            'cod_matricula': cod_matricula,
            'importe_matricula': importe_matricula,
            'modalidad': modalidad,
            'fec_matricula_original': row['fec_matricula'],
            'meses_duracion_original': row['meses_duracion'],
            'fecha_inicio_reconocimiento_original': row['fecha_inicio_reconocimiento'],
            'fecha_fin_reconocimiento_original': row['fecha_fin_reconocimiento'],
            'fecha_inicio_producto_original': row['fecha_inicio'],
            'fecha_fin_producto_original': row['fecha_fin'],
            'fecha_inicio_reparto_calculada': fecha_inicio_reparto.strftime('%Y-%m-%d') if pd.notna(fecha_inicio_reparto) else None,
            'fecha_fin_reparto_calculada': fecha_fin_reparto.strftime('%Y-%m-%d') if pd.notna(fecha_fin_reparto) else None,
            'origen_fechas_calculadas': origen_fechas
        }
        lista_debug.append(debug_info)

        # Realizamos el reparto con la lógica corregida.
        if fecha_inicio_reparto and fecha_fin_reparto and fecha_inicio_reparto <= fecha_fin_reparto:
            numero_dias = (fecha_fin_reparto - fecha_inicio_reparto).days + 1
            
            if numero_dias == 1: # Si el reparto es en un solo día
                lista_reparto.append({
                    'FECHA': fecha_inicio_reparto.strftime('%Y-%m-%d'),
                    'COD_MATRICULA': cod_matricula,
                    'ID_DIM_PRODUCTO': id_dim_producto,
                    'IMPORTE': importe_matricula
                })
            elif numero_dias > 1: # Si el reparto es en múltiples días
                importe_diario = round(importe_matricula / numero_dias, 2)
                repartido_hasta_ahora = 0
                
                # Repartimos para los primeros N-1 días
                for i in range(numero_dias - 1):
                    fecha_actual = fecha_inicio_reparto + timedelta(days=i)
                    lista_reparto.append({
                        'FECHA': fecha_actual.strftime('%Y-%m-%d'),
                        'COD_MATRICULA': cod_matricula,
                        'ID_DIM_PRODUCTO': id_dim_producto,
                        'IMPORTE': importe_diario
                    })
                    repartido_hasta_ahora += importe_diario
                
                # El importe del último día es el total menos lo ya repartido
                importe_ultimo_dia = round(importe_matricula - repartido_hasta_ahora, 2)
                fecha_ultimo_dia = fecha_inicio_reparto + timedelta(days=numero_dias - 1)
                lista_reparto.append({
                    'FECHA': fecha_ultimo_dia.strftime('%Y-%m-%d'),
                    'COD_MATRICULA': cod_matricula,
                    'ID_DIM_PRODUCTO': id_dim_producto,
                    'IMPORTE': importe_ultimo_dia
                })
        else: # Fallback: Si no se encontraron fechas válidas o son inconsistentes.
            if pd.notna(fec_matricula):
                # Si ninguna regla funcionó o las fechas son inválidas, usamos la fecha de matrícula como último recurso.
                print(f"Advertencia: No se pudieron determinar fechas válidas para {cod_matricula}. Usando fec_matricula.")
                lista_reparto.append({
                    'FECHA': fec_matricula.strftime('%Y-%m-%d'),
                    'COD_MATRICULA': cod_matricula,
                    'ID_DIM_PRODUCTO': id_dim_producto,
                    'IMPORTE': importe_matricula
                })
            else: # El caso más extremo: no hay fechas ni fecha de matrícula.
                print(f"Advertencia: {cod_matricula} sin fechas de reparto válidas NI fec_matricula. No se puede repartir.")

    # Cuando hemos recorrido todas las matrículas, convertimos las listas en DataFrames.
    df_reparto_final = pd.DataFrame(lista_reparto)
    df_debug_final = pd.DataFrame(lista_debug)
    
    return df_reparto_final, df_debug_final # Devolvemos AMBAS tablas.

def guardar_csv(df_resultado, path):
    """Guarda el DataFrame resultado en un archivo CSV.
    
    Args:
        df_resultado: El DataFrame de Pandas que queremos guardar.
        path: La ruta (nombre y carpeta) donde se guardará el archivo CSV.
    """
    try:
        # 'to_csv' es la función de Pandas para guardar como CSV.
        # 'index=False' evita que se guarde una columna extra con los números de fila de Pandas.
        # 'encoding='utf-8-sig'' ayuda a que Excel abra bien los caracteres especiales (tildes, ñ, etc.).
        df_resultado.to_csv(path, index=False, encoding='utf-8-sig')
        print(f"Archivo CSV guardado exitosamente en: {path}")
    except Exception as e: # Si hay algún error al intentar guardar...
        print(f"Error al guardar el archivo CSV: {e}")

# --- Función Principal (Orquestador) ---
def main():
    """Función principal que coordina la ejecución de todo el script."""
    print("--- Iniciando script de reparto de ingresos ---")
    engine = conectar_db() # Paso 1: Intentar conectar a la base de datos.
    
    if engine: # Si la conexión fue exitosa...
        print("\n--- Conexión establecida, obteniendo datos... ---")
        df_datos_combinados = obtener_datos(engine) # Paso 2: Obtener y combinar los datos.
        
        # Nos aseguramos de que el importe sea numérico para la suma de verificación
        if not df_datos_combinados.empty:
            df_datos_combinados['importe_matricula'] = pd.to_numeric(df_datos_combinados['importe_matricula'], errors='coerce').fillna(0)
            total_original = df_datos_combinados['importe_matricula'].sum()
            print(f"Suma total del importe original (verificación): {total_original:,.2f} €")

            print("\n--- Datos obtenidos, procesando reparto... ---")
            df_reparto_calculado, df_debug = procesar_reparto(df_datos_combinados) # Paso 3: Calcular el reparto, ahora devuelve dos DFs.
            
            if not df_reparto_calculado.empty: # Si el cálculo del reparto generó resultados...
                print("\n--- Reparto procesado, guardando archivos... ---")
                guardar_csv(df_reparto_calculado, OUTPUT_CSV_PATH) # Paso 4a: Guardar el resultado.
                guardar_csv(df_debug, DEBUG_CSV_PATH)              # Paso 4b: Guardar el fichero de depuración.

                # --- Generar y guardar resumen por matrícula y curso ---
                print("--- Generando resumen por matrícula y curso... ---")
                df_resumen_matricula_curso = df_reparto_calculado.groupby(['ID_DIM_PRODUCTO', 'COD_MATRICULA']).agg(
                    IMPORTE_REPARTIDO=('IMPORTE', 'sum')
                ).reset_index()
                guardar_csv(df_resumen_matricula_curso, './resumen_por_matricula_y_curso.csv')

                # Calculamos y mostramos la suma total del dinero repartido.
                total_repartido = df_reparto_calculado['IMPORTE'].sum()
                print(f"\n--- Verificación Final ---")
                print(f"Suma total del dinero repartido en el CSV: {total_repartido:,.2f} €")
                
                # Comprobamos si el total repartido coincide con el original
                if abs(total_original - total_repartido) < 0.01: # Usamos una pequeña tolerancia para errores de redondeo
                    print("VERIFICACIÓN OK: El total original coincide con el total repartido.")
                else:
                    diferencia = total_repartido - total_original
                    print(f"ATENCIÓN: Discrepancia de {diferencia:,.2f} € entre el total original y el repartido.")

            else:
                print("El procesamiento del reparto no generó resultados para guardar.")
        else:
            print("No se obtuvieron datos de la base de datos para procesar.")
        
        # Es importante cerrar/liberar los recursos del motor de la base de datos al final.
        engine.dispose()
        print("\n--- Conexión a PostgreSQL (SQLAlchemy) cerrada. Script finalizado. ---")
    else:
        print("No se pudo establecer conexión con la base de datos. Script finalizado con errores.")

# Esta es la forma estándar en Python de decir: "Si ejecuto este archivo .py directamente,
# entonces llama a la función main() para que empiece todo".
# Si este archivo fuera importado por otro script, esto no se ejecutaría automáticamente.
if __name__ == "__main__":
    main() 