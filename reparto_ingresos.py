import pandas as pd
# import psycopg2 # Ya no es necesario importar psycopg2 directamente aquí
from sqlalchemy import create_engine # Importar create_engine
from datetime import datetime, timedelta

# Constantes (ejemplos, ajustar según sea necesario)
DB_HOST = "psql-metrodoralakehouse-dev.postgres.database.azure.com"
DB_NAME = "lakehouse"
DB_USER = "metrodora_reader_dev"
DB_PASSWORD = "ContraseñaSegura123*"
OUTPUT_CSV_PATH = "./reparto_ingresos_output.csv"

# Cadena de conexión para SQLAlchemy (PostgreSQL)
DB_URL = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}/{DB_NAME}"

def conectar_db():
    """Establece conexión con la base de datos PostgreSQL usando SQLAlchemy."""
    engine = None
    try:
        engine = create_engine(DB_URL)
        # Opcional: probar la conexión inmediatamente
        with engine.connect() as connection:
            print("Conexión a PostgreSQL (SQLAlchemy) exitosa.")
    except Exception as error:
        print(f"Error al conectar a PostgreSQL (SQLAlchemy): {error}")
    return engine

def obtener_datos(engine): # Ahora recibe un engine de SQLAlchemy
    """Obtiene los datos de las tablas FCT_MATRICULA y DIM_PRODUCTO."""
    print("Obteniendo datos...")
    
    query_matriculas = "SELECT cod_matricula, fec_matricula, importe_matricula, id_dim_producto FROM fct_matricula;"
    query_productos = "SELECT id_dim_producto, modalidad, fecha_inicio, fecha_fin, fecha_inicio_reconocimiento, fecha_fin_reconocimiento, meses_duracion FROM dim_producto;"
    
    # pandas.read_sql_query funciona directamente con el engine de SQLAlchemy
    df_matriculas = pd.read_sql_query(query_matriculas, engine)
    df_productos = pd.read_sql_query(query_productos, engine)
    
    df_completo = pd.merge(df_matriculas, df_productos, on="id_dim_producto", how="left")
    
    print(f"Se obtuvieron {len(df_completo)} registros.")
    return df_completo

def procesar_reparto(df_datos):
    """Procesa los datos y calcula el reparto de ingresos."""
    # Placeholder: Esta función se implementará para procesar el reparto
    print("Procesando reparto de ingresos...")
    
    lista_reparto = []

    for index, row in df_datos.iterrows():
        cod_matricula = row['cod_matricula']
        importe_matricula = row['importe_matricula']
        fec_matricula = pd.to_datetime(row['fec_matricula'])
        
        modalidad = row['modalidad']
        fecha_inicio_reconocimiento = pd.to_datetime(row['fecha_inicio_reconocimiento'], errors='coerce')
        fecha_fin_reconocimiento = pd.to_datetime(row['fecha_fin_reconocimiento'], errors='coerce')
        fecha_inicio_producto = pd.to_datetime(row['fecha_inicio'], errors='coerce')
        fecha_fin_producto = pd.to_datetime(row['fecha_fin'], errors='coerce')
        meses_duracion = row['meses_duracion']

        if modalidad == 'ONLINE':
            lista_reparto.append({
                'FECHA': fec_matricula.strftime('%Y-%m-%d'),
                'COD_MATRICULA': cod_matricula,
                'IMPORTE': importe_matricula
            })
        else:
            fecha_inicio_reparto = None
            fecha_fin_reparto = None

            # Prioridad para fechas de reparto
            if pd.notna(fecha_inicio_reconocimiento) and pd.notna(fecha_fin_reconocimiento):
                fecha_inicio_reparto = fecha_inicio_reconocimiento
                fecha_fin_reparto = fecha_fin_reconocimiento
            elif pd.notna(fecha_inicio_producto) and pd.notna(fecha_fin_producto):
                fecha_inicio_reparto = fecha_inicio_producto
                fecha_fin_reparto = fecha_fin_producto
            elif pd.notna(fec_matricula) and pd.notna(meses_duracion):
                fecha_inicio_reparto = fec_matricula
                # Aproximación de meses a días (30 días por mes)
                fecha_fin_reparto = fec_matricula + timedelta(days=int(meses_duracion * 30)) 
            
            if fecha_inicio_reparto and fecha_fin_reparto and fecha_inicio_reparto <= fecha_fin_reparto:
                numero_dias = (fecha_fin_reparto - fecha_inicio_reparto).days + 1
                importe_diario = importe_matricula / numero_dias
                
                for i in range(numero_dias):
                    fecha_actual = fecha_inicio_reparto + timedelta(days=i)
                    lista_reparto.append({
                        'FECHA': fecha_actual.strftime('%Y-%m-%d'),
                        'COD_MATRICULA': cod_matricula,
                        'IMPORTE': importe_diario
                    })
            else:
                # Si no se pueden determinar las fechas, o son inválidas, se asigna a la fecha de matrícula (fallback)
                print(f"Advertencia: No se pudieron determinar fechas válidas para {cod_matricula}. Usando fec_matricula.")
                lista_reparto.append({
                    'FECHA': fec_matricula.strftime('%Y-%m-%d'),
                    'COD_MATRICULA': cod_matricula,
                    'IMPORTE': importe_matricula
                })

    df_reparto_final = pd.DataFrame(lista_reparto)
    if not df_reparto_final.empty:
        df_reparto_final['IMPORTE'] = df_reparto_final['IMPORTE'].round(2) # Redondear a 2 decimales

    return df_reparto_final

def guardar_csv(df_resultado, path):
    """Guarda el dataframe resultado en un archivo CSV."""
    try:
        df_resultado.to_csv(path, index=False, encoding='utf-8-sig') # utf-8-sig para mejor compatibilidad con Excel
        print(f"Archivo CSV guardado exitosamente en: {path}")
    except Exception as e:
        print(f"Error al guardar el archivo CSV: {e}")

def main():
    """Función principal del script."""
    engine = conectar_db() # Renombrado conn a engine para claridad
    if engine:
        df_datos = obtener_datos(engine)
        if not df_datos.empty:
            df_reparto = procesar_reparto(df_datos)
            if not df_reparto.empty:
                guardar_csv(df_reparto, OUTPUT_CSV_PATH)
            else:
                print("No se generaron datos de reparto.")
        else:
            print("No se obtuvieron datos para procesar.")
        
        engine.dispose() # Usar dispose() para el engine de SQLAlchemy
        print("Conexión a PostgreSQL (SQLAlchemy) cerrada.")

if __name__ == "__main__":
    main() 