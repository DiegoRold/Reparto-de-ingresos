# Documentación Técnica: Script de Reparto de Ingresos

## 1. Tecnologías Usadas y Versiones

Este script está desarrollado en Python y utiliza las siguientes librerías principales. Las versiones corresponden al entorno de ejecución en el momento de crear esta documentación.

- **pandas**: `2.2.3` - Utilizada para la manipulación y análisis de datos, especialmente para la gestión de DataFrames.
- **SQLAlchemy**: `2.0.41` - Usada como ORM para la conexión y comunicación con la base de datos PostgreSQL. Abstrae la lógica de conexión.
- **psycopg2-binary**: `2.9.10` - Driver de PostgreSQL para Python que SQLAlchemy utiliza internamente para conectarse a la base de datos.
- **numpy**: `2.2.6` - Dependencia de pandas, utilizada para cálculos numéricos eficientes.
- **python-dateutil**: `2.9.0.post0` - Dependencia de pandas, proporciona extensiones potentes para el manejo de fechas y horas.

## 2. Funcionalidad

El propósito principal de la aplicación es procesar los ingresos por matrículas de cursos y repartirlos a lo largo del tiempo según un conjunto de reglas de negocio.

El flujo de trabajo es el siguiente:
1.  **Conexión a la Base de Datos**: El script se conecta a una base de datos PostgreSQL para obtener los datos necesarios.
2.  **Extracción de Datos**: Se extraen datos de las tablas `fct_matricula` (información de matrículas) y `dim_producto` (detalles de los cursos).
3.  **Procesamiento y Reparto**: Para cada matrícula, el script determina un rango de fechas de reparto basándose en una jerarquía de reglas:
    - **Modalidad ONLINE**: El importe total se asigna a la fecha de la matrícula.
    - **Otras Modalidades**: Se sigue una prioridad para encontrar las fechas de inicio y fin:
        1.  Fechas de reconocimiento del curso.
        2.  Fechas de inicio y fin del producto.
        3.  Fecha de matrícula más la duración en meses del curso (aproximación).
4.  **Generación de Archivos**: El script genera tres archivos CSV como salida:
    - `reparto_ingresos_output.csv`: Contiene el reparto detallado de ingresos por día para cada matrícula.
    - `reparto_ingresos_debug.csv`: Un archivo de depuración que muestra los datos de entrada y las fechas calculadas para cada matrícula, facilitando la validación de la lógica.
    - `resumen_por_matricula_y_curso.csv`: Un resumen que agrupa por curso y matrícula, mostrando el importe total repartido para cada una.

## 3. Estructura de Ficheros

La aplicación se compone de un único script principal y los archivos de salida que genera.

-   `reparto_ingresos.py`: El script principal que contiene toda la lógica de conexión, extracción, procesamiento y guardado de datos.
-   `requirements.txt`: (Opcional, pero recomendado) Archivo que define las dependencias de Python para asegurar la reproducibilidad del entorno.
-   `reparto_ingresos_output.csv`: (Salida) Fichero con el reparto diario de ingresos.
-   `reparto_ingresos_debug.csv`: (Salida) Fichero de depuración.
-   `resumen_por_matricula_y_curso.csv`: (Salida) Fichero de resumen.

## 4. Cosas a Tener en Cuenta

-   **Credenciales de la Base de Datos**: Las credenciales de acceso a la base de datos (`DB_HOST`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`) están definidas como constantes al inicio del script. Para un entorno de producción, se recomienda gestionarlas de forma segura (ej. variables de entorno, gestores de secretos).
-   **Manejo de Fechas Inválidas**: Si para una matrícula no se puede determinar un rango de fechas válido (ej. fechas inconsistentes, nulas, o una fecha de inicio posterior a la de fin), el script emite una advertencia (`Advertencia: No se pudieron determinar fechas válidas...`) y, como fallback, reparte el importe íntegro en la `fec_matricula`. Esto asegura que la suma total de importes repartidos siempre coincida con la suma original, pero es importante revisar estas advertencias para detectar posibles errores en los datos de origen.
-   **Codificación de CSV**: Los archivos CSV se guardan con codificación `utf-8-sig` para garantizar la compatibilidad y correcta visualización de caracteres especiales (como tildes o 'ñ') en programas como Microsoft Excel. 