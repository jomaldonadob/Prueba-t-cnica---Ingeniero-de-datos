# Prueba tecnica Ingeniero de datos
Prueba técnica - Ingeniero de datos

Este proyecto implementa un proceso ETL utilizando PySpark para cargar, transformar y almacenar datos desde un archivo Excel.
Disenado para ejecutarse en una terminal que permita interfaz grafica para visualizar las graficas.

## Requisitos

- Python 3.8+
- PySpark
- Pandas
- openpyxl (para leer archivos Excel)
- numpy
- matplotlib
- seaborn

## Instalación

1. Clonar el repositorio: git clone <repo_url>

## Implementación y Justificación del Diseño
### Entradas y salidas

1. **Entradas**
   - Archivo Films
      El archivo Films_2.xlsx es el archivo de entrada que contiene la información inicial que se procesará. Este archivo contiene varias hojas con datos relacionados con películas, inventarios y clientes. 
2. **Salidas**
   - Archivos Parquet
      - El objetivo es generar archivos Parquet debido a su alta eficiencia en términos de almacenamiento y procesamiento, especialmente cuando se trabaja con grandes volúmenes de datos. Este formato es ampliamente utilizado en ecosistemas de Big Data como Spark y Hadoop.
   - Graficos
      - Se generan gráficos informativos con la información clave extraída de los datos, como la distribución de alquileres por mes o las categorías de películas más populares. Esto proporciona un valor añadido al proyecto, permitiendo visualizar los insights de forma clara y comprensible.
### Implementación

Este proyecto sigue un enfoque modular para implementar el proceso ETL (Extracción, Transformación y Carga) utilizando PySpark, una herramienta poderosa para procesar grandes volúmenes de datos. A continuación, se detalla cada una de las etapas del proceso:

1. **Extracción de Datos**:
   - Descripción: El proceso de extracción lee los datos desde un archivo Excel, que es el archivo de entrada. Las hojas de este archivo se cargan como DataFrames de pandas y luego se convierten a DataFrames de Spark para permitir el procesamiento distribuido.
   - Tecnología utilizada: pandas para la lectura del archivo Excel y pyspark.sql para manejar los datos de manera distribuida.
   - Función principal: extract_data (ubicada en etl/extract.py#L6).

2. **Transformación de Datos**:
   - Descripción: Una vez extraídos los datos, el siguiente paso es transformarlos. Esto incluye la renombración de columnas, la normalización de valores, la aplicación de filtros y la realización de uniones entre distintos conjuntos de datos.
   - Tecnología utilizada: PySpark para aplicar transformaciones sobre los DataFrames de Spark.
   - Función principal: transform_data (ubicada en etl/transform.py#L6).

3. **Carga de Datos**:
   - Descripción: Esta etapa carga los datos transformados en el formato Parquet. El uso de Parquet como formato de salida es clave para el procesamiento eficiente de grandes volúmenes de datos.
   - Tecnología utilizada: PySpark para escribir los datos en el formato Parquet.
   - Función principal: load_data.

4. **Limpieza de Datos**:
   - Descripción: La limpieza de datos es una parte esencial del proceso ETL. En esta etapa, se eliminan o corrigen los datos erróneos, duplicados o incompletos.
   - Tecnología utilizada: PySpark y pandas para la limpieza.
   - Funciones principales: clean_film_data, clean_inventory_data, clean_customer_data (ubicadas en etl/data_cleaner.py).

5. **Consultas de Negocio**:
   - Descripción: El módulo de consultas de negocio permite ejecutar consultas analíticas sobre los datos transformados. Estas consultas pueden responder preguntas clave relacionadas con los alquileres de películas, la distribución de categorías y otros aspectos importantes del negocio.
   - Tecnología utilizada: PySpark SQL para ejecutar consultas SQL sobre los DataFrames de Spark.
   - Funciones principales: query_peliculas_alquiladas_por_mes, query_categorias_mas_alquiladas (ubicadas en queries/business_queries.py).

### Justificación del Diseño

1. **Modularidad**:
   - El diseño modular permite que el proyecto sea fácilmente mantenido, extendido y escalado. Cada módulo (extracción, transformación, carga, limpieza y consultas) se enfoca en una única responsabilidad, lo que facilita la depuración y mejora la comprensión del código.
   - Si en el futuro se requiere procesar más datos o agregar nuevas funcionalidades, se pueden agregar nuevos módulos o actualizar los existentes sin afectar el resto del flujo ETL.

2. **Uso de PySpark**:
   - El uso de PySpark permite aprovechar las capacidades de procesamiento distribuido de Spark, lo que hace que el procesamiento de grandes volúmenes de datos sea mucho más eficiente. 

3. **Limpieza de Datos**:
   - La limpieza de datos es una etapa crítica en cualquier proceso ETL. Al realizar la limpieza antes de la transformación, aseguramos que los datos sean consistentes y correctos, lo que reduce el riesgo de errores y mejora la calidad de los resultados finales.

4. **Consultas de Negocio**:
   - Las consultas de negocio proporcionan insights clave que pueden ser utilizados para tomar decisiones informadas. Estas consultas pueden ser fácilmente adaptadas y extendidas para responder preguntas adicionales según sea necesario.

5. **Registro de Logs**:
   - El sistema de logging permite monitorizar el proceso ETL y detectar posibles errores o problemas en tiempo real. Esto es crucial para garantizar la transparencia y la facilidad de depuración del sistema.

Este diseño asegura que el proceso ETL sea robusto, escalable y fácil de mantener, cumpliendo con los requisitos de procesamiento eficiente de datos y generación de informes y gráficos con la información clave.

### Funciones Clave

A continuación se describen algunas de las funciones clave utilizadas en este proyecto:

#### Extracción de Datos

- **`extract_data(file_path: str) -> DataFrame`**:
    - Esta función se encarga de leer los datos desde un archivo Excel utilizando `pandas` y convertirlos en un DataFrame de Spark.
    - **Parámetros**:
        - `file_path` (str): La ruta del archivo Excel a leer.
    - **Retorno**:
        - `DataFrame`: Un DataFrame de Spark con los datos extraídos.

#### Transformación de Datos

- **`transform_data(df: DataFrame) -> DataFrame`**:
    - Esta función realiza varias transformaciones en el DataFrame de Spark, como renombrar columnas, realizar uniones y aplicar funciones de limpieza específicas.
    - **Parámetros**:
        - `df` (DataFrame): El DataFrame de Spark a transformar.
    - **Retorno**:
        - `DataFrame`: Un DataFrame de Spark con los datos transformados.

#### Carga de Datos

- **`load_data(df: DataFrame, output_path: str)`**:
    - Esta función guarda el DataFrame de Spark transformado en archivos Parquet en el directorio de salida especificado.
    - **Parámetros**:
        - `df` (DataFrame): El DataFrame de Spark a guardar.
        - `output_path` (str): La ruta del directorio donde se guardarán los archivos Parquet.

#### Limpieza de Datos

- **`clean_film_data(df: DataFrame) -> DataFrame`**:
    - Esta función limpia los datos específicos del DataFrame de películas, asegurando la calidad de los datos antes de la transformación.
    - **Parámetros**:
        - `df` (DataFrame): El DataFrame de Spark con los datos de películas a limpiar.
    - **Retorno**:
        - `DataFrame`: Un DataFrame de Spark con los datos de películas limpios.

- **`clean_inventory_data(df: DataFrame) -> DataFrame`**:
    - Esta función limpia los datos específicos del DataFrame de inventario.
    - **Parámetros**:
        - `df` (DataFrame): El DataFrame de Spark con los datos de inventario a limpiar.
    - **Retorno**:
        - `DataFrame`: Un DataFrame de Spark con los datos de inventario limpios.

- **`clean_customer_data(df: DataFrame) -> DataFrame`**:
    - Esta función limpia los datos específicos del DataFrame de clientes.
    - **Parámetros**:
        - `df` (DataFrame): El DataFrame de Spark con los datos de clientes a limpiar.
    - **Retorno**:
        - `DataFrame`: Un DataFrame de Spark con los datos de clientes limpios.

#### Consultas de Negocio

- **`query_peliculas_alquiladas_por_mes(df: DataFrame) -> DataFrame`**:
    - Esta función realiza una consulta para obtener el número de películas alquiladas por mes.
    - **Parámetros**:
        - `df` (DataFrame): El DataFrame de Spark con los datos de alquileres.
    - **Retorno**:
        - `DataFrame`: Un DataFrame de Spark con el número de películas alquiladas por mes.

- **`query_categorias_mas_alquiladas(df: DataFrame) -> DataFrame`**:
    - Esta función realiza una consulta para obtener las categorías de películas más alquiladas.
    - **Parámetros**:
        - `df` (DataFrame): El DataFrame de Spark con los datos de alquileres.
    - **Retorno**:
        - `DataFrame`: Un DataFrame de Spark con las categorías de películas más alquiladas.
