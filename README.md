# Prueba tecnica Ingeniero de datos
Prueba técnica - Ingeniero de datos

Este proyecto implementa un proceso ETL utilizando PySpark para cargar, transformar y almacenar datos desde un archivo Excel.

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

### Implementación

Este proyecto sigue un enfoque modular para implementar un proceso ETL (Extracción, Transformación y Carga) utilizando PySpark. A continuación, se detalla cada una de las etapas del proceso:

1. **Extracción de Datos**:
    - El módulo [`etl.extract`](etl/extract.py) se encarga de leer los datos desde un archivo Excel utilizando `pandas`. Las hojas del archivo Excel se cargan en DataFrames de pandas y luego se convierten en DataFrames de Spark para su posterior procesamiento.
    - Función principal: [`extract_data`](etl/extract.py#L6)

2. **Transformación de Datos**:
    - El módulo [`etl.transform`](etl/transform.py) realiza varias transformaciones en los DataFrames. Esto incluye renombrar columnas, realizar uniones y aplicar funciones de limpieza específicas.
    - Función principal: [`transform_data`](etl/transform.py#L6)

3. **Carga de Datos**:
    - El módulo [`etl.load`](etl/load.py) guarda los DataFrames transformados en archivos Parquet en el directorio de salida especificado.
    - Función principal: `load_data`

4. **Limpieza de Datos**:
    - El módulo [`etl.data_cleaner`](etl/data_cleaner.py) contiene funciones para limpiar datos específicos de cada DataFrame, asegurando la calidad de los datos antes de la transformación.
    - Funciones principales: [`clean_film_data`](etl/data_cleaner.py), [`clean_inventory_data`](etl/data_cleaner.py), [`clean_customer_data`](etl/data_cleaner.py)

5. **Consultas de Negocio**:
    - El módulo [`queries.business_queries`](queries/business_queries.py) contiene funciones para realizar consultas específicas sobre los datos transformados, como el número de películas alquiladas por mes y las categorías más alquiladas.
    - Funciones principales: [`query_peliculas_alquiladas_por_mes`](queries/business_queries.py#L18), `query_categorias_mas_alquiladas`

### Justificación del Diseño

1. **Modularidad**:
    - El diseño modular permite una fácil mantenibilidad y escalabilidad del proyecto. Cada módulo tiene una responsabilidad específica, lo que facilita la comprensión y el desarrollo de nuevas funcionalidades.

2. **Uso de PySpark**:
    - PySpark se utiliza para manejar grandes volúmenes de datos de manera eficiente. La conversión de DataFrames de pandas a DataFrames de Spark permite aprovechar las capacidades de procesamiento distribuido de Spark.

3. **Limpieza de Datos**:
    - La limpieza de datos es una etapa crítica en cualquier proceso ETL. Este proyecto incluye funciones específicas para limpiar los datos antes de la transformación, asegurando que los datos sean precisos y consistentes.

4. **Consultas de Negocio**:
    - Las consultas de negocio permiten obtener información valiosa de los datos transformados. Este proyecto incluye ejemplos de consultas que pueden ser fácilmente adaptadas o extendidas para satisfacer necesidades específicas.

5. **Registro de Logs**:
    - El uso de un sistema de logging (registro de logs) en cada módulo permite monitorear y depurar el proceso ETL de manera efectiva. Esto es crucial para identificar y resolver problemas rápidamente.

Este diseño asegura que el proceso ETL sea robusto, eficiente y fácil de mantener, cumpliendo con los requisitos de un entorno de datos moderno.
