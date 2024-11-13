import pandas as pd
from pyspark.sql import SparkSession
from etl.logger import get_logger

logger = get_logger("ETL_Extract")

def extract_data(spark, input_file):
    logger.info(f"Extrayendo datos desde el archivo Excel: {input_file}")

    # Leer el archivo Excel con las hojas en el orden correcto
    xls = pd.ExcelFile(input_file)

    # Extraer las hojas
    film_df = pd.read_excel(xls, sheet_name=1)        # Hoja 2: film (índice 1)
    inventory_df = pd.read_excel(xls, sheet_name=2)    # Hoja 3: inventory
    rental_df = pd.read_excel(xls, sheet_name=3)       # Hoja 4: rental
    customer_df = pd.read_excel(xls, sheet_name=4)     # Hoja 5: customer
    store_df = pd.read_excel(xls, sheet_name=5)        # Hoja 6: store

    # Verificar si algún DataFrame está vacío
    for df_name, df in zip(["film", "inventory", "rental", "customer", "store"], 
                            [film_df, inventory_df, rental_df, customer_df, store_df]):
        if df.empty:
            logger.warning(f"La hoja '{df_name}' está vacía.")
            return {}  # Puedes devolver un diccionario vacío o manejar el error de otra manera

    # Convertir los DataFrames de pandas a DataFrames de Spark
    film_spark_df = spark.createDataFrame(film_df)
    inventory_spark_df = spark.createDataFrame(inventory_df)
    rental_spark_df = spark.createDataFrame(rental_df)
    customer_spark_df = spark.createDataFrame(customer_df)
    store_spark_df = spark.createDataFrame(store_df)

    # Limpiar los nombres de las columnas: eliminar espacios al principio y al final
    def clean_column_names(df):
        cleaned_columns = [col.strip() for col in df.columns]
        return df.toDF(*cleaned_columns)

    # Limpiar los nombres de las columnas de todos los DataFrames
    film_spark_df = clean_column_names(film_spark_df)
    inventory_spark_df = clean_column_names(inventory_spark_df)
    rental_spark_df = clean_column_names(rental_spark_df)
    customer_spark_df = clean_column_names(customer_spark_df)
    store_spark_df = clean_column_names(store_spark_df)

    # Almacenar los DataFrames en un diccionario para ser utilizados más tarde
    data_frames = {
        "film": film_spark_df,
        "inventory": inventory_spark_df,
        "rental": rental_spark_df,
        "customer": customer_spark_df,
        "store": store_spark_df
    }

    logger.info("Datos extraídos y procesados correctamente.")
    return data_frames
