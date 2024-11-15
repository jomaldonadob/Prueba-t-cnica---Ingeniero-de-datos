from pyspark.sql import SparkSession
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.logger import get_logger
from etl.data_cleaner import clean_film_data, clean_inventory_data, clean_customer_data

logger = get_logger("ETL_Main")

def main():
    try:
        # Crear sesión de Spark
        spark = SparkSession.builder \
            .appName("ETL_Spark_Project") \
            .getOrCreate()

        # Ruta del archivo Excel de entrada y el directorio de salida
        input_file = "/home/stiven/Documentos/Prueba-t-cnica---Ingeniero-de-datos/data/Films_2.xlsx"
        output_path = "output_data"

        # Ejecución del ETL
        logger.info("Iniciando el proceso ETL")
        data_frames = extract_data(spark, input_file)  # Extraer los datos desde el archivo Excel
        
        # Limpieza de datos en los tres DataFrames
        clean_film_data(data_frames['film'])  # Limpiar el DataFrame 'film'
        clean_inventory_data(data_frames['inventory'])  # Limpiar el DataFrame 'inventory'
        clean_customer_data(data_frames['customer'])  # Limpiar el DataFrame 'customer'
        
        # Transformación de los datos
        data_frames = transform_data(data_frames)

        # Cargar los datos transformados
        load_data(data_frames, output_path)

        logger.info("Proceso ETL completado exitosamente")

    except Exception as e:
        logger.error(f"Ocurrió un error en el proceso ETL: {e}")
        raise  # Relanzamos la excepción para asegurarnos de que el proceso falle correctamente

    finally:
        # Finalizar sesión de Spark
        spark.stop()

if __name__ == "__main__":
    main()
