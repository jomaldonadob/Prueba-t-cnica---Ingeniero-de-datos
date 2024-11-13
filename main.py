from pyspark.sql import SparkSession
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data
from etl.logger import get_logger

logger = get_logger("ETL_Main")

def main():
    try:
        # Crear sesión de Spark
        spark = SparkSession.builder \
            .appName("ETL_Spark_Project") \
            .getOrCreate()

        # Ruta del archivo Excel de entrada y el directorio de salida
        input_file = "/home/stiven/Documentos/Prueba-t-cnica---Ingeniero-de-datos/data/Films_2.xlsx"  # Ruta completa al archivo
        output_path = "output_data"      # Directorio donde se guardará la salida

        # Ejecución del ETL
        logger.info("Iniciando el proceso ETL")
        data_frames = extract_data(spark, input_file)  # Extraer los datos desde el archivo Excel
        data_frames = transform_data(data_frames)     # Transformar los datos según la lógica definida
        load_data(data_frames, output_path)           # Cargar los datos transformados en el destino

        # Finalizar sesión de Spark
        spark.stop()
        logger.info("Proceso ETL completado exitosamente")

    except Exception as e:
        logger.error(f"Ocurrió un error en el proceso ETL: {e}")
        raise  # Relanzamos la excepción para asegurarnos de que el proceso falle correctamente

if __name__ == "__main__":
    main()
