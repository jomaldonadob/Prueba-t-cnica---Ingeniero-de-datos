from pyspark.sql import SparkSession

def query_data():
    # Iniciar sesión de Spark
    spark = SparkSession.builder \
        .appName("Business_Queries") \
        .getOrCreate()
    
    # Cargar el archivo Parquet para realizar consultas
    film_df = spark.read.parquet("output_data/film_cleaned.parquet")
    inventory_df = spark.read.parquet("output_data/inventory_cleaned.parquet")
    customer_df = spark.read.parquet("output_data/customer_cleaned.parquet")
    
    # Mostrar estructura de los datos (opcional)
    film_df.show()  
    film_df.printSchema()
    
    # Crear vistas temporales para hacer consultas SQL
    film_df.createOrReplaceTempView("film_data")
    inventory_df.createOrReplaceTempView("inventory_data")
    customer_df.createOrReplaceTempView("customer_data")

    # Ejemplo de consulta de negocio
    query = """
    SELECT title, rental_rate, rating
    FROM film_data
    WHERE rating = 'PG-13' AND rental_rate > 2.0
    ORDER BY rental_rate DESC
    LIMIT 10
    """
    result_df = spark.sql(query)
    result_df.show()

    # Finalizar la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    query_data()
