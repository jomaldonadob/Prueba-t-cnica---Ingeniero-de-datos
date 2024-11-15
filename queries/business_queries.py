from pyspark.sql import SparkSession
from pyspark.sql.functions import col, month, year, count, sum
import pandas as pd
from pyspark.sql import functions as F
from pyspark.sql import DataFrame
# Crear sesión de Spark
spark = SparkSession.builder.appName("BusinessQueries").getOrCreate()
spark.sparkContext.setLogLevel("ERROR")

# Cargar los datos limpios (ajusta la ruta según tu configuración)
customer_df = spark.read.parquet('output_data/customer_cleaned.parquet')
rentals_df = spark.read.parquet('output_data/rentals.parquet')
store_df = spark.read.parquet('output_data/store.parquet')
inventory_df = spark.read.parquet('output_data/inventory_cleaned.parquet')
film_df = spark.read.parquet('output_data/film_cleaned.parquet')

# Archivo: business_queries.py

def query_peliculas_alquiladas_por_mes(spark, rentals_df):
    # Crear vista temporal para rentals
    rentals_df.createOrReplaceTempView("rental")

    query = """
        SELECT YEAR(rental_date) AS anio, MONTH(rental_date) AS mes, COUNT(*) AS total_peliculas
        FROM rental
        GROUP BY YEAR(rental_date), MONTH(rental_date)
        ORDER BY anio, mes
    """
    return spark.sql(query)


def query_categorias_mas_alquiladas(spark, film_df, rentals_df, inventory_df):
    # Crear vistas temporales
    film_df.createOrReplaceTempView("film")
    rentals_df.createOrReplaceTempView("rental")
    inventory_df.createOrReplaceTempView("inventory")

    query = """
        SELECT f.rating AS categoria, COUNT(*) AS total_alquiladas
        FROM film f
        JOIN inventory i ON f.film_film_id = i.inventory_film_id
        JOIN rental r ON i.inventory_inventory_id = r.rental_inventory_id
        GROUP BY f.rating
        ORDER BY total_alquiladas DESC
    """
    return spark.sql(query)

def query_clientes_mas_activos(spark, rentals_df, customer_df):
    # Crear vistas temporales
    rentals_df.createOrReplaceTempView("rental")
    customer_df.createOrReplaceTempView("customer")

    query = """
        SELECT c.customer_id, CONCAT(c.first_name, ' ', c.last_name) AS cliente, COUNT(*) AS total_alquiladas
        FROM rental r
        JOIN customer c ON r.rental_customer_id = c.customer_id
        GROUP BY c.customer_id, c.first_name, c.last_name
        ORDER BY total_alquiladas DESC
        LIMIT 10
    """
    return spark.sql(query)

def query_peliculas_mas_populares(spark, film_df, rentals_df, inventory_df):
    # Crear vistas temporales
    film_df.createOrReplaceTempView("film")
    rentals_df.createOrReplaceTempView("rental")
    inventory_df.createOrReplaceTempView("inventory")

    query = """
        SELECT f.title AS pelicula, COUNT(*) AS total_alquiladas
        FROM film f
        JOIN inventory i ON f.film_film_id = i.inventory_film_id
        JOIN rental r ON i.inventory_inventory_id = r.rental_inventory_id
        GROUP BY f.title
        ORDER BY total_alquiladas DESC
        LIMIT 10
    """
    return spark.sql(query)

def query_ingresos_mensuales(spark, rentals_df, film_df, inventory_df):
    # Crear vistas temporales
    rentals_df.createOrReplaceTempView("rental")
    film_df.createOrReplaceTempView("film")
    inventory_df.createOrReplaceTempView("inventory")

    query = """
        SELECT YEAR(rental_date) AS anio, MONTH(rental_date) AS mes, SUM(f.rental_rate) AS ingresos_mensuales
        FROM rental r
        JOIN inventory i ON r.rental_inventory_id = i.inventory_inventory_id
        JOIN film f ON i.inventory_film_id = f.film_film_id
        GROUP BY YEAR(rental_date), MONTH(rental_date)
        ORDER BY anio, mes
    """
    return spark.sql(query)

def query_alquileres_por_categoria_mes(spark, rentals_df, film_df):
    # Crear vistas temporales para rentals y film
    rentals_df.createOrReplaceTempView("rental")
    film_df.createOrReplaceTempView("film")

    query = """
        SELECT 
            MONTH(r.rental_date) AS mes,
            f.rating AS categoria,
            COUNT(1) AS total_alquiladas
        FROM rental r
        INNER JOIN film f 
            ON r.rental_inventory_id = f.film_film_id
        GROUP BY 
            MONTH(r.rental_date), f.rating
        ORDER BY 
            mes ASC, categoria ASC
        """
    return spark.sql(query)
