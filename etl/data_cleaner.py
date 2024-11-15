from pyspark.sql import functions as F
from pyspark.sql.types import DecimalType, IntegerType

def clean_film_data(df_film):
    # Limpieza de `film_id`: extraer solo los números, y asegurar que es un entero
    df_film = df_film.withColumn("film_id", 
        F.regexp_extract("film_id", r"(\d+)", 1).cast(IntegerType()))  # Extrae solo el primer grupo de números encontrados

    df_film = df_film.withColumn("rental_rate", 
        F.regexp_extract("rental_rate", r"(\d+\.\d{2})", 1).cast(DecimalType(4, 2)))

    # Limpieza de `release_year`: extraer solo los números de 4 dígitos para el año
    df_film = df_film.withColumn("release_year", 
        F.regexp_extract("release_year", r"(\d{4})", 1).cast(IntegerType()))  # Extrae solo el primer grupo de 4 dígitos
    
    # Limpieza de `length`: extraer solo los números y convertir a SMALLINT
    df_film = df_film.withColumn("length", 
        F.regexp_extract("length", r"(\d+)", 1).cast(IntegerType()))
    
    df_film = df_film.withColumn("replacement_cost", 
        F.regexp_extract("replacement_cost", r"(\d+)", 1).cast(IntegerType()))  
    
    df_film = df_film.withColumn("num_voted_users", 
        F.regexp_extract("num_voted_users", r"(\d+)", 1).cast(IntegerType()))
    
    # Limpieza de `rating`: Asegurar que el valor esté en una lista de valores válidos
    valid_ratings = ["G", "PG", "PG-13", "R", "NC-17"]
    df_film = df_film.withColumn("rating",
        F.when(df_film["rating"].isin(valid_ratings), df_film["rating"])
         .otherwise("NR"))  # Asigna un valor por defecto ("NR") para valores inválidos en lugar de `None`
    
    # Guardar el DataFrame limpio
    df_film.write.mode("overwrite").parquet("output_data/film_cleaned.parquet")
    print("Datos limpiados y guardados en 'output_data/film_cleaned.parquet'")

def clean_inventory_data(df_inventory):
    # Limpieza de `store_id`: extraer solo los números y convertir a TINYINT
    df_inventory = df_inventory.withColumn("store_id", 
        F.regexp_extract("store_id", r"(\d+)", 1).cast(IntegerType()))
    # Guardar el DataFrame limpio
    df_inventory.write.mode("overwrite").parquet("output_data/inventory_cleaned.parquet")

    print("Limpieza de datos de inventory completada y guardada en 'output_data/inventory_cleaned.parquet'.")


def clean_customer_data(df_customer):
    # Limpiar `firstname` y `lastname`
    df_customer_cleaned = df_customer \
        .withColumn("first_name", F.upper(F.regexp_replace("first_name", r"[^A-Z]", ""))) \
        .withColumn("last_name", F.upper(F.regexp_replace("last_name", r"[^A-Z]", ""))) \
        .withColumn("email", F.regexp_replace("email", r"\\r$", "")) # Limpiar `email` eliminando el carácter "\r" al final si está presente
    # Guardar el DataFrame limpio
    df_customer_cleaned.write.mode("overwrite").parquet("output_data/customer_cleaned.parquet")
    
    print("Limpieza de datos en customer completada.")
