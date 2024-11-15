from pyspark.sql import functions as F
from etl.logger import get_logger
from etl.data_cleaner import clean_customer_data, clean_inventory_data, clean_film_data

logger = get_logger("ETL_Transform")

def transform_data(data_frames):
    logger.info("Transformando los datos")
    
    # Obtener los DataFrames base
    customers_df = data_frames["customer"]
    rentals_df = data_frames["rental"]
    inventory_df = data_frames["inventory"]
    store_df = data_frames["store"]
    film_df = data_frames["film"]

    # Transformaciones y uniones
    # Renombrar y transformar columnas en rental
    rentals_df = rentals_df.withColumnRenamed("customer_id", "rental_customer_id") \
                           .withColumnRenamed("inventory_id", "rental_inventory_id") \
                           .withColumnRenamed("last_update", "rental_last_update")
    
    # Renombrar y transformar columnas en inventory
    inventory_df = inventory_df.withColumnRenamed("inventory_id", "inventory_inventory_id") \
                               .withColumnRenamed("film_id", "inventory_film_id") \
                               .withColumnRenamed("last_update", "inventory_last_update")
    
    # Renombrar y transformar columnas en customer
    customers_df = customers_df.withColumnRenamed("store_id", "customer_store_id") \
                               .withColumnRenamed("last_update", "customer_last_update")
    
    # Renombrar y transformar columnas en store
    store_df = store_df.withColumnRenamed("store_id", "store_store_id") \
                       .withColumnRenamed("last_update", "store_last_update")
    
    # Renombrar y transformar columnas en film
    film_df = film_df.withColumnRenamed("film_id", "film_film_id") \
                     .withColumnRenamed("last_update", "film_last_update")

    # Uniones entre DataFrames
    rental_customers_df = rentals_df.join(customers_df, rentals_df["rental_customer_id"] == customers_df["customer_id"], "left")
    rental_inventory_df = rental_customers_df.join(inventory_df, rental_customers_df["rental_inventory_id"] == inventory_df["inventory_inventory_id"], "left")
    rental_inventory_films_df = rental_inventory_df.join(film_df, rental_inventory_df["inventory_film_id"] == film_df["film_film_id"], "left")
    store_inventory_df = inventory_df.join(store_df, inventory_df["store_id"] == store_df["store_store_id"], "left")
    
    # Crear un diccionario con los DataFrames transformados
    transformed_data = {
        "rental_customers": rental_customers_df,
        "rental_inventory": rental_inventory_df,
        "rental_inventory_films": rental_inventory_films_df,
        "store_inventory": store_inventory_df
    }

    logger.info(f"Transformaciones completadas. Total de filas en 'rental_inventory_films': {rental_inventory_films_df.count()}")
    
    return transformed_data

