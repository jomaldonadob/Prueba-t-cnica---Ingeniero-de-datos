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

    #Crear un diccionario con los DataFrames transformados
    transformed_data = {
        "customers": customers_df,
        "rentals": rentals_df,
        "inventory": inventory_df,
        "store": store_df,
        "film": film_df
    }
    logger.info(f"Transformaciones completadas")
    
    return transformed_data

