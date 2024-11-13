from pyspark.sql import functions as F
from etl.logger import get_logger

logger = get_logger("ETL_Transform")

def transform_data(data_frames):
    logger.info("Transformando los datos")

    # Ejemplo de transformación: filtrar clientes activos
    customers_df = data_frames["customer"]
    active_customers_df = customers_df.filter(customers_df["active"] == True)
    data_frames["customer"] = active_customers_df
    logger.info(f"Clientes activos filtrados (total: {active_customers_df.count()})")

    # Renombrar customer_id en el dataframe de rental para evitar conflictos
    rentals_df = data_frames["rental"]
    rentals_df = rentals_df.withColumnRenamed("customer_id", "rental_customer_id")

    # Renombrar inventory_id en rental y inventory para evitar conflictos
    rentals_df = rentals_df.withColumnRenamed("inventory_id", "rental_inventory_id")
    inventory_df = data_frames["inventory"].withColumnRenamed("inventory_id", "inventory_inventory_id")

    # Renombrar la columna last_update para evitar conflictos
    rentals_df = rentals_df.withColumnRenamed("last_update", "rental_last_update")
    customers_df = customers_df.withColumnRenamed("last_update", "customer_last_update")
    inventory_df = inventory_df.withColumnRenamed("last_update", "inventory_last_update")
    film_df = data_frames["film"].withColumnRenamed("last_update", "film_last_update")
    store_df = data_frames["store"].withColumnRenamed("last_update", "store_last_update")

    # Unir rental con customer (uno a muchos)
    rental_customers_df = rentals_df.join(customers_df, rentals_df["rental_customer_id"] == customers_df["customer_id"], "left")
    data_frames["rental_customers"] = rental_customers_df
    logger.info(f"Alquileres con clientes unidos (total: {rental_customers_df.count()})")

    # Unir rental con inventory (uno a muchos)
    rental_inventory_df = rental_customers_df.join(inventory_df, rental_customers_df["rental_inventory_id"] == inventory_df["inventory_inventory_id"], "left")
    data_frames["rental_inventory"] = rental_inventory_df
    logger.info(f"Alquileres con inventarios unidos (total: {rental_inventory_df.count()})")

    # Unir inventory con film (uno a muchos)
    rental_inventory_films_df = rental_inventory_df.join(film_df, rental_inventory_df["film_id"] == film_df["film_id"], "left")
    data_frames["rental_inventory_films"] = rental_inventory_films_df
    logger.info(f"Alquileres con inventarios y películas unidos (total: {rental_inventory_films_df.count()})")

    # Unir store con inventory (uno a muchos)
    store_inventory_df = inventory_df.join(store_df, inventory_df["store_id"] == store_df["store_id"], "left")
    data_frames["store_inventory"] = store_inventory_df
    logger.info(f"Inventarios con tiendas unidos (total: {store_inventory_df.count()})")

    return data_frames
