from etl.logger import get_logger

logger = get_logger("ETL_Load")

def load_data(data_frames, output_path):
    logger.info("Cargando datos transformados en el destino")
    
    for name, df in data_frames.items():
        output_file = f"{output_path}/{name}.parquet"
        df.write.mode("overwrite").parquet(output_file)
        logger.info(f"Datos de {name} guardados en {output_file}")
