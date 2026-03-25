import os
import random
from datetime import datetime
from airflow.decorators import dag, task
from tenacity import retry, stop_after_attempt, wait_exponential

# ==========================================
# 1. FUNCIONES SPARK CORE Y RESILIENCIA
# ==========================================
@retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
def ejecutar_spark_job(input_csv: str, output_parquet: str):
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
    import pyspark.sql.functions as F
    
    spark = SparkSession.builder.appName("Procesamiento_Masivo").master("local[*]").getOrCreate()
        
    try:
        df = spark.read.csv(input_csv, header=True, inferSchema=True)
        
        df_limpio = df \
            .filter(F.col("temperatura").isNotNull()) \
            .withColumn("fecha_particion", F.to_date(F.col("timestamp"))) \
            .withColumn("temp_c_redondeada", F.round(F.col("temperatura"), 2)) \
            .groupBy("id_sensor", "fecha_particion") \
            .agg(
                F.max("temp_c_redondeada").alias("temp_maxima"),
                F.avg("temp_c_redondeada").alias("temp_promedio"),
                F.count("*").alias("total_registros_procesados")
            )
            
        df_limpio.write.mode("overwrite").partitionBy("fecha_particion").parquet(output_parquet)
        return True
    except Exception as e:
        raise e
    finally:
        spark.stop()

def auditar_parquet(output_parquet: str):
    """Función para demostrar los resultados leyendo el Parquet generado."""
    import findspark
    findspark.init()
    from pyspark.sql import SparkSession
    
    spark = SparkSession.builder.appName("Auditoria_Parquet").master("local[*]").getOrCreate()
    try:
        print("=== RESULTADOS DEL PROCESAMIENTO DISTRIBUIDO ===")
        df_parquet = spark.read.parquet(output_parquet)
        df_parquet.show(10, truncate=False)
        print(f"Total de registros agregados: {df_parquet.count()}")
    finally:
        spark.stop()

# ==========================================
# 2. DEFINICIÓN DEL DAG
# ==========================================
@dag(
    dag_id='laboratorio_6_spark_etl',
    schedule=None,
    start_date=datetime(2026, 3, 16),
    catchup=False,
    tags=['Spark', 'Big Data']
)
def orquestacion_spark():
    
    BASE_DIR = "/opt/airflow/data_lake"
    RAW_PATH = f"{BASE_DIR}/raw/sensores_clima.csv"
    SILVER_PATH = f"{BASE_DIR}/silver/sensores_agregados"

    @task
    def generar_carga_masiva():
        os.makedirs(f"{BASE_DIR}/raw", exist_ok=True)
        with open(RAW_PATH, "w") as f:
            f.write("timestamp,id_sensor,temperatura,humedad\n")
            for _ in range(250000):
                sensor = random.choice(["SENS-NTE", "SENS-SUR", "SENS-ESTE", "SENS-OESTE"])
                temp = random.uniform(-5.0, 42.0)
                f.write(f"2026-03-21 08:00:00,{sensor},{temp},80.0\n")

    @task
    def procesar_con_spark():
        os.makedirs(f"{BASE_DIR}/silver", exist_ok=True)
        ejecutar_spark_job(RAW_PATH, SILVER_PATH)

    @task
    def demostrar_resultados_spark():
        auditar_parquet(SILVER_PATH)

    generar_carga_masiva() >> procesar_con_spark() >> demostrar_resultados_spark()

dag_principal = orquestacion_spark()