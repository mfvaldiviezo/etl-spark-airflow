from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

default_args = {
    'owner': 'Procesos_ETL',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='owd_etl_robusto',
    default_args = default_args,
    start_date=datetime(2025,2,1),
    schedule_interval='@hourly',
    catchup= False,
    tags=['iot', 'tenacity', 'etl', 'security'],
    description='Pipeline ETL Robusto con datos IoT del Clima'
)

def pipeline_weather_iot():
    BASE_DIR = '/opt/airflow/dags/data_lake/master_weather'

    @task
    def extraer_raw() -> str:
        print("Iniciando la extracción de datos desde la Api...")

        API_KEY = Variable.get('OPENWEATHER_API_KEY')

        ciudades = ['Quito', 'Loja', 'Guayaquil', 'Cuenca', 'London', 'Tokyo', 'Madrid'] 

        # Resiliencia (Tenacity)
        @retry(
            stop=stop_after_attempt(5),
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(requests.exceptions.RequestException)
        )

        def safe_api_call(city):
            url=f"https://api.openweathermap.org/data/2.5/weather?q={city}&appid={API_KEY}&units=metric"
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            return response.json()

        datos_crudos = []

        for ciudad in ciudades:
            try:
                datos_crudos.append(safe_api_call(ciudad))
                print(f"Extraido: {ciudad}")
            except Exception as e:
                print(f"Error al extraer en {ciudad}: {e}")
        
        if not datos_crudos:
            raise ValueError("Fallo: Ningún dato extraido.")
        
        df_raw = pd.DataFrame(datos_crudos)
        os.makedirs(f"{BASE_DIR}/bronze", exist_ok=True)
        bronze_path=f"{BASE_DIR}/bronze/raw_weather_{datetime.now().strftime('%Y%m%d_%H%M')}.parquet"

        df_raw.to_parquet(bronze_path, index=False)
        print(f"(Bronze) Datos guardados en: {bronze_path}")

        return bronze_path     # Usando XCom
    
    @task
    def transformar_datos(bronze_path: str) -> str:
        print(f"Porcesando datos desde: {bronze_path}")

        df_raw = pd.read_parquet(bronze_path)
        datos_limpios = []

        for _, row in df_raw.iterrows():
            datos_limpios.append({
                'station_id':row.get('id'),
                'city_name':row.get('name'),
                'temperature_c':row['main'].get('temp'),
                'weather_condition':row['weather'][0].get('main'),
                'extraction_ts': pd.to_datetime(row.get('dt'), unit='s')
            })

        df_silver = pd.DataFrame(datos_limpios)

        # Dictionary Encoding
        memoria_antes = df_silver.memory_usage(deep=True).sum()

        df_silver['city_name'] = df_silver['city_name'].astype('category')
        df_silver['weather_condition'] = df_silver['weather_condition'].astype('category')

        memoria_despues = df_silver.memory_usage(deep=True).sum()

        ahorro = (memoria_antes - memoria_despues) / memoria_antes * 100

        print(f"Dictionary Encoding Aplicado. Ahorro en RAM: {ahorro:.2f}%")

        os.makedirs(f"{BASE_DIR}/silver", exist_ok=True)
        silver_path=f"{BASE_DIR}/silver/clean_weather_data.parquet"

        df_silver.to_parquet(silver_path, index=False)
        print(f"(Silver) Datos guardados en: {silver_path}")

        return silver_path     #  Usando Xcom, se transmite únicamente la Ruta en lugar del DF
    
    @task
    def cargar_datos(silver_path: str):
        print("Iniciando Carga Incremental")

        df_staging = pd.read_parquet(silver_path)
        os.makedirs(f"{BASE_DIR}/gold", exist_ok=True)
        master_path = f"{BASE_DIR}/gold/master_weather_table.parquet"

        if not os.path.exists(master_path):
            print("Creando Tabla Maestra (Full Load).")
            df_staging.to_parquet(master_path, index= False)
            return
        
        df_master = pd.read_parquet(master_path)
        master_ids = df_master['station_id'].tolist()

        df_updates = df_staging[df_staging['station_id'].isin(master_ids)]
        df_inserts = df_staging[~df_staging['station_id'].isin(master_ids)]

        ids_to_update = df_updates['station_id'].tolist()
        df_master_preserved = df_master[df_master['station_id'].isin(ids_to_update)]

        df_final = pd.concat([df_master_preserved, df_updates, df_inserts], ignore_index=True)
        df_final.to_parquet(master_path, index=False)

        print("Reporte de Carga Incremental")

        print(f"   -> Actualizados (Updates): {len(df_updates)}")
        print(f"   -> Nuevos (Inserts):       {len(df_inserts)}")
        print(f"   -> Total en Master:        {len(df_final)}")

    
    #Topología del Grafo DAG
    path_raw = extraer_raw()
    path_clean = transformar_datos(path_raw)
    cargar_datos(path_clean)


# Instanciación
dag_instance = pipeline_weather_iot()



        
