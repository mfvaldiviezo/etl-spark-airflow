from airflow.decorators import dag, task
from datetime import datetime, timedelta
import pandas as pd
import requests
import json
import os

# Argumentos base del DAG
default_args = {
    'owner': 'Clases_etl',
    'retries': 1,
    'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='owd_etl',
    default_args=default_args,
    start_date=datetime(2026, 2, 14),
    schedule_interval='@daily',
    catchup=False,
    tags=['basico', 'etl', 'weather', 'json']
)
def simple_weather_etl():
    
    # Rutas base de nuestro Data Lake local
    BASE_DIR = '/opt/airflow/dags/data_lake/simple_weather'
    API_KEY = "f270e6944923e5b76493372838b6f627" # Solo para demostración ubicar la ApiKey aquí
    CITY = "Loja,EC"
    
    # ---------------------------------------------------------
    # 1. EXTRACT: Entendiendo la estructura del JSON
    # ---------------------------------------------------------
    @task
    def extraer_api_y_explorar() -> str:
        print(f"📡 Solicitando clima actual para: {CITY}")
        url = f"https://api.openweathermap.org/data/2.5/weather?q={CITY}&appid={API_KEY}&units=metric"
        
        response = requests.get(url)
        response.raise_for_status()
        datos_json = response.json()
        
        # 💡 PARTE PEDAGÓGICA: Imprimir el JSON formateado en los logs
        print("\n" + "="*50)
        print("🔍 ESTRUCTURA DEL JSON RECIBIDO (RAW DATA):")
        print(json.dumps(datos_json, indent=4))
        print("="*50 + "\n")
        
        # Guardamos el archivo crudo en formato .json
        os.makedirs(f"{BASE_DIR}/raw", exist_ok=True)
        raw_path = f"{BASE_DIR}/raw/clima_crudo.json"
        
        with open(raw_path, 'w') as f:
            json.dump(datos_json, f)
            
        return raw_path

    # ---------------------------------------------------------
    # 2. TRANSFORM: Aplanando el JSON (Flattening)
    # ---------------------------------------------------------
    @task
    def transformar_datos(raw_path: str) -> str:
        print(f"⚙️ Leyendo archivo crudo desde: {raw_path}")
        
        with open(raw_path, 'r') as f:
            datos = json.load(f)
            
        # 💡 EXPLICACIÓN DEL PARSEO:
        # Aquí navegamos por el árbol del JSON que vimos en la tarea anterior
        diccionario_plano = {
            'ciudad': datos['nam'],                         # Nivel raíz
            'pais': datos['sys']['country'],                 # Diccionario dentro de diccionario
            'temperatura_actual': datos['main']['temp'],     # Diccionario dentro de diccionario
            'humedad': datos['main']['humidity'],            # Diccionario dentro de diccionario
            'clima_descripcion': datos['weather'][0]['main'] # ¡Lista que contiene un diccionario!
        }
        
        # Convertimos nuestro diccionario plano a un DataFrame de Pandas
        df = pd.DataFrame([diccionario_plano])
        print("\n✅ DATOS TRANSFORMADOS (TABULAR):")
        print(df.to_string())
        
        # Guardamos en formato tabular eficiente (Parquet)
        os.makedirs(f"{BASE_DIR}/clean", exist_ok=True)
        clean_path = f"{BASE_DIR}/clean/clima_limpio.parquet"
        df.to_parquet(clean_path, index=False)
        
        return clean_path

    # ---------------------------------------------------------
    # 3. LOAD: Simulando la carga analítica
    # ---------------------------------------------------------
    @task
    def cargar_datos(clean_path: str):
        print(f"📈 Cargando datos finales desde: {clean_path}")
        df_final = pd.read_parquet(clean_path)
        
        # Simulamos insertar en una base de datos o Data Warehouse
        print("\n🚀 CARGA EXITOSA. Listo para Power BI / Tableau:")
        print(df_final)

    # ---------------------------------------------------------
    # ORDEN DE EJECUCIÓN (TOPOLOGÍA)
    # ---------------------------------------------------------
    archivo_json = extraer_api_y_explorar()
    archivo_parquet = transformar_datos(archivo_json)
    cargar_datos(archivo_parquet)

# Instanciar el DAG
dag_instance = simple_weather_etl()