from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

default_args = {
    'owner': 'maestria_bigdata',
    'retries': 2,
    'retry_delay': timedelta(minutes=2),
}

@dag(
    dag_id='spotify_etl',
    default_args=default_args,
    start_date=datetime(2026, 2, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['spotify', 'medallion', 'tenacity', 'cdc']
)
def master_spotify_pipeline():
    
    BASE_DIR = '/opt/airflow/dags/data_lake/spotify_project'

    # ==========================================
    # 🥉 CAPA BRONZE: Autenticación y Extracción
    # ==========================================
    @task
    def extraer_spotify_bronze() -> str:
        print("🔐 [BRONZE] Autenticando con Spotify OAuth 2.0...")
        client_id = Variable.get("SPOTIFY_CLIENT_ID")
        client_secret = Variable.get("SPOTIFY_CLIENT_SECRET")
        
        # 1. Obtener el Token de Acceso
        auth_url = 'https://accounts.spotify.com/api/token'
        auth_response = requests.post(auth_url, data={
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
        })
        auth_response.raise_for_status()
        access_token = auth_response.json()['access_token']
        print(f"🔑 Token (primeros 20 chars): {access_token[:20]}...")
        headers = {'Authorization': f'Bearer {access_token}'}

        # 2. Lista de IDs de Artistas (Ej: The Weeknd, Rosalía, Daft Punk, Bad Bunny)
        artistas_ids = ['1Xyo4u8uXC1ZmMpatF05PJ', '7ltDVBr6mKbRvohxheJ9h1', '4tZwfgrHOc3mvqYlEYSvVi', '4dpARuHxo51G3z768sgnrY','7dGJo4pcD2V6oG8kP0tJRR']
        
        # 3. Extracción Resiliente
        @retry(
            stop=stop_after_attempt(5), 
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(requests.exceptions.RequestException)
        )
        def _fetch_artist(artist_id):
            url = f"https://api.spotify.com/v1/artists/{artist_id}"
            print(f"   🔍 Solicitando artista {artist_id} desde {url}")
            res = requests.get(url, headers=headers, timeout=10)
            print(f"   📡 Código de respuesta: {res.status_code}")
            print(f"   📨 Headers de respuesta: {dict(res.headers)}")
            if res.status_code != 200:
                print(f"   ⚠️ Contenido del error: {res.text}")
            res.raise_for_status()
            data = res.json()
            print(f"   ✅ Claves recibidas: {list(data.keys())}")
            print(f"   📦 JSON completo: {data}") 
            return data

        datos_crudos = []
        for a_id in artistas_ids:
            try:
                datos_crudos.append(_fetch_artist(a_id))
                print(f"   ✅ Artista extraído exitosamente.")
            except Exception as e:
                print(f"   ❌ Error extrayendo artista {a_id}: {e}")
        
        df_raw = pd.DataFrame(datos_crudos)
        os.makedirs(f"{BASE_DIR}/bronze", exist_ok=True)
        ruta_bronze = f"{BASE_DIR}/bronze/raw_artists_{datetime.now().strftime('%Y%m%d_%H%M')}.parquet"
        
        df_raw.to_parquet(ruta_bronze, index=False)
        return ruta_bronze # XCom por referencia

    # ==========================================
    # 🥈 CAPA SILVER: Transformación y Optimización
    # ==========================================
    @task
    def transformar_silver(ruta_bronze: str) -> str:
        print(f"⚙️ [SILVER] Procesando: {ruta_bronze}")
        df_raw = pd.read_parquet(ruta_bronze)
        datos_limpios = []
        
        # Aplanamiento de JSON
        for _, row in df_raw.iterrows():
            print(row)
            datos_limpios.append({
                'artist_id': row.get('id'),
                'artist_name': row.get('name'),
                #'followers': row['followers'].get('total'),
                #'popularity': row.get('popularity'),
                # Extraemos solo el primer género principal (o 'Desconocido' si no tiene)
                #'main_genre': row.get('genres')[0] if isinstance(row.get('genres'), list) and len(row.get('genres')) > 0 else 'Desconocido',
                'extraction_date': datetime.now().strftime('%Y-%m-%d')
            })
            
        df_silver = pd.DataFrame(datos_limpios)
        
        # Dictionary Encoding (Optimización de RAM para el género)
        #df_silver['main_genre'] = df_silver['main_genre'].astype('category')
        
        os.makedirs(f"{BASE_DIR}/silver", exist_ok=True)
        ruta_silver = f"{BASE_DIR}/silver/clean_artists.parquet"
        df_silver.to_parquet(ruta_silver, index=False)
        return ruta_silver # XCom por referencia

    # ==========================================
    # 🥇 CAPA GOLD: Carga Incremental (CDC)
    # ==========================================
    @task
    def cargar_gold(ruta_silver: str):
        print("🎯 [GOLD] Ejecutando Carga Incremental (Merge)...")
        df_staging = pd.read_parquet(ruta_silver)
        
        os.makedirs(f"{BASE_DIR}/gold", exist_ok=True)
        ruta_master = f"{BASE_DIR}/gold/master_spotify_artists.parquet"
        
        if not os.path.exists(ruta_master):
            print("✨ Creando Tabla Maestra (Full Load)")
            df_staging.to_parquet(ruta_master, index=False)
            return

        df_master = pd.read_parquet(ruta_master)
        master_ids = df_master['artist_id'].tolist()
        
        # Separación Lógica
        df_updates = df_staging[df_staging['artist_id'].isin(master_ids)]
        df_inserts = df_staging[~df_staging['artist_id'].isin(master_ids)]
        
        # UPSERT
        ids_to_update = df_updates['artist_id'].tolist()
        df_master_preservado = df_master[~df_master['artist_id'].isin(ids_to_update)]
        
        df_final = pd.concat([df_master_preservado, df_updates, df_inserts], ignore_index=True)
        df_final.to_parquet(ruta_master, index=False)
        
        print(f"📊 REPORTE CDC | Updates: {len(df_updates)} | Inserts: {len(df_inserts)} | Total: {len(df_final)}")

    # ==========================================
    # 🔀 Topología del Grafo (DAG)
    # ==========================================
    ruta_raw = extraer_spotify_bronze()
    ruta_clean = transformar_silver(ruta_raw)
    cargar_gold(ruta_clean)

mi_dag_productivo = master_spotify_pipeline()