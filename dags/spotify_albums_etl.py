from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import time  # Para control de tasa
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ==========================================
# ⚙️ CONFIGURACIÓN MACRO DEL ORQUESTADOR
# ==========================================
default_args = {
    'owner': 'maestria_bigdata',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='08_spotify_albums_etl',
    default_args=default_args,
    start_date=datetime(2026, 2, 1),
    schedule_interval='@weekly',
    catchup=False,
    tags=['spotify', 'albums', 'medallion', 'fail_fast'],
    description='Pipeline Relacional: Extracción de Álbumes con Fail-Fast y CDC'
)
def spotify_albums_pipeline():
    
    BASE_DIR = '/opt/airflow/dags/data_lake/spotify_project'

    # ==========================================
    # 🥉 CAPA BRONZE: Extracción Resiliente y Fail-Fast
    # ==========================================
    @task
    def extraer_albumes_bronze() -> str:
        print("🔐 [BRONZE] Iniciando autenticación OAuth 2.0...")
        
        client_id = Variable.get("SPOTIFY_CLIENT_ID")
        client_secret = Variable.get("SPOTIFY_CLIENT_SECRET")
        
        # 1. Negociación del Token
        auth_url = 'https://accounts.spotify.com/api/token'
        auth_response = requests.post(auth_url, data={
            'grant_type': 'client_credentials',
            'client_id': client_id,
            'client_secret': client_secret,
        })
        auth_response.raise_for_status()
        headers = {'Authorization': f"Bearer {auth_response.json()['access_token']}"}

        # Lista de artistas objetivo
        ARTISTAS = [
            {'id': '1Xyo4u8uXC1ZmMpatF05PJ', 'name': 'The Weeknd'},
            {'id': '7ltDVBr6mKbRvohxheJ9h1', 'name': 'ROSALÍA'},
            {'id': '4tZwfgrHOc3mvqYlEYSvVi', 'name': 'Daft Punk'},
            {'id': '4dpARuHxo51G3z768sgnrY', 'name': 'Adele'},
            {'id': '7dGJo4pcD2V6oG8kP0tJRR', 'name': 'Eminem'}
        ]
        
        # MICRO-RESILIENCIA: Reintentos con backoff exponencial
        @retry(
            stop=stop_after_attempt(5), 
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(requests.exceptions.RequestException)
        )
        def _fetch_albums_page(artist_id, offset, limit=20):
            """Obtiene una página de álbumes para un artista."""
            url = f"https://api.spotify.com/v1/artists/{artist_id}/albums"
            # Parámetros simplificados (sin include_groups para evitar errores con token de cliente)
            params = {
                'limit': limit,
                'offset': offset
            }
            resp = requests.get(url, headers=headers, params=params, timeout=10)
            resp.raise_for_status()
            return resp.json()

        datos_crudos = []
        MAX_PAGINAS_POR_ARTISTA = 3  # Límite demostrativo (en producción podrías no poner límite)
        
        # 2. Extracción con paginación
        for artista in ARTISTAS:
            artist_id = artista['id']
            artist_name = artista['name']
            print(f"\n🎤 Procesando artista: {artist_name} ({artist_id})")
            
            offset = 0
            limit = 20
            total_albumes = None
            pagina = 1
            paginas_procesadas = 0
            
            while (total_albumes is None or offset < total_albumes) and paginas_procesadas < MAX_PAGINAS_POR_ARTISTA:
                print(f"   📄 Página {pagina} (offset {offset})...")
                try:
                    data = _fetch_albums_page(artist_id, offset, limit)
                    
                    if total_albumes is None:
                        total_albumes = data.get('total', 0)
                        print(f"   📊 Total de álbumes en API: {total_albumes}")
                    
                    items = data.get('items', [])
                    if not items:
                        print("      ⚠️ No hay más álbumes.")
                        break
                    
                    # Enriquecer con metadatos del artista (opcional, pero útil)
                    for item in items:
                        item['_extracted_artist_id'] = artist_id
                        item['_extracted_artist_name'] = artist_name
                        item['_extracted_page'] = pagina
                    
                    datos_crudos.extend(items)
                    print(f"      ✅ {len(items)} álbumes agregados de la página {pagina}.")
                    
                    offset += limit
                    pagina += 1
                    paginas_procesadas += 1
                    
                    # Pausa para no exceder el rate limit de Spotify
                    time.sleep(1)
                    
                except Exception as e:
                    print(f"   ❌ Error al obtener página {pagina} para {artist_name}: {e}")
                    # En un pipeline real podrías decidir si detener todo o continuar con el siguiente artista
                    raise  # Relanzamos para que falle rápido (fail-fast)
            
            print(f"   ✅ {paginas_procesadas} páginas extraídas de {artist_name} (total álbumes: {len([x for x in datos_crudos if x.get('_extracted_artist_id') == artist_id])})")
        
        # 🚨 COMPUERTA DE CALIDAD (FAIL-FAST) 🚨
        if not datos_crudos:
            raise ValueError("Fallo Crítico: No se extrajo ningún álbum. El pipeline se detiene aquí para proteger el Data Lakehouse.")
        
        # 3. Persistencia Raw
        df_raw = pd.DataFrame(datos_crudos)
        os.makedirs(f"{BASE_DIR}/bronze", exist_ok=True)
        ruta_bronze = f"{BASE_DIR}/bronze/raw_albums_{datetime.now().strftime('%Y%m%d_%H%M')}.parquet"
        
        df_raw.to_parquet(ruta_bronze, index=False)
        print(f"📦 [BRONZE] {len(df_raw)} álbumes guardados en total.")
        
        return ruta_bronze

    # ==========================================
    # 🥈 CAPA SILVER: Programación Defensiva y Encoding
    # ==========================================
    @task
    def transformar_albumes_silver(ruta_bronze: str) -> str:
        print(f"⚙️ [SILVER] Procesando dataset desde: {ruta_bronze}")
        df_raw = pd.read_parquet(ruta_bronze)
        albumes_limpios = []
        
        for _, row in df_raw.iterrows():
            # PROGRAMACIÓN DEFENSIVA: Extraer el ID del artista primario de forma segura
            artistas_involucrados = row.get('artists')
            if isinstance(artistas_involucrados, list) and len(artistas_involucrados) > 0:
                id_artista_principal = artistas_involucrados[0].get('id', 'Desconocido')
            else:
                id_artista_principal = 'Desconocido'

            albumes_limpios.append({
                'album_id': row.get('id', 'ID_Nulo'),
                'album_name': row.get('name', 'Sin Nombre'),
                'album_type': row.get('album_type', 'unknown'), 
                'release_date': row.get('release_date', '1900-01-01'),
                'total_tracks': row.get('total_tracks', 0),
                'primary_artist_id': id_artista_principal,
                'extraction_date': datetime.now().strftime('%Y-%m-%d')
            })
            
        df_silver = pd.DataFrame(albumes_limpios)
        
        # OPTIMIZACIÓN DE MEMORIA (Dictionary Encoding)
        df_silver['album_type'] = df_silver['album_type'].astype('category')
        
        os.makedirs(f"{BASE_DIR}/silver", exist_ok=True)
        ruta_silver = f"{BASE_DIR}/silver/clean_albums.parquet"
        df_silver.to_parquet(ruta_silver, index=False)
        
        return ruta_silver

    # ==========================================
    # 🥇 CAPA GOLD: Upsert Lógico (CDC)
    # ==========================================
    @task
    def cargar_albumes_gold(ruta_silver: str):
        print("🎯 [GOLD] Ejecutando Carga Incremental (CDC)...")
        df_staging = pd.read_parquet(ruta_silver)
        
        os.makedirs(f"{BASE_DIR}/gold", exist_ok=True)
        ruta_master = f"{BASE_DIR}/gold/master_spotify_albums.parquet"
        
        # ESCENARIO 1: Full Load Inicial
        if not os.path.exists(ruta_master):
            print("✨ [GOLD] Creando Tabla Maestra (Full Load)")
            df_staging.drop_duplicates(subset=['album_id'], keep='last', inplace=True)
            df_staging.to_parquet(ruta_master, index=False)
            return

        # ESCENARIO 2: CDC / Upsert Lógico
        df_master = pd.read_parquet(ruta_master)
        master_ids = df_master['album_id'].tolist()
        
        df_updates = df_staging[df_staging['album_id'].isin(master_ids)]
        df_inserts = df_staging[~df_staging['album_id'].isin(master_ids)]
        
        ids_to_update = df_updates['album_id'].tolist()
        df_master_preservado = df_master[~df_master['album_id'].isin(ids_to_update)]
        
        df_final = pd.concat([df_master_preservado, df_updates, df_inserts], ignore_index=True)
        df_final.drop_duplicates(subset=['album_id'], keep='last', inplace=True)
        
        df_final.to_parquet(ruta_master, index=False)
        print(f"📊 REPORTE CDC | Álbumes Actualizados: {len(df_updates)} | Nuevos Lanzamientos: {len(df_inserts)} | Total en Master: {len(df_final)}")

    # ==========================================
    # 🔀 TOPOLOGÍA DEL GRAFO
    # ==========================================
    ruta_raw = extraer_albumes_bronze()
    ruta_clean = transformar_albumes_silver(ruta_raw)
    cargar_albumes_gold(ruta_clean)

# Instanciación
dag_instance = spotify_albums_pipeline()