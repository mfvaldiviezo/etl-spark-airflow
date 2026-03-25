from airflow.decorators import dag, task
from airflow.models import Variable
from datetime import datetime, timedelta
import pandas as pd
import requests
import os
import time
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

# ==========================================
# ⚙️ CONFIGURACIÓN DEL ORQUESTADOR
# ==========================================
default_args = {
    'owner': 'maestria_bigdata',
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

@dag(
    dag_id='10_crypto_live_etl',
    default_args=default_args,
    start_date=datetime(2026, 2, 1),
    schedule_interval='@hourly', # Ejecución frecuente
    catchup=False,
    tags=['crypto', 'medallion', 'live_demo', 'cdc'],
    description='Pipeline en Tiempo Real: Criptomonedas, Throttling y Upsert Extremo'
)
def crypto_live_pipeline():
    
    BASE_DIR = '/opt/airflow/dags/data_lake/crypto_project'

    # ==========================================
    # 🥉 CAPA BRONZE: Paginación y Control de Límite (Throttling)
    # ==========================================
    @task
    def extraer_crypto_bronze() -> str:
        print("🚀 [BRONZE] Conectando al Mercado Cripto 24/7...")
        
        api_key = Variable.get("COINGECKO_API_KEY")
        headers = {
            "accept": "application/json",
            "x-cg-demo-api-key": api_key  # Autenticación requerida por CoinGecko
        }
        
        # MICRO-RESILIENCIA: Tolerancia a bloqueos
        @retry(
            stop=stop_after_attempt(5), 
            wait=wait_exponential(multiplier=1, min=2, max=10),
            retry=retry_if_exception_type(requests.exceptions.RequestException)
        )
        def _fetch_crypto_page(page):
            # Trae 100 monedas por página, ordenadas por capitalización de mercado
            url = f"https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&order=market_cap_desc&per_page=100&page={page}"
            res = requests.get(url, headers=headers, timeout=10)
            res.raise_for_status()
            return res.json()

        datos_crudos = []
        paginas_a_extraer = 3 # Top 300 criptomonedas
        
        # Bucle de Paginación
        for pagina in range(1, paginas_a_extraer + 1):
            try:
                respuesta = _fetch_crypto_page(pagina)
                print(respuesta)
                datos_crudos.extend(respuesta)
                print(f"   ✅ Página {pagina} (100 monedas) extraída con éxito.")
                
                # 🛡️ THROTTLING VITAL: La API gratuita permite max 30 req/minuto
                # Si no ponemos a dormir el código, explotará con Error 429
                time.sleep(3) # Pausa ética
                
            except requests.exceptions.HTTPError as e:
                print(f"   ❌ HTTPError en página {pagina}: {e.response.text}")
            except Exception as e:
                print(f"   ❌ Error general en página {pagina}: {e}")

        # 🚨 FAIL-FAST
        if not datos_crudos:
            raise ValueError("Fallo Crítico: No se pudo leer el mercado. Abortando para proteger la capa Silver.")

        # Persistencia Raw
        df_raw = pd.DataFrame(datos_crudos)
        os.makedirs(f"{BASE_DIR}/bronze", exist_ok=True)
        ruta_bronze = f"{BASE_DIR}/bronze/raw_crypto_{datetime.now().strftime('%Y%m%d_%H%M%S')}.parquet"
        
        df_raw.to_parquet(ruta_bronze, index=False)
        print(f"📦 [BRONZE] Extraídas {len(df_raw)} criptomonedas del Top Global.")
        
        return ruta_bronze 

    # ==========================================
    # 🥈 CAPA SILVER: Limpieza y Casting
    # ==========================================
    @task
    def transformar_crypto_silver(ruta_bronze: str) -> str:
        print(f"⚙️ [SILVER] Estandarizando telemetría financiera...")
        df_raw = pd.read_parquet(ruta_bronze)
        monedas_limpias = []
        
        for _, row in df_raw.iterrows():
            monedas_limpias.append({
                'coin_id': row.get('id', 'desconocido'),
                'symbol': str(row.get('symbol', '')).upper(),
                'name': row.get('name', 'Sin Nombre'),
                'current_price_usd': float(row.get('current_price', 0.0) or 0.0),
                'market_cap': int(row.get('market_cap', 0) or 0),
                'price_change_24h': float(row.get('price_change_percentage_24h', 0.0) or 0.0),
                'last_updated': row.get('last_updated', datetime.now().isoformat()),
                'extraction_timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            })
            
        df_silver = pd.DataFrame(monedas_limpias)
        
        os.makedirs(f"{BASE_DIR}/silver", exist_ok=True)
        ruta_silver = f"{BASE_DIR}/silver/clean_crypto.parquet"
        df_silver.to_parquet(ruta_silver, index=False)
        
        return ruta_silver

    # ==========================================
    # 🥇 CAPA GOLD: Upsert Lógico (CDC Extremo)
    # ==========================================
    @task
    def cargar_crypto_gold(ruta_silver: str):
        print("🎯 [GOLD] Ejecutando Merge Bursátil (CDC)...")
        df_staging = pd.read_parquet(ruta_silver)
        
        os.makedirs(f"{BASE_DIR}/gold", exist_ok=True)
        ruta_master = f"{BASE_DIR}/gold/master_crypto_prices.parquet"
        
        # ESCENARIO 1: Primera vez (Full Load)
        if not os.path.exists(ruta_master):
            print("✨ [GOLD] Inicializando Data Lakehouse Financiero (Full Load)")
            df_staging.drop_duplicates(subset=['coin_id'], keep='last', inplace=True)
            df_staging.to_parquet(ruta_master, index=False)
            return

        # ESCENARIO 2: Actualización en vivo (CDC)
        df_master = pd.read_parquet(ruta_master)
        master_ids = df_master['coin_id'].tolist()
        
        df_updates = df_staging[df_staging['coin_id'].isin(master_ids)]
        df_inserts = df_staging[~df_staging['coin_id'].isin(master_ids)]
        
        ids_to_update = df_updates['coin_id'].tolist()
        df_master_preservado = df_master[~df_master['coin_id'].isin(ids_to_update)]
        
        df_final = pd.concat([df_master_preservado, df_updates, df_inserts], ignore_index=True)
        df_final.drop_duplicates(subset=['coin_id'], keep='last', inplace=True)
        
        df_final.to_parquet(ruta_master, index=False)
        
        # Mensaje de impacto para la clase
        print("==================================================")
        print(f"📈 REPORTE CDC EN VIVO:")
        print(f"   🔄 Precios Actualizados (Updates): {len(df_updates)}")
        print(f"   ✨ Nuevas Monedas en el Top 300 (Inserts): {len(df_inserts)}")
        print(f"   🏦 Total de activos rastreados: {len(df_final)}")
        print("==================================================")

    # ==========================================
    # 🔀 TOPOLOGÍA
    # ==========================================
    ruta_raw = extraer_crypto_bronze()
    ruta_clean = transformar_crypto_silver(ruta_raw)
    cargar_crypto_gold(ruta_clean)

dag_instance = crypto_live_pipeline()