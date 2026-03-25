# 1. Imagen base oficial
FROM apache/airflow:2.8.1

# 2. Permisos de superusuario para dependencias del OS
USER root

# 3. Instalación de Java (Open JRE 17) - Estrictamente necesario para Spark
RUN apt-get update \
  && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
  && apt-get autoremove -yqq --purge \
  && apt-get clean \
  && rm -rf /var/lib/apt/lists/*

# 4. Variables de entorno para que PySpark encuentre Java
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64

# 5. Volver al usuario de Airflow por seguridad
USER airflow

# 6. Instalación de librerías Python requeridas para la maestría
RUN pip install --no-cache-dir pyspark findspark pandas requests pyarrow tenacity dbt-postgres