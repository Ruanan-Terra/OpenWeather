import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, from_unixtime

# ==========================================
# 1. CONFIGURAÇÕES E CREDENCIAIS
# ==========================================
load_dotenv()

# Baseado nos logs de erro anteriores, este é o nome da sua Storage Account
storage_account_name = "datalakeruanan26"  
azure_storage_key = os.getenv("AZURE_STORAGE_KEY1")

if not azure_storage_key:
    raise ValueError("A variável AZURE_STORAGE_KEY1 não foi encontrada no arquivo .env")

# ==========================================
# 2. INICIALIZAÇÃO OTIMIZADA DO SPARK (Mac M5)
# ==========================================
# Limitamos a memória do driver a 4GB para evitar superaquecimento no Mac fanless
# e carregamos os pacotes do Hadoop-Azure para permitir comunicação via protocolo abfss://
spark = SparkSession.builder \
    .appName("OpenWeather-ETL-Silver") \
    .config("spark.driver.memory", "4g") \
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.6") \
    .config(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", azure_storage_key) \
    .getOrCreate()

print("🚀 SparkSession iniciada e conectada ao Azure Data Lake Gen2!")

try:
    # ==========================================
    # 3. EXTRAÇÃO: LENDO DA CAMADA BRONZE
    # ==========================================
    path_bronze = f"abfss://datalake@{storage_account_name}.dfs.core.windows.net/bronze/weather_*.json"
    print(f"📖 Lendo arquivos brutos de: {path_bronze}")

    df_bronze = spark.read \
        .option("multiLine", "true") \
        .json(path_bronze)

    # ==========================================
    # 4. TRANSFORMAÇÃO: DATA WRANGLING & FLATTENING
    # ==========================================
    df_silver = df_bronze \
        .withColumn("weather_exploded", explode(col("weather"))) \
        .select(
            col("id").alias("city_id"),
            col("name").alias("city_name"),
            col("coord.lon").cast("double").alias("longitude"),
            col("coord.lat").cast("double").alias("latitude"),
            col("main.temp").cast("double").alias("temperature"),
            col("main.feels_like").cast("double").alias("feels_like"),
            col("main.humidity").cast("int").alias("humidity"),
            col("weather_exploded.main").alias("weather_condition"),
            col("weather_exploded.description").alias("weather_description"),
            col("wind.speed").cast("double").alias("wind_speed"),
            # Convertendo timestamp Unix (1780414093) para data/hora real
            from_unixtime(col("dt")).cast("timestamp").alias("capture_time")
        )

    print("✨ Transformações aplicadas: JSON achatado e tipagens definidas.")
    
    # Exibe as primeiras 5 linhas no terminal para validação visual rápida
    df_silver.show(5, truncate=False)

    # ==========================================
    # 5. CARGA: SALVANDO NA CAMADA SILVER
    # ==========================================
    path_silver = f"abfss://datalake@{storage_account_name}.dfs.core.windows.net/silver/cleaned_weather"
    print(f"Gravando os dados em formato Parquet na pasta: {path_silver}")
    
    df_silver.write \
        .mode("overwrite") \
        .parquet(path_silver)

    print("Pipeline da Camada Silver concluído com SUCESSO ABSOLUTO!")

except Exception as e:
    print(f"Ocorreu um erro durante o processamento: {e}")

finally:
    # Boas práticas: Sempre encerre a sessão para liberar a memória unificada do seu Mac
    spark.stop()
    print("SparkSession encerrada com segurança.")