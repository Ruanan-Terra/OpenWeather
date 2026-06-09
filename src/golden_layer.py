import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def process_gold_layer():
    print("A iniciar a construção da Camada Gold...")

    # 1. Carregar variáveis de ambiente de forma segura
    load_dotenv()
    azure_storage_key = os.getenv("AZURE_STORAGE_KEY1")
    
    # Credenciais do Azure
    storage_account_name = "datalakeruanan26"
    container_name = "datalake"

    # 2. Inicializar o Spark (Otimizado para Apple Silicon M5)
    spark = SparkSession.builder \
        .appName("OpenWeather_GoldLayer") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.3.6") \
        .config(f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net", azure_storage_key) \
        .getOrCreate()

    # Caminhos do Data Lake
    silver_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/silver/cleaned_weather"
    gold_dim_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold/dim_cidades"
    gold_fact_path = f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/gold/fact_meteorologia"

    try:
        # 3. Ler a Tabela Silver (O Tabelão achatado)
        print("A ler dados da Camada Silver...")
        df_silver = spark.read.parquet(silver_path)

        # 4. Construir a Tabela Dimensão (dim_cidades)
        # Usamos distinct() para garantir que cada cidade apareça apenas uma vez, independentemente de quantas medições existam.
        print("A modelar a Tabela Dimensão (dim_cidades)...")
        dim_cidades = df_silver.select(
            col("city_id"),
            col("city_name"),
            col("latitude").alias("lat"),
            col("longitude").alias("lon")
        ).distinct()

        # 5. Construir a Tabela Fato (fact_meteorologia)
        # Selecionamos apenas as métricas de tempo e clima, usando o city_id como "chave estrangeira".
        print("A modelar a Tabela Fato (fact_meteorologia)...")
        fact_meteorologia = df_silver.select(
            col("city_id"),
            col("capture_time").alias("timestamp"),
            col("temperature").alias("temp"),
            col("humidity"),
            col("wind_speed")
        )

        # 6. Gravar as tabelas na Camada Gold
        print("A gravar Tabelas Analíticas no Azure Data Lake (Camada Gold)...")
        
        # Gravar a Dimensão (Modo Overwrite, pois os dados estáticos apenas se atualizam)
        dim_cidades.write.mode("overwrite").parquet(gold_dim_path)
        print(" - dim_cidades gravada com sucesso.")

        # Gravar a Fato (Modo Overwrite ou Append, dependendo da sua estratégia de carga diária)
        fact_meteorologia.write.mode("overwrite").parquet(gold_fact_path)
        print(" - fact_meteorologia gravada com sucesso.")

    except Exception as e:
        print(f"Erro durante o processamento da Camada Gold: {e}")
    finally:
        spark.stop()
        print("Sessão do Spark encerrada.")

if __name__ == "__main__":
    process_gold_layer()