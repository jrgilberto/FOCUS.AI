import duckdb
import polars as pl
import os
import logging
from sqlalchemy import create_engine

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Configurações
INPUT_DIRECTORY = r"C:\Users\gilberto.junior\OneDrive - NTT\Documents\Consult-FinOps\FOCUS\UniE_Focus_data"
OUTPUT_FILE = r"C:\Users\gilberto.junior\OneDrive - NTT\Desktop\Mestrado\FinOps\focus_billing_framework\data\src_parquet\focus_sample_100000.parquet"
DESIRED_COLUMNS = [
    "BilledCost", "BillingPeriodStart", "BillingPeriodEnd", "ConsumedQuantity", "ConsumedUnit", "ProviderName",
    "RegionId", "ResourceName", "ResourceType", "ResourceId",
    "ServiceCategory", "ServiceName", "SubAccountName",
    "tag_application", "tag_environment", "tag_business_unit"  # Adicionadas as novas colunas
]

def validate_directories():
    """Verifica se os diretórios de entrada e saída são válidos."""
    if not os.path.exists(INPUT_DIRECTORY):
        logger.error(f"Diretório de entrada não existe: {INPUT_DIRECTORY}")
        return False
    output_dir = os.path.dirname(OUTPUT_FILE)
    if not os.path.exists(output_dir):
        logger.info(f"Criando diretório de saída: {output_dir}")
        try:
            os.makedirs(output_dir)
        except Exception as e:
            logger.error(f"Erro ao criar diretório de saída: {str(e)}")
            return False
    try:
        with open(os.path.join(output_dir, "test_write.txt"), "w") as f:
            f.write("teste")
        os.remove(os.path.join(output_dir, "test_write.txt"))
    except Exception as e:
        logger.error(f"Sem permissão para escrever em {output_dir}: {str(e)}")
        return False
    return True

def consolidate_parquet_files():
    """Consolida arquivos Parquet do diretório de entrada em um único arquivo."""
    if not validate_directories():
        return False
    
    dataframes = []
    parquet_files = []
    
    # Buscar arquivos Parquet
    for root, _, files in os.walk(INPUT_DIRECTORY):
        for file in files:
            if file.endswith(".parquet"):
                parquet_files.append(os.path.join(root, file))
    
    if not parquet_files:
        logger.error(f"Nenhum arquivo Parquet encontrado em: {INPUT_DIRECTORY}")
        return False
    
    logger.info(f"Encontrados {len(parquet_files)} arquivos Parquet.")
    
    for file_path in parquet_files:
        logger.info(f"Processando arquivo: {file_path}")
        try:
            df = pl.read_parquet(file_path)
            available_columns = [col for col in DESIRED_COLUMNS if col in df.columns]
            if not available_columns:
                logger.warning(f"Nenhuma coluna desejada encontrada em {file_path}. Pulando.")
                continue
            logger.info(f"Colunas disponíveis em {file_path}: {available_columns}")
            df_filtered = df.select(available_columns)
            # Adicionar colunas ausentes com NULL
            for col in DESIRED_COLUMNS:
                if col not in df_filtered.columns:
                    df_filtered = df_filtered.with_columns(pl.lit(None).alias(col))
            df_filtered = df_filtered.select(DESIRED_COLUMNS)
            dataframes.append(df_filtered)
        except Exception as e:
            logger.error(f"Erro ao processar {file_path}: {str(e)}")
            continue
    
    if not dataframes:
        logger.error("Nenhum arquivo Parquet válido processado.")
        return False
    
    logger.info("Concatenando arquivos...")
    try:
        consolidated_df = pl.concat(dataframes, how="vertical")
        logger.info(f"Arquivo consolidado contém {len(consolidated_df)} linhas.")
        logger.info(f"Esquema final: {consolidated_df.schema}")
        consolidated_df.write_parquet(OUTPUT_FILE)
        logger.info(f"Arquivo consolidado salvo em: {OUTPUT_FILE}")
        return True
    except Exception as e:
        logger.error(f"Erro ao concatenar ou salvar o arquivo consolidado: {str(e)}")
        return False

def get_duckdb_connection():
    """Retorna uma conexão DuckDB com a tabela consolidated_billing carregada."""
    if not os.path.exists(OUTPUT_FILE):
        logger.error(f"Arquivo Parquet não encontrado: {OUTPUT_FILE}. Executando consolidação...")
        if not consolidate_parquet_files():
            raise FileNotFoundError(f"Não foi possível criar o arquivo {OUTPUT_FILE}.")
    
    con = duckdb.connect(database=':memory:')
    try:
        # Verificar se a tabela já existe
        con.execute("SHOW TABLES")
        tables = [row[0] for row in con.fetchall()]
        if 'consolidated_billing' not in tables:
            logger.info(f"Carregando {OUTPUT_FILE} no DuckDB...")
            con.execute(f"CREATE TABLE consolidated_billing AS SELECT * FROM '{OUTPUT_FILE}'")
        return con
    except Exception as e:
        logger.error(f"Erro ao carregar tabela no DuckDB: {str(e)}")
        con.close()
        raise

if __name__ == "__main__":
    if not os.path.exists(OUTPUT_FILE):
        logger.info("Arquivo consolidado não existe. Iniciando consolidação...")
        consolidate_parquet_files()
    else:
        logger.info(f"Arquivo consolidado já existe: {OUTPUT_FILE}")