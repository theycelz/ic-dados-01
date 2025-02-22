import os
import boto3
from pathlib import Path
from functools import partial
from kaggle.api.kaggle_api_extended import KaggleApi

# Configs
KAGGLE_DATASET = "theworldbank/education-statistics"
LOCAL_PATH = Path("/tmp/kaggle_data")
S3_BUCKET = "data-lake-ic-eos"

# Inicializando cliente S3 e Kaggle API
s3_client = boto3.client("s3")
kaggle_api = KaggleApi()
kaggle_api.authenticate()


def baixar_dados_kaggle(dataset: str, destino: Path) -> list[Path]:
    """baixa o dataset do Kaggle (versão mais recente) e retorna uma lista de arquivos baixados."""
    try:
        destino.mkdir(parents=True, exist_ok=True)
        print(f"baixando dataset: {dataset} para {destino}")
        kaggle_api.dataset_download_files(
            dataset, path=str(destino), unzip=True)
        arquivos = list(destino.glob("*"))
        if not arquivos:
            raise ValueError("nenhum arquivo baixado. dataset inexistente")
        return arquivos
    except Exception as e:
        print(f"erro ao baixar dataset: {e}")
        return []


def enviar_para_s3(arquivo: Path, bucket: str) -> None:
    """envia o arquivo local para o bucket S3."""
    try:
        s3_client.upload_file(str(arquivo), bucket, f"raw/{arquivo.name}")
        print(f"enviado para S3: s3://{bucket}/raw/{arquivo.name}")
    except Exception as e:
        print(f"erro ao enviar {arquivo.name} para S3: {e}")


def pipeline_ingestao(dataset: str, destino: Path, bucket: str) -> None:
    """pipeline completo de ingestão: baixa do kaggle e envia para o S3."""
    arquivos = baixar_dados_kaggle(dataset, destino)
    if arquivos:
        list(map(partial(enviar_para_s3, bucket=bucket), arquivos))
        print("pipeline finalizado!")
    else:
        print("pipeline falhou: nenhum arquivo para processar.")


if __name__ == "__main__":
    pipeline_ingestao(KAGGLE_DATASET, LOCAL_PATH, S3_BUCKET)
