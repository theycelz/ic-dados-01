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
    """Baixa o dataset do Kaggle (versão mais recente) e retorna uma lista de arquivos baixados recursivamente."""
    try:
        destino.mkdir(parents=True, exist_ok=True)
        print(f"Baixando dataset: {dataset} para {destino}")
        kaggle_api.dataset_download_files(
            dataset, path=str(destino), unzip=True)

        arquivos = [arq for arq in destino.rglob("*") if arq.is_file()]

        if not arquivos:
            raise ValueError("Nenhum arquivo baixado. ")

        for arquivo in arquivos:
            print(f" Arquivo encontrado: {arquivo}")

        return arquivos

    except Exception as e:
        print(f"Erro ao baixar dataset: {e}")
        return []


def enviar_para_s3(arquivo: Path, bucket: str, base_path: Path) -> None:
    """Envia o arquivo local para o bucket S3, preservando a estrutura de diretórios."""
    try:
        if not arquivo.is_file():
            raise FileNotFoundError(f"{arquivo} não é um arquivo válido.")

        s3_key = f"Bronze/{arquivo.relative_to(base_path)}"

        print(f"Tentando enviar: {arquivo} -> s3://{bucket}/{s3_key}")

        with open(arquivo, "rb") as f:
            s3_client.upload_fileobj(f, bucket, s3_key)

        print(f" Enviado para S3: s3://{bucket}/{s3_key}")

    except Exception as e:
        print(f"Erro ao enviar {arquivo.name} para S3: {e}")


def pipeline_ingestao(dataset: str, destino: Path, bucket: str) -> None:
    """Pipeline completo de ingestão: baixa do Kaggle e envia para o S3."""
    arquivos = baixar_dados_kaggle(dataset, destino)
    if arquivos:

        list(map(partial(enviar_para_s3, bucket=bucket, base_path=destino), arquivos))
        print(" Pipeline finalizado!")
    else:
        print(" Pipeline falhou: nenhum arquivo para processar.")


if __name__ == "__main__":
    pipeline_ingestao(KAGGLE_DATASET, LOCAL_PATH, S3_BUCKET)
