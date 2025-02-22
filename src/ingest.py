import os
import boto3
import kagglehub
from pathlib import Path
from functools import partial

# configs
KAGGLE_DATASET = "theworldbank/education-statistics"
LOCAL_PATH = Path("/tmp/kaggle_data")
S3_BUCKET = "data-lake-ic-eos"

# inicializando cliente s3
s3_client = boto3.client("s3")


def baixar_dados_kaggle(dataset: str, destino: Path) -> list[Path]:
    """baixa o dataset do Kaggle e retorna uma lista de arquivos baixados."""
    destino.mkdir(parents=True, exist_ok=True)
    kagglehub.dataset_download(dataset, path=str(destino))
    return list(destino.glob("*"))


def enviar_para_s3(arquivo: Path, bucket: str) -> None:
    """envia o arquivo local para o bucket S3."""
    s3_client.upload_file(str(arquivo), bucket, f"raw/{arquivo.name}")
    print(f"✅ Enviado para S3: s3://{bucket}/raw/{arquivo.name}")


def pipeline_ingestao(dataset: str, destino: Path, bucket: str) -> None:
    """pipeline completo de ingestão: baixa do Kaggle e envia para o S3."""
    arquivos = baixar_dados_kaggle(dataset, destino)
    list(map(partial(enviar_para_s3, bucket=bucket), arquivos))
    print("🚀 Pipeline finalizado!")


if __name__ == "__main__":
    pipeline_ingestao(KAGGLE_DATASET, LOCAL_PATH, S3_BUCKET)
