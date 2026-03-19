import csv
import os
import uuid
from decimal import Decimal
from datetime import datetime

from django.conf import settings
from django.core.files.storage import FileSystemStorage
from django.db import transaction

from api_tele.models import MedicaoVeiculoTemp, Veiculo, Medicao


def processar_csv_medicoes(arquivo):
    arquivoid = str(uuid.uuid4())

    pasta_destino = os.path.join(settings.MEDIA_ROOT, "importacoes_medicao")
    os.makedirs(pasta_destino, exist_ok=True)

    nome_salvo = f"{arquivoid}_{arquivo.name}"
    fs = FileSystemStorage(location=pasta_destino)
    nome_arquivo_salvo = fs.save(nome_salvo, arquivo)
    caminho_completo = os.path.join(pasta_destino, nome_arquivo_salvo)

    total_linhas_arquivo = 0
    erros = []
    linhas_para_inserir = []

    veiculos_cache = {v.id: v for v in Veiculo.objects.all()}
    medicoes_cache = {m.id: m for m in Medicao.objects.all()}

    with open(caminho_completo, mode="r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=';')

        campos_esperados = {"veiculoid", "medicaoid", "data", "valor"}

        if not reader.fieldnames:
            raise Exception("O CSV não possui cabeçalho.")

        if not campos_esperados.issubset(set(reader.fieldnames)):
            raise Exception(
                f"Cabeçalho inválido. Esperado: {list(campos_esperados)}. Recebido: {reader.fieldnames}"
            )

        for numero_linha, row in enumerate(reader, start=2):
            total_linhas_arquivo += 1

            try:
                id_veiculo = int(row["veiculoid"])
                id_medicao = int(row["medicaoid"])

                veiculo = veiculos_cache.get(id_veiculo)
                if not veiculo:
                    raise Exception(f"Veículo {id_veiculo} não encontrado.")

                medicao = medicoes_cache.get(id_medicao)
                if not medicao:
                    raise Exception(f"Medição {id_medicao} não encontrada.")

                data_convertida = datetime.strptime(
                    row["data"].strip(),
                    "%Y-%m-%d %H:%M:%S"
                )

                valor_convertido = Decimal(row["valor"].strip())

                linhas_para_inserir.append(
                    MedicaoVeiculoTemp(
                        veiculoid=veiculo,
                        medicaoid=medicao,
                        data=data_convertida,
                        valor=valor_convertido,
                        arquivoid=arquivoid
                    )
                )

            except Exception as e:
                erros.append({
                    "linha": numero_linha,
                    "erro": str(e)
                })

    with transaction.atomic():
        MedicaoVeiculoTemp.objects.bulk_create(linhas_para_inserir, batch_size=1000)

    total_linhas_importadas = MedicaoVeiculoTemp.objects.filter(
        arquivoid=arquivoid
    ).count()

    return {
        "arquivoid": arquivoid,
        "arquivo_salvo": nome_arquivo_salvo,
        "caminho": caminho_completo,
        "total_linhas_arquivo": total_linhas_arquivo,
        "total_linhas_importadas": total_linhas_importadas,
        "quantidades_conferem": total_linhas_arquivo == total_linhas_importadas,
        "erros": erros
    }