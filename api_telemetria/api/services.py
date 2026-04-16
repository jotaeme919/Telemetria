import csv  # Trabalhar com arquivos CSV
import os  
import uuid  
from decimal import Decimal  # Trabalhar com números
from datetime import datetime  # Trabalhar com datas

from django.conf import settings  # Acessa configurações do Django
from django.core.files.storage import FileSystemStorage  # Salvar arquivos
from django.db import transaction, connection  

from api_telemetria.models import MedicaoVeiculoTemp, Veiculo, Medicao  # Models usados


def executar_procedure_pos_importacao(arquivoid):
    """
    Executa uma procedure no banco após a importação.
    Basicamente, finaliza ou processa os dados importados.
    """
    with connection.cursor() as cursor:
        # Chama a procedure passando o ID do arquivo
        cursor.callproc("processa_arquivo", [arquivoid])


def processar_csv_medicoes(arquivo):
    # Gera um ID único 
    arquivoid = str(uuid.uuid4())

    # Define onde o arquivo vai ser salvo
    pasta_destino = os.path.join(settings.MEDIA_ROOT, "importacoes_medicao")
    os.makedirs(pasta_destino, exist_ok=True)  # Cria a pasta se não existir

    # Define nome do arquivo com ID único
    nome_salvo = f"{arquivoid}_{arquivo.name}"
    fs = FileSystemStorage(location=pasta_destino)

    # Salva o arquivo no servidor
    nome_arquivo_salvo = fs.save(nome_salvo, arquivo)
    caminho_completo = os.path.join(pasta_destino, nome_arquivo_salvo)

    # Contadores e listas auxiliares
    total_linhas_arquivo = 0  
    erros = []  
    linhas_para_inserir = [] 

    # Cache dos dados para evitar consultas repetidas no banco
    veiculos_cache = {v.id: v for v in Veiculo.objects.all()}
    medicoes_cache = {m.id: m for m in Medicao.objects.all()}

    # Abre o arquivo CSV
    with open(caminho_completo, mode="r", encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f, delimiter=';')  

        # Campos obrigatórios no CSV
        campos_esperados = {"veiculoid", "medicaoid", "data", "valor"}

        # Verifica se tem cabeçalho
        if not reader.fieldnames:
            raise Exception("O CSV não possui cabeçalho.")

        # Valida se os campos estão corretos
        if not campos_esperados.issubset(set(reader.fieldnames)):
            raise Exception(
                f"Cabeçalho inválido. Esperado: {list(campos_esperados)}. Recebido: {reader.fieldnames}"
            )

        # Percorre cada linha do CSV
        for numero_linha, row in enumerate(reader, start=2):
            total_linhas_arquivo += 1

            try:
                # Converte IDs para inteiro
                id_veiculo = int(row["veiculoid"])
                id_medicao = int(row["medicaoid"])

                # Busca no cache (mais rápido que no banco)
                veiculo = veiculos_cache.get(id_veiculo)
                if not veiculo:
                    raise Exception(f"Veículo {id_veiculo} não encontrado.")

                medicao = medicoes_cache.get(id_medicao)
                if not medicao:
                    raise Exception(f"Medição {id_medicao} não encontrada.")

                # Converte a data para formato datetime
                data_convertida = datetime.strptime(
                    row["data"].strip(),
                    "%Y-%m-%d %H:%M:%S"
                )

                valor_convertido = Decimal(row["valor"].strip())

                # Adiciona na lista de inserção
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

    # Total de linhas válidas
    total_linhas_validas = len(linhas_para_inserir)

    # Inicia transação 
    with transaction.atomic():
        if linhas_para_inserir:
            
            MedicaoVeiculoTemp.objects.bulk_create(linhas_para_inserir, batch_size=1000)

        # Conta quantas linhas foram inseridas
        total_linhas_importadas = MedicaoVeiculoTemp.objects.filter(
            arquivoid=arquivoid
        ).count()

        # Verifica se inseriu tudo corretamente
        quantidades_conferem = total_linhas_validas == total_linhas_importadas

        if quantidades_conferem:
            # Se deu tudo certo, executa a procedure
            executar_procedure_pos_importacao(arquivoid)
        else:
            # Se deu erro, apaga os dados inseridos
            MedicaoVeiculoTemp.objects.filter(arquivoid=arquivoid).delete()

    # Retorna um resumo da importação
    return {
        "arquivoid": arquivoid,
        "arquivo_salvo": nome_arquivo_salvo,
        "caminho": caminho_completo,
        "total_linhas_arquivo": total_linhas_arquivo,
        "total_linhas_importadas": total_linhas_importadas,
        "quantidades_conferem": total_linhas_arquivo == total_linhas_importadas,
        "erros": erros
    }