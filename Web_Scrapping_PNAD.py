# Extração de dados da PNAD através da plataforma Base dos Dados
# --- 1. INSTALANDO E IMPORTANDO BIBLIOTECAS NECESSÁRIAS
import pandas as pd                             
import basedosdados as bd

# --- 2. CONFIGURAÇÃO DOS PARÂMETROS
anos = list(range(1981, 2002))
billing_project_id = "basedosdados-381718"

# --- 3. MONTAGEM DA CONSULTA SQL
query = f"""
SELECT
    ano,
    sigla_uf,
    posicao_ocupacao,
    grupos_ocupacao,
    ocupacao_semana,
    atividade_ramo_negocio_agregado,
    atividade_ramo_negocio_semana,
    renda_mensal_ocupacao_principal,
    renda_mensal_ocupacao_principal_deflacionado,
    renda_mensal_dinheiro,
    renda_mensal_dinheiro_deflacionado,
    horas_trabalhadas_semana,
    horas_trabalhadas_outros_trabalhos,
    horas_trabalhadas_todos_trabalhos,
    grau_frequentado,
    ultimo_grau_frequentado,
    anos_estudo
FROM
    `basedosdados.br_ibge_pnad.microdados_compatibilizados_pessoa`
WHERE
    ano IN ({', '.join(map(str, anos))})
"""

# --- 4. EXECUÇÃO DA CONSULTA E DOWNLOAD DOS DADOS
print("Iniciando o download dos dados da PNAD...")
print("Isso pode levar alguns minutos...")

try:
    df = bd.read_sql(query, billing_project_id=billing_project_id)
    print("Download concluído com sucesso!")
    
    # --- 5. SALVAMENTO DOS DADOS
    arquivo_csv = 'pnad_1981_2001.csv'
    df.to_csv(arquivo_csv, index=False)
    print(f"Dados salvos em: {arquivo_csv}")
    print(f"Total de registros: {len(df):,}")
    print("Processo finalizado!")
    
except Exception as e:
    print(f"Ocorreu um erro: {e}")
    print("Verifique se você está logado na conta Google correta e se o projeto tem o BigQuery API ativado.")