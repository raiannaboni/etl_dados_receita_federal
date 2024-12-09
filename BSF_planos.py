'''
1. Criar lista de dic com as informações de cada plano
    1.1. Cidades/cnaes/UF
2. Fazer função para consulta das cidades no banco de dados    
    2.2. Buscar os códigos das cidades no banco de dados
    2.3. Transformar a lista de cidades e cnaes em tuplas e UF em string
        2.3.1. verificar se a tupla com cnaes tem apenas um item , se sim, pegar o primeiro 
3. Fazer a query das empresas
4.     


'''

# Bibliotecas
from datetime import datetime
from dateutil.relativedelta import relativedelta

import pandas as pd 
import numpy as np
import re   
import sqlalchemy
import urllib.parse
import psycopg2

from auxiliar.utils import adicionar_numero_empregados_2021, buscar_dados_plano_bd, conectar_bd, criar_coluna_faixa_capital_social, criar_coluna_numero_empregados, criar_tabela_novas_empresas, get_dados_empresas_com_numeros_socios, get_empresas_bd, get_socios_bd, organizar_ordem_colunas_BI, organizar_ordem_colunas_enviar_sindicato, transformar_dados_empresas
from auxiliar.dados import planos_dic


def preencher_planilha_planos_bsf():
    conexao_bd = conectar_bd()

    for dados_plano in planos_dic:

        cidades_ids, cnae_lista, uf = buscar_dados_plano_bd(dados_plano, conexao_bd)
        empresas_plano = get_empresas_bd(cidades_ids, cnae_lista, uf, conexao_bd)
        socios_empresas_plano = get_socios_bd(cidades_ids, cnae_lista, uf, conexao_bd)
        dados_empresas_com_socios = get_dados_empresas_com_numeros_socios(empresas_plano, socios_empresas_plano)
        df_dados_empresas = transformar_dados_empresas(dados_empresas_com_socios)
        df_dados_empresas = criar_coluna_faixa_capital_social(df_dados_empresas)
        df_dados_empresas = criar_coluna_numero_empregados(df_dados_empresas)
        df_dados_empresas = adicionar_numero_empregados_2021(df_dados_empresas)
        df_dados_empresas_enviar_sindicato = organizar_ordem_colunas_enviar_sindicato(df_dados_empresas, dados_plano)
        df_dados_empresas_BI = organizar_ordem_colunas_BI(df_dados_empresas, dados_plano)
        df_novas_empresas = criar_tabela_novas_empresas(df_dados_empresas_enviar_sindicato, dados_plano)
               




def criar_tabela_mundanca_situacao_cadastral(data, dados_brutos_do_plano):

    data_atual = datetime.now()
    mes_anterior = data_atual - relativedelta(months=1)
    mes_anterior_anterior = data_atual - relativedelta(months=2)
    mes_anterior_formatado = mes_anterior.strftime('%Y-%m')
    mes_anterior_anterior_formatado = mes_anterior_anterior.strftime('%Y-%m')
    mes_ano = data_atual.strftime('%B-%Y')
    
    uf = dados_brutos_do_plano['uf'].lower()
    nome_plano = dados_brutos_do_plano['nome_plano']
    caminho_arquivo_salvo = f'projeto_{uf}_{nome_plano}/BSF_{nome_plano}_novas_empresas_{mes_ano}.xlsx'

    # Abrindo dataset da última atualização
    empresas_passado = pd.read_excel("G:/Meu Drive/nucleo_analytics/BSF/projeto_ba_760/BSF_760_nova_situacao_cadastral_set_24.xlsx",
                                usecols=['cnpj', 'situacao'], dtype={'cnpj': str})

    # Selecionando empresas ativas da atualização passada
    empresas_passado = empresas_passado[empresas_passado['situacao'] == 'Ativa']
    empresas_passado = empresas_passado.drop(columns='situacao')

    # Selecionando empresas não ativas da nova atualização
    empresas_fechadas_novo = data[data['situacao'] != 'Ativa']

    # Encontrando empresas que não estão mais ativas
    recem_fechadas = pd.merge(empresas_passado, empresas_fechadas_novo, how='inner', on='cnpj')

    # Salvando arquivo
    # recem_fechadas.to_excel('G:/Meu Drive/nucleo_analytics/BSF/projeto_ba_760/BSF_760_nova_situacao_cadastral_set_24.xlsx',
    #                         sheet_name='empresas', index=False)






preencher_planilha_planos_bsf()       

