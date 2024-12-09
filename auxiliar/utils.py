


# Bibliotecas
import pandas as pd 
import numpy as np
import re   
import sqlalchemy
import urllib.parse
import psycopg2
from datetime import datetime
from dateutil.relativedelta import relativedelta


def conectar_bd():
    # Conexão com o banco de dados
    engine = sqlalchemy.create_engine(
                f'postgresql+psycopg2://postgres:n0v4s3mpr3s4s@195.35.18.55:5432/receita_federal')
    conn = engine.connect()
    return conn

def buscar_dados_plano_bd(dados_planos, conexao_bd):
    # Query para buscar os códigos das cidades no bd
    cidades = str(dados_planos['cidades'])

    cidades_query = f"""
        SELECT * 
        FROM municipios 
        WHERE descricao IN {cidades};
    """

    cid = pd.read_sql_query(cidades_query, con=conexao_bd)

    cidades_ids = str(tuple(list(cid['id'].astype(str))))

    # Transformar cnaes em tuplas
    cnaes = dados_planos['cnaes']

    if type(cnaes) == str:            
        cnae_lista =  (cnaes,)
        cnae_lista = f"('{cnae_lista[0]}')"
    else:
        cnae_lista = cnaes
    
    # UF
    uf = dados_planos['uf']

    return cidades_ids, cnae_lista, uf

def get_empresas_bd(cidades_ids, cnae_lista, uf, conn):
      
    query_data = f"""

            SELECT e.*, em.*, c.descricao, nj.descricao, m.descricao, s.*, q.descricao, p.descricao 
            FROM estabelecimentos e
                    LEFT JOIN cnaes c ON e.cnae_fiscal = c.id
                    LEFT JOIN empresas em ON e.cnpj_basico = em.cnpj_basico
                    LEFT JOIN naturezas_juridicas nj ON em.natureza_juridica = nj.id
                    LEFT JOIN municipios m ON e.cod_municipio = m.id
                    LEFT JOIN simples s ON e.cnpj_basico = s.cnpj_basico
                    LEFT JOIN qualificacoes q ON em.qualif_resp = q.id 
                    LEFT JOIN paises p ON e.cod_pais = p.id 
            WHERE  e.cnae_fiscal IN {cnae_lista} AND
                    e.cod_municipio IN {cidades_ids} AND
                    e.uf = '{uf}'
            
            """

    response = pd.read_sql_query(query_data, con=conn)
    
    response.columns = ['cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'matriz_filial',
       'nome_fantasia', 'situacao', 'data_situacao', 'motivo_situacao',
       'nm_cidade_exterior', 'cod_pais', 'data_inicio_ativ', 'cnae_fiscal',
       'cnae_fiscal_secundaria', 'tipo_logradouro', 'logradouro', 'numero',
       'complemento', 'bairro', 'cep', 'uf', 'cod_municipio', 'ddd_1',
       'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax', 'num_fax', 'email',
       'sit_especial', 'data_sit_especial', 'cnpj', 'cnpj_basico2',
       'razao_social', 'natureza_juridica', 'qualif_resp', 'capital_social',
       'porte', 'ente_federativo', 'descricao2', 'descricao3', 'descricao4',
       'cnpj_basico3', 'opcao_simples', 'data_opcao', 'data_exclusao',
       'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei', 'descricao5',
       'descricao6']

    return response

def get_socios_bd(cidades_ids, cnae_lista, uf, conn):
      
    query_data_socios = f"""

            SELECT e.cnpj_basico, s.*, q.descricao
            FROM estabelecimentos e
                LEFT JOIN socios s ON e.cnpj_basico = s.cnpj_basico
                LEFT JOIN qualificacoes q ON s.qualificacao_socio = q.id 
            WHERE e.cnae_fiscal IN {cnae_lista} AND
                e.cod_municipio IN {cidades_ids} AND
                e.uf = '{uf}'

                """

    response_socios = pd.read_sql_query(query_data_socios, con=conn)
    
    response_socios.columns = ['cnpj_basico', 'cnpj', 'cnpj_basico2', 'tipo_socio', 'nome_socio',
       'cnpj_cpf_socio', 'qualificacao_socio', 'data_entrada', 'cod_pais',
       'cpf_representante_legal', 'nome_representante_legal',
       'qualificacao_rep_legal', 'faixa_etaria', 'descricao']

    return response_socios

def get_dados_empresas_com_numeros_socios(dados_empresas, dados_socios):
    
    # Adicionando dados de sócios
    
    data_socios_gr = dados_socios[['cnpj_basico', 'nome_socio']].groupby('cnpj_basico').size().reset_index()

    data_socios_gr = pd.merge(data_socios_gr, dados_socios, 
                              how='left', on='cnpj_basico').drop_duplicates(subset='cnpj_basico').reset_index(drop=True)

    data_socios_gr = data_socios_gr.rename(columns={0: 'qtd_socios'})

    data_socios_gr = data_socios_gr.drop(columns='cnpj_basico2')

    df_dados_empresas = pd.merge(dados_empresas, data_socios_gr, how='left', on='cnpj_basico')
    
    return df_dados_empresas

def transformar_dados_empresas(data):
    # Abrindo arquivo de motivos da situação cadastral
    motivo = pd.read_excel('G:/Meu Drive/nucleo_analytics/banco_de_dados/motivo_situacao.xlsx')

    # Porte
    data['porte'] = data['porte'].apply(lambda x: x.title())

    # Simples
    data['opcao_simples'] = data['opcao_simples'].apply(lambda x: 'Sim' if x == 'S' else 
                                                       'Não' if x == 'N' else x)

    # Matriz e filial
    data['matriz_filial'] = data['matriz_filial'].apply(lambda x: x.title())

    # Situacao cadastral
    data['motivo_situacao'] = data.motivo_situacao.apply(lambda x: int(x))
    data = pd.merge(data, motivo, how='left', on='motivo_situacao')

    data['situacao'] = data['situacao'].apply(lambda x: x.title())
    
    # Criando coluna de MEI
    data['mei'] = data['razao_social'].str.contains(r'.\d{11}|...\d{3}.\d{3}', regex=True)
    data['mei'] = data['mei'].apply(lambda x: 'Sim' if x == True else 'Não')

    # Colunas
    data = data.drop(['motivo_situacao', 'cod_municipio', 
                   'natureza_juridica', 'cnpj_basico2', 'cnpj_basico3', 'qualificacao_socio'], axis=1)

    data = data.rename(columns={'descricao2': 'cnae', 'descricao3': 'natureza_juridica', 'descricao4': 'municipio',
                           'descricao5': 'qualificacao_responsavel', 
                             'descricao6': 'descricao_socio',
                           'descricao': 'motivo_situacao', 'cnpj_x': 'cnpj', 'descricao_x': 'qualificacao_socio',
                               'descricao_y': 'motivo_situacao'})

    # Município
    data['municipio'] = data['municipio'].apply(lambda x: x.title())


    # Datas
    data['data_inicio_ativ'] = pd.to_datetime(data.data_inicio_ativ)
    
    # Dtypes
    data['cep'] = data['cep'].astype(str)

    return data

def criar_coluna_faixa_capital_social(data):
    # Faixa de capital social:
    data['capital_social'] = data.capital_social.apply(lambda x: float(x))
    data['capital_social'] = data.capital_social.apply(lambda x: int(x))
    
    data['faixa_capital_social'] = data['capital_social'].apply(lambda x: 'Não informado' if np.isnan(x) else
                             'Até R$10 mil' if x <= 10000 else
                             'R$10 mil - 20 mil' if (x > 10000) & (x <= 20000) else
                             'R$20 mil - 30 mil' if (x > 20000) & (x <= 30000) else
                             'R$30 mil - 40 mil' if (x > 30000) & (x <= 40000) else
                             'R$40 mil - 50 mil' if (x > 40000) & (x <= 50000) else
                             'R$50 mil - 100 mil' if (x > 50000) & (x <= 100000) else
                             'R$100 mil - R$1 mi' if (x > 100000) & (x <= 1000000) else
                             'R$1 mi - R$10 mi' if (x > 1000000) & (x <= 10000000) else
                             'R$10 mi - R$100 mi' if (x > 10000000) & (x <= 100000000) else
                             'R$100 mi - R$1 bi' if (x > 100000000) & (x <= 1000000000) else
                             'R$1 bi - R$10 bi' if (x > 1000000000) & (x <= 10000000000) else
                             'Acima de R$10 bi')
    
    data['ordenando_capital_social'] = data.faixa_capital_social.apply(lambda x:
                             1 if x == 'Até R$10 mil' else
                             2 if x == 'R$10 mil - 20 mil' else
                             3 if x == 'R$20 mil - 30 mil' else
                             4 if x == 'R$30 mil - 40 mil' else
                             5 if x == 'R$40 mil - 50 mil' else
                             6 if x == 'R$50 mil - 100 mil' else
                             7 if x == 'R$100 mil - R$1 mi' else
                             8 if x == 'R$1 mi - R$10 mi' else
                             9 if x == 'R$10 mi - R$100 mi' else
                             10 if x == 'R$100 mi - R$1 bi' else
                             11 if x == 'R$1 bi - R$10 bi' else
                             12 if x == 'Acima de R$10 bi' else 13)

    return data

def criar_coluna_numero_empregados(data):
    # Número de empregados a partir do porte da empresa:
    data['empregados_porte'] = ''

    
    for i in range(len(data)):

        nao_eh_mei = data.loc[i, 'mei'] == 'Não'
        eh_microempresa = data.loc[i, 'porte'] == 'Micro Empresa'
        eh_pequeno_porte = data.loc[i, 'porte'] == 'Pequeno Porte'
        eh_demais = data.loc[i, 'porte'] == 'Demais'

        if nao_eh_mei:            
            if eh_microempresa:
                data.loc[i, 'empregados_porte_BI'] = '01 a 09 (Micro Empresa)'
            elif eh_pequeno_porte:
                data.loc[i, 'empregados_porte_BI'] = '10 a 49 (Pequeno porte)'
            elif eh_demais:
                data.loc[i, 'empregados_porte_BI'] = 'acima de 50 (Demais)'                
        else:            
            data.loc[i, 'empregados_porte_BI'] = '00 (MEI)'
            
    return data

def adicionar_numero_empregados_2021(data):
    
    # Abrindo arquivos rais
    rais = pd.read_csv('G:/Meu Drive/nucleo_analytics/banco_de_dados/rais_2021_CNPJ_cleaned.csv', 
                   usecols=['cnpj_raiz', 'cnpj', 'qtd_vinculos_ativos'], 
                   dtype={'cnpj_raiz': str, 'cnpj': str, 'qtd_vinculos_ativos': 'int64'})
    
    # Merge entre cnpj e rais para selecionar apenas as empresas de interesse para que eu possa agrupar os cnpj raiz e somar empregados

    data_cnpj = data[['cnpj']]

    data_rais = pd.merge(data_cnpj, rais, how='left', on='cnpj').drop_duplicates(subset='cnpj').reset_index(drop=True)

    receita_rais = pd.merge(data, data_rais[['cnpj', 'qtd_vinculos_ativos']], how='left', on='cnpj')


    # Coluna com número de empregados estimado e do rais juntos para a tabela do BI:
    receita_rais['empregados_both_BI'] = 'Não disponível'

    for i in range(len(receita_rais)):

        if pd.isna(receita_rais.loc[i, 'qtd_vinculos_ativos']) :
            receita_rais.loc[i, 'empregados_both_BI'] = receita_rais.loc[i, 'empregados_porte_BI']

        else:
            receita_rais.loc[i, 'empregados_both_BI'] = receita_rais.loc[i, 'qtd_vinculos_ativos']


    # Renomeando colunas
    receita_rais = receita_rais.rename(columns={'qtd_vinculos_ativos': 'numero_empregados_rais_2021', 
                                                'empregados_porte': 'numero_empregados_por_porte'})


    return receita_rais

def organizar_ordem_colunas_enviar_sindicato(data, dados_brutos_do_plano):
    data_empresas = data[[
            'cnpj', 'nome_fantasia', 'razao_social', 'mei', 'data_inicio_ativ', 'situacao', 'motivo_situacao',
            'capital_social', 'cnae', 'cnae_fiscal', 'cnae_fiscal_secundaria',
            'natureza_juridica', 'matriz_filial', 'porte', 'empregados_porte_BI',
            'numero_empregados_rais_2021', 'municipio', 'tipo_logradouro', 'logradouro', 'numero', 'complemento', 'bairro',
            'cep', 'uf', 'ddd_1', 'telefone_1', 'ddd_2', 'telefone_2', 'ddd_fax',
            'num_fax', 'email', 'data_situacao', 'opcao_simples', 'data_opcao',
            'data_exclusao', 'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei', 'qualificacao_responsavel', 
            'cnpj_basico', 'cnpj_ordem', 'cnpj_dv', 'nm_cidade_exterior',
            'sit_especial', 'data_sit_especial', 'qtd_socios', 'tipo_socio',
            'nome_socio', 'cnpj_cpf_socio', 'qualificacao_socio', 'data_entrada',
            'cpf_representante_legal', 'nome_representante_legal',
            'qualificacao_rep_legal', 'faixa_etaria'
            ]]
    
    data_atual = datetime.now()
    mes_ano = data_atual.strftime('%B-%Y')

    uf = dados_brutos_do_plano['uf'].lower()
    nome_plano = dados_brutos_do_plano['nome_plano']

    caminho_arquivo_salvo = f'projeto_{uf}_{nome_plano}/BSF_{nome_plano}_dados_receita_rais_{mes_ano}.xlsx'

    # Salvando o arquivo
    data_empresas.to_excel(f'G:/Meu Drive/nucleo_analytics/BSF/{caminho_arquivo_salvo}',
                                sheet_name='empresas', index=False)
    
    return data_empresas

def organizar_ordem_colunas_BI(data, dados_brutos_do_plano):
    data_empresas_BI = data[[
            'cnpj', 'nome_fantasia', 'razao_social', 'mei', 'data_inicio_ativ', 'situacao',
            'capital_social', 'cnae', 'cnae_fiscal', 'cnae_fiscal_secundaria',
            'natureza_juridica', 'matriz_filial', 'porte',
            'numero_empregados_rais_2021', 'faixa_capital_social',
            'numero_empregados_por_porte', 'empregados_both_BI',
            'empregados_porte_BI', 'municipio', 'tipo_logradouro', 'logradouro', 'numero',
            'complemento', 'bairro', 'uf', 'cep', 'email', 'telefone_1',
            'telefone_2', 'data_situacao', 'opcao_simples', 'data_opcao',
            'data_exclusao', 'opcao_mei', 'data_opcao_mei', 'data_exclusao_mei',
            'qtd_socios', 'nome_socio'
                ]]
    
    data_empresas_BI = data_empresas_BI[data_empresas_BI['situacao'] == 'Ativa']

    uf = dados_brutos_do_plano['uf'].lower()
    nome_plano = dados_brutos_do_plano['nome_plano']

    caminho_arquivo_salvo = f'projeto_{uf}_{nome_plano}/BSF_{nome_plano}_dados_receita_rais_BI.xlsx'

    # Salvando o arquivo
    data_empresas_BI.to_excel('G:/Meu Drive/nucleo_analytics/BSF/{caminho_arquivo_salvo}',
                                sheet_name='empresas', index=False)
    
    return data_empresas_BI


def criar_tabela_novas_empresas(data, dados_brutos_do_plano):
    
    data['data_inicio_ativ'] = pd.to_datetime(data['data_inicio_ativ'])

    data_atual = datetime.now()
    mes_anterior = data_atual - relativedelta(months=1)
    mes_anterior_anterior = data_atual - relativedelta(months=2)
    mes_anterior_formatado = mes_anterior.strftime('%Y-%m')
    mes_anterior_anterior_formatado = mes_anterior_anterior.strftime('%Y-%m')
    mes_ano = data_atual.strftime('%B-%Y')
    
    novas_empresas = data[(data['data_inicio_ativ'] >= f'{mes_anterior_anterior_formatado}-15') & 
                        (data['data_inicio_ativ'] <= f'{mes_anterior_formatado}-15')].reset_index(drop=True)
    
    novas_empresas['data_inicio_ativ'] = pd.to_datetime(novas_empresas['data_inicio_ativ']).dt.strftime('%d-%m-%Y')
    
    uf = dados_brutos_do_plano['uf'].lower()
    nome_plano = dados_brutos_do_plano['nome_plano']

    caminho_arquivo_salvo = f'projeto_{uf}_{nome_plano}/BSF_{nome_plano}_novas_empresas_{mes_ano}.xlsx'
    
    # Salvando arquivo
    novas_empresas.to_excel(f'G:/Meu Drive/nucleo_analytics/BSF/{caminho_arquivo_salvo}', 
                             sheet_name='empresas', index=False)
    
    return novas_empresas