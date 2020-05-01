import pandas as pd
import collections

import etl_idesp
from util import gera_id_gestao

NOMES_SARESP = [
    'dados/SARESP_escolas_2014.csv',
    'dados/SARESP_escolas_2015.csv',
    'dados/SARESP_escolas_2016.csv',
    'dados/SARESP_escolas_2017.csv',
    'dados/SARESP_escolas_2018.csv']

ANOS_SARESP = [2014, 2015, 2016, 2017, 2018]


def etl():
    dados_saresp = extracao()
    dados_saresp = transformacao(dados_saresp)
    return dados_saresp

def extracao():
    dados_saresp = []
    for arquivo, ano in zip(NOMES_SARESP, ANOS_SARESP):
        dados = pd.read_csv(arquivo, sep=';', encoding='latin-1')
        dados['ano'] = ano
        dados_saresp.append(dados)
    saresp = dados_saresp[0]
    for i in range(1,len(dados_saresp)):
        saresp.append(dados_saresp[1])
    return saresp.copy()
    

def transformacao(dados_saresp):
    dados_saresp.loc[:,'medprof'] = dados_saresp.medprof.apply(para_numero)
    dados_saresp = dados_saresp.dropna(subset=['medprof']).copy()
    dados_saresp.loc[:,'medprof'] = dados_saresp.medprof.astype(float)
    id_gestao = []
    for i in dados_saresp.index:
        id_gestao.append(gera_id_gestao(dados_saresp.loc[i,'CODESC'],dados_saresp.loc[i,'ano']))
    dados_saresp['id_gestao'] = id_gestao

    saresp = {}
    saresp['id_gestao'] = []
    saresp['id_escola'] = []
    saresp['ano_gestao'] = []
    saresp['nota_saresp'] = []
    for nome, dados in dados_saresp.groupby('id_gestao'):
        saresp['id_gestao'].append(nome)
        saresp['id_escola'].append(dados.CODESC.iloc[0])
        saresp['ano_gestao'].append(dados.ano.iloc[0])
        saresp['nota_saresp'].append(dados.medprof.median())
    return pd.DataFrame(saresp)


def para_numero(num_entrada):
    num_entrada = str(num_entrada)
    num_entrada = num_entrada.replace('.','')
    num_entrada = num_entrada.replace(',','.')
    try:
        num_entrada = float(num_entrada)
        return num_entrada
    except:
        return None














