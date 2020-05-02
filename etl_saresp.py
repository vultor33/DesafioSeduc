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
    dados2014 = pd.read_csv(NOMES_SARESP[0], sep=';', encoding='latin-1')
    dados2014['ano'] = 2014
    dados2015 = pd.read_csv(NOMES_SARESP[1], sep=';', encoding='latin-1')
    dados2015['ano'] = 2015
    dados2016 = pd.read_csv(NOMES_SARESP[2], sep=';', encoding='latin-1')
    dados2016['ano'] = 2016
    dados2017 = pd.read_csv(NOMES_SARESP[3], sep=';', encoding='latin-1')
    dados2017['ano'] = 2017
    dados2018 = pd.read_csv(NOMES_SARESP[4], sep=';', encoding='latin-1')
    dados2018['ano'] = 2018
    dados = dados2014.append(dados2015).append(dados2016).append(dados2017).append(dados2018)
    return dados.reset_index(drop=True)

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














