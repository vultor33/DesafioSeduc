import pandas as pd
import collections
from etl_idesp import etl as etlidesp

LISTA_ANOS = ['2014', '2015', '2016', '2017', '2018']

ARQUIVO_DIRETORES = 'dados\DIRETORES DE ESCOLA.csv'
ARQUIVO_VICES = 'dados\VICE_DIRETOR.csv'
ARQUIVO_COORDENADORES = 'dados\PROFESSOR_COORDENADOR.csv'


def etl():
    return transformacao()

def foi_gestor(ano):
    return ano == 'SIM'

def extracao():
    diretor = pd.read_csv(ARQUIVO_DIRETORES, sep=';')
    vice = pd.read_csv(ARQUIVO_VICES, sep=';')
    coord = pd.read_csv(ARQUIVO_COORDENADORES, sep=';')
    return [diretor, vice, coord]
    
def limpeza(tabela_extraida, funcao_gestor, df_gestao):
    for _, row in tabela_extraida.iterrows():
        for ANO in LISTA_ANOS:
            if foi_gestor(row.loc[ANO]):
                df_gestao['id_gestao'].append('esc-' + str(row.CD_ESCOLA) + '-ano-' + str(ANO))
                df_gestao['id_escola'].append(row.CD_ESCOLA)
                df_gestao['id_gestor'].append(row.id_interno)
                df_gestao['ano_gestao'].append(ANO)
                df_gestao['diretoria'].append(row.DIRETORIA)
                df_gestao['funcao'].append(funcao_gestor)
    return df_gestao

def transformacao():
    diretor, vice, coordenador = extracao()
    df_gestao = {}
    df_gestao['id_gestao'] = []
    df_gestao['id_escola'] = []
    df_gestao['id_gestor'] = []
    df_gestao['ano_gestao'] = []
    df_gestao['diretoria'] = []
    df_gestao['funcao'] = []
    df_gestao = limpeza(diretor, 'DIRETOR', df_gestao)
    df_gestao = limpeza(vice, 'VICE', df_gestao)
    df_gestao = limpeza(coordenador, 'COORDENADOR', df_gestao)
    # ATENCAO - Descartando linhas incorretamente preenchidas
    df_gestao = pd.DataFrame(df_gestao)
    df_gestao = df_gestao.drop(df_gestao[df_gestao.id_gestor == 'SIM'].index)
    df_gestao = df_gestao.drop(df_gestao[df_gestao.id_gestor == 'NAO'].index)
    df_gestao.id_gestor = df_gestao.id_gestor.astype(int)
    return df_gestao

