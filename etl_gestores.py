import pandas as pd
import collections
from etl_idesp import etl as etlidesp

LISTA_ANOS = ['2014', '2015', '2016', '2017', '2018']


def etl():
    return transformacao()

def etl_nota():
    df_gestao = etl()
    df_gestao_nota = adiciona_nota(df_gestao)
    return df_gestao_nota

def adiciona_nota(df_gestao):
    df_idesp = etlidesp()
    mergeidesp = pd.merge(df_idesp, df_gestao, on='id_gestao')
    nota_gestor = {}
    nota_gestor['id_gestor'] = []
    nota_gestor['nota_var'] = []
    nota_gestor['n_atuacoes'] = []
    for nome, dados in mergeidesp.groupby('id_gestor'):
        nota_gestor['id_gestor'].append(nome)
        nota_gestor['nota_var'].append(dados.variacao.mean())
        nota_gestor['n_atuacoes'].append(len(dados))
    nota_gestor = pd.DataFrame(data=nota_gestor)
    mergeidesp = pd.merge(df_gestao, nota_gestor, on='id_gestor')
    return mergeidesp
    
def foi_gestor(ano):
    return ano == 'SIM'

def extracao():
    arquivo = 'dados\lideres\lideres_DIRETORES DE ESCOLA.csv'
    diretor = pd.read_csv(arquivo, sep=';')
    arquivo = 'dados\lideres\lideres_VICE_DIRETOR.csv'
    vice = pd.read_csv(arquivo, sep=';')
    arquivo = 'dados\lideres\lideres_PROFESSOR_COORDENADOR.csv'
    coord = pd.read_csv(arquivo, sep=';')
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

