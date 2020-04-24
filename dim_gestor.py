import pandas as pd
import collections
import etl_idesp
import etl_gestores

def dim():
    df_gestao = etl_nota()
    df_gestao = agrupa_id_gestor(df_gestao)
    return df_gestao

def agrupa_id_gestor(df_gestao):
    gestores = {}
    gestores['id_gestor'] = []
    gestores['n_atuacoes'] = []
    gestores['nota_var'] = []
    gestores['escolas_diferentes'] = []
    for nome, dados in df_gestao.groupby('id_gestor'): 
        gestores['id_gestor'].append(nome)
        gestores['n_atuacoes'].append(dados.n_atuacoes.iloc[0])
        gestores['nota_var'].append(dados.nota_var.iloc[0])   
        gestores['escolas_diferentes'].append(len(set(dados.id_escola)))
    return pd.DataFrame(data=gestores)

def etl_nota():
    df_gestao = etl_gestores.etl()
    df_gestao_nota = adiciona_nota(df_gestao)
    return df_gestao_nota

def adiciona_nota(df_gestao):
    df_idesp = etl_idesp.etl()
    mergeidesp = pd.merge(df_idesp, df_gestao, on='id_gestao')
    nota_gestor = {}
    nota_gestor['id_gestor'] = []
    nota_gestor['nota_var'] = []
    nota_gestor['n_atuacoes'] = []
    for nome, dados in mergeidesp.groupby('id_gestor'):
        nota_var = 0
        n_atuacoes = 0
        for id_gestao, id_dados in dados.groupby('id_gestao'):
            nota_var += id_dados.variacao.iloc[0]
            n_atuacoes += 1
        nota_var /= n_atuacoes
        
        nota_gestor['id_gestor'].append(nome)
        nota_gestor['nota_var'].append(nota_var)
        nota_gestor['n_atuacoes'].append(n_atuacoes)
    nota_gestor = pd.DataFrame(data=nota_gestor)
    mergeidesp = pd.merge(df_gestao, nota_gestor, on='id_gestor')
    return mergeidesp
    
    