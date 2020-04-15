import pandas as pd
import collections

CRIMPAR_MAX_VARIACAO = 30 # valor nos 90% maiores
CRIMPAR_MIN_VARIACAO = -13 # valor nos 10% menores
LISTA_ANOS = ['2014', '2015', '2016', '2017', '2018']
LISTA_ANO_ANTERIOR = ['2013', '2014', '2015', '2016', '2017']

def etl():
    df_idesp = extracao()
    df_idesp = limpeza(df_idesp)
    df_idesp = agrupamento_id_gestao(df_idesp)
    return df_idesp

def enulo(valor):
    return str(valor) == 'nan'   

def extracao():
    df_idesp_anosiniciais = pd.read_csv('dados\idesp/IDESP_Escolas_2007_2018_AI.CSV',sep=',')
    df_idesp_anosfinais = pd.read_csv('dados\idesp/IDESP_Escolas_2007_2018_AF.CSV',sep=',')
    df_idesp_ensinomedio = pd.read_csv('dados\idesp/IDESP_Escolas_2007_2018_EM.CSV',sep=',')
    df_idesp = df_idesp_anosiniciais.append(df_idesp_anosfinais).append(df_idesp_ensinomedio)
    return df_idesp

def limpeza(df_idesp):
    idesp = {}
    idesp['id_gestao'] = []
    idesp['id_escola'] = []
    idesp['ano_gestao'] = []
    idesp['nivel_ensino'] = []
    idesp['nota_ano'] = []
    idesp['nota_anterior'] = []

    for _, row in df_idesp.iterrows():
        for ano,ano_anterior in zip(LISTA_ANOS, LISTA_ANO_ANTERIOR):
            if enulo(row.loc[ano]):
                continue
            idesp_anterior = row.loc[ano_anterior]
            if enulo(idesp_anterior):
                idesp_anterior = row.loc[ano]

            idesp['id_gestao'].append('esc-' + str(row.loc['CODIGO CIE']) + '-ano-' + str(ano))
            idesp['id_escola'].append(row.loc['CODIGO CIE'])
            idesp['ano_gestao'].append(ano)
            idesp['nivel_ensino'].append(row.loc['NIVEL ENSINO'])
            idesp['nota_ano'].append(row.loc[ano])
            idesp['nota_anterior'].append(idesp_anterior)
    
    return pd.DataFrame(data=idesp)

def agrupamento_id_gestao(df_idesp):
    idesp_group = {}
    idesp_group['id_gestao'] = []
    idesp_group['id_escola'] = []
    idesp_group['ano_gestao'] = []
    idesp_group['nota_ano'] = []
    idesp_group['variacao'] = []

    for nome, dados in df_idesp.groupby('id_gestao'):
        nota_ano = dados.nota_ano.mean()
        nota_anterior = dados.nota_anterior.mean()
        variacao = int(100 * (nota_ano - nota_anterior) / nota_anterior)
        idesp_group['id_gestao'].append(nome)
        idesp_group['id_escola'].append(dados.id_escola.iloc[0])
        idesp_group['ano_gestao'].append(int(dados.ano_gestao.iloc[0]))
        idesp_group['nota_ano'].append(nota_ano)
        idesp_group['variacao'].append(variacao)
    
    idesp_group = pd.DataFrame(data=idesp_group)
    idesp_group.variacao = idesp_group.variacao.apply(lambda x: CRIMPAR_MAX_VARIACAO if x > CRIMPAR_MAX_VARIACAO else x)
    idesp_group.variacao = idesp_group.variacao.apply(lambda x: CRIMPAR_MIN_VARIACAO if x < CRIMPAR_MIN_VARIACAO else x)
    return idesp_group
    
