import pandas as pd
import collections
from util import gera_id_gestao

LISTA_ANOS = ['2014', '2015', '2016', '2017', '2018']
LISTA_ANO_ANTERIOR = ['2013', '2014', '2015', '2016', '2017']
NOME_IDESP_AI = 'dados\IDESP_Escolas_2007_2018_AI.CSV'
NOME_IDESP_AF = 'dados\IDESP_Escolas_2007_2018_AF.CSV'
NOME_IDESP_EM = 'dados\IDESP_Escolas_2007_2018_EM.CSV'


def etl():
    ''' Funcao que extrai, limpa e consolida os dados disponibilizados do IDESP
    em uma tabela com base no nosso grao: id_gestao (escola+ano).'''
    
    df_idesp = extracao()
    df_idesp = limpeza(df_idesp)
    df_idesp = agrupamento_id_gestao(df_idesp)
    return df_idesp

def extracao():
    '''Extracao dos dados a partir das tabelas disponibilizadas no contexto
    do desafio SEDUC
    SAIDA
    * df_idesp: uniao dos dados do IDESP'''
    
    df_idesp_anosiniciais = pd.read_csv(NOME_IDESP_AI, sep=',')
    df_idesp_anosfinais = pd.read_csv(NOME_IDESP_AF, sep=',')
    df_idesp_ensinomedio = pd.read_csv(NOME_IDESP_EM, sep=',')
    df_idesp = df_idesp_anosiniciais.append(df_idesp_anosfinais).append(df_idesp_ensinomedio)
    return df_idesp

def limpeza(df_idesp):
    '''Na tabela df_idesp os anos de um mesmo nivel de ensino
    e escola ficam na mesma linha. Precisamos que cada resultado
    do IDESP esteja em uma linha separada. Adicionalmente, e necessario
    descartar anos onde a prova nao foi aplicada.'''

    idesp = {}
    idesp['id_gestao'] = []
    idesp['id_escola'] = []
    idesp['ano_gestao'] = []
    idesp['nivel_ensino'] = []
    idesp['nota_ano'] = []
    idesp['nota_anterior'] = []
    idesp['anterior_nulo'] = []
    idesp['atual_nulo'] = []

    df_idesp = df_idesp.dropna(thresh=2)  # DESCARTE DE 1 LINHA VAZIA
    for _, row in df_idesp.iterrows():
        for ano, ano_anterior in zip(LISTA_ANOS, LISTA_ANO_ANTERIOR):
            atual_nulo = 0
            idesp_atual = row.loc[ano]
            if enulo(idesp_atual):
                atual_nulo = 1
                idesp_atual = 0
            idesp_anterior = row.loc[ano_anterior]
            anterior_nulo = 0
            if enulo(idesp_anterior):
                anterior_nulo = 1
                idesp_anterior = idesp_atual

            idesp['id_gestao'].append(gera_id_gestao(row.loc['CODIGO CIE'], ano))
            idesp['id_escola'].append(int(row.loc['CODIGO CIE']))
            idesp['ano_gestao'].append(int(ano))
            idesp['nivel_ensino'].append(row.loc['NIVEL ENSINO'])
            idesp['nota_ano'].append(idesp_atual)
            idesp['nota_anterior'].append(idesp_anterior)
            idesp['anterior_nulo'].append(anterior_nulo)
            idesp['atual_nulo'].append(atual_nulo)
    return pd.DataFrame(data=idesp)
    
    
def agrupamento_id_gestao(df_idesp):
    '''O IDESP se divide em 3 fases - Anos iniciais, anos finais e ensino medio.
    Nem todas as escolas passam por essas 3 etapas e, adicionalmente, existem anos
    onde ha falhas nas aplicacoes das provas. 
    Nesta funcao, consolidamos os resultados do idesp em um indicador unico com base
    no que definimos como nosso grao: id_gestao (escola+ano).
    ENTRADA
    * df_idesp: dados do IDESP limpos.
    SAIDA
    * df_idesp: dados do IDESP consolidados por id_gestao.'''
    
    idesp_group = {}
    idesp_group['id_gestao'] = []
    idesp_group['id_escola'] = []
    idesp_group['ano_gestao'] = []
    idesp_group['nota_ano'] = []
    idesp_group['variacao'] = []
    idesp_group['anterior_nulo'] = []
    idesp_group['atual_nulo'] = []

    for nome, dados in df_idesp.groupby('id_gestao'):
        atual_nulo = atual_enulo(dados)
        if atual_nulo == 2:
            nota_ano = None
            nota_anterior = None
            variacao = None
        else:
            if atual_nulo == 1:
                dados = dados[dados.atual_nulo == 0]
            nota_ano = dados.nota_ano.mean()
            nota_anterior = dados.nota_anterior.mean()
            variacao = 100 * (nota_ano - nota_anterior) / nota_anterior
        
        idesp_group['id_gestao'].append(nome)
        idesp_group['id_escola'].append(dados.id_escola.iloc[0])
        idesp_group['ano_gestao'].append(int(dados.ano_gestao.iloc[0]))
        idesp_group['nota_ano'].append(nota_ano)
        idesp_group['variacao'].append(variacao)
        idesp_group['anterior_nulo'].append(int(dados.anterior_nulo.sum() > 0))
        idesp_group['atual_nulo'].append(atual_nulo)
    
    idesp_group = pd.DataFrame(data=idesp_group)
    return idesp_group
    
    
def enulo(valor):
    '''TODO - fortalecer essa conferencia de nulo'''
    
    return str(valor) == 'nan'

def atual_enulo(dados):
    '''Verifca se a prova idesp foi aplicada no ano da gestao
    ENTRADA
    * dados: dados da tabela IDESP, limpos e agrupados pelo id_gestao
    SAIDA
    * atual_nulo: 0 se todas as provas foram aplicadas, 1 se a aplicacao foi parcial
                  2 se nenhuma das provas foi aplicada.'''

    soma_atual = dados.atual_nulo.sum()
    if soma_atual == 0:
        return 0
    elif soma_atual == len(dados):
        return 2
    else:
        return 1
