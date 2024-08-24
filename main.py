import re
import apache_beam as beam
from apache_beam.io import ReadFromText
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions

pipeline_options = PipelineOptions(argv = None)
pipeline = beam.Pipeline(options=pipeline_options)

colunas_escolar = [

    'ID_Aluno','Nome_Aluno','Idade','Turma','Nota_Matematica','Nota_Portugues','Nota_Ciencias'

]

colunas_alimentar = [

    'ID_Pessoa','Nome_Pessoa','Idade','Gosta_Frutas','Gosta_Vegetais','Gosta_Doces'


]

def lista_para_dicionario(elemento, colunas):
    """
    Recebe 2 listas
    Retorna 1 dicionário
    """
    return dict(zip(colunas,elemento))

def texto_para_lista(elemento, delimitador=';'):
    """
    Recebe um texto e um delimitador
    Retorna uma lista de elementos pelo delimitador
    """
    return elemento.split(delimitador)

def chave_id(elemento, elechave):
    """
    Rebece um dicionário 
    Retorna uma tupla  com o id_aluno e o elemento (id_aluno, dicionário)
    """
    chave = elemento[elechave]
    return (chave, elemento)

def descompactar_elementos(elemento):
    """
    Receber uma tupla ('1', {'ID_Aluno': '1', 'Nome_Aluno': 'João Silva', 'Idade': '10', 'Turma': '4A', 'Nota_Matematica': '8.5', 'Nota_Portugues': '7.0', 'Nota_Ciencias': '9.0'})
    Retornar uma tupla ('5', 'Lucas Alves')
    """
    chave, dados = elemento
    nome = dados['Nome_Aluno']
    idade = dados['Idade']
    result = f'{chave}-{nome}'
    return str(result), str(idade)

def descompactar_elementos_alimentar(elemento):
    """
    Receber uma tupla ('1', {'ID_Pessoa': '1', 'Nome_Pessoa': 'João Silva', 'Idade': '25', 'Gosta_Frutas': 'Sim', 'Gosta_Vegetais': 'Não', 'Gosta_Doces': 'Sim'})
    Retornar uma tupla ('5', 'Lucas Alves')
    """
    chave, dados = elemento
    nome = dados['Nome_Pessoa']
    fruta = dados['Gosta_Frutas']
    vegetal = dados['Gosta_Vegetais']
    doce = dados['Gosta_Doces']
    result = f'{chave}-{nome}'
    res = f'{fruta}-{vegetal}-{doce}'
    return str(result), str(res)

def descompactar_elementos_result(elemento):
    """
    Receber uma tupla ('5-Lucas Alves', {'escola': ['10'], 'alimento': ['Não-Não-Não']})
    Retornar uma tupla ('5','Lucas Alves','10','Não','Não','Não')
    """
    chave, dados = elemento
    id, nome = chave.split('-')
    idade = dados['escola'][0]
    alimento = dados['alimento'][0]
    fruta, vegetal, doce = alimento.split('-')
    return id, nome, str(idade), fruta, vegetal, doce

def preparar_csv(elemento, delimitador=';'):
    """
    Receber uma tupla ('5', 'Lucas Alves', '10', 'Não', 'Não', 'Não')
    Retorna uma string delimitada
    """    
    return f"{delimitador}".join(elemento)

escolar = (
   pipeline
   | "Leitura do dataset de desempenho escolar" >> ReadFromText('desempenho_escolar.csv', skip_header_lines=1)
   | "De texto para lista" >> beam.Map(texto_para_lista)
   | "Lista para dicionário" >> beam.Map(lista_para_dicionario,colunas_escolar)
   | "Criar chave pelo id_aluno" >> beam.Map(chave_id,'ID_Aluno')
   | 'Descompactar elementos' >> beam.Map(descompactar_elementos)
   #| "Mostrar resultados" >> beam.Map(print)
)

alimentar = (
    pipeline
    | "Leitura do dataset de preferencias alimentares" >> ReadFromText('preferencias_alimentares.csv', skip_header_lines=1)
    | "De texto para lista (pref. alimentares)" >> beam.Map(texto_para_lista)
    | "Lista para dicionário alimentar" >> beam.Map(lista_para_dicionario,colunas_alimentar)
    | "Criando a chave id_pessoa" >> beam.Map(chave_id,'ID_Pessoa')
    | 'Descompactar elementos alimentares' >> beam.Map(descompactar_elementos_alimentar)
    #| "Soma dos casos pela chave UF-ANO-MES" >> beam.CombinePerKey(sum)
    #| "Arredondar resultados de chuvas " >> beam.Map(arredonda)
    #| "Mostrar resultados de pref. alimentares" >> beam.Map(print)
)

resultado = (
    # (escolar, alimentar)
    # | "Empilha as pcols" >> beam.Flatten()
    # | "Agrupo as pcols" >> beam.GroupByKey()
    ({'escola':escolar, 'alimento':alimentar})
    | 'Mesclar pcols' >> beam.CoGroupByKey()
    | 'Descompactar elementos do resultado' >> beam.Map(descompactar_elementos_result)
    | 'Preparar csv' >> beam.Map(preparar_csv)
    #| "Mostrar resultados da união" >> beam.Map(print)
)


header = 'ID;NOME;IDADE;GOSTA_FRUTAS;GOSTA_VEGETAIS;GOSTA_DOCES'
resultado | 'Criar arquivo CSV' >> WriteToText('resultado', file_name_suffix='.csv', header=header)

pipeline.run()