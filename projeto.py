import apache_beam as beam
#import pandas as pd

# Função para converter "Sim" e "Não" em valores numéricos
def convert_preference(preference):
    return 1 if preference == 'Sim' else 0

def preparar_csv(elemento, delimitador=';'):
    """
    Receber uma tupla {'ID_Aluno': '1', 'Nome_Aluno': 'João Silva', 'Nota_Matematica': 8.5, 'Nota_Portugues': 7.0, 'Nota_Ciencias': 9.0, 'Gosta_Frutas': 1, 'Gosta_Vegetais': 0, 'Gosta_Doces': 1}
    Retorna uma string delimitada
    """    
    # Converter o dicionário para texto separado por ';'
    texto_separado = f"{delimitador}".join([str(valor) for valor in elemento.values()])
    return texto_separado
   
# Definindo o pipeline
with beam.Pipeline() as p:
    # Leitura e processamento do dataset de desempenho escolar
    desempenho = (
        p
        | 'Read Desempenho' >> beam.io.ReadFromText('desempenho_escolar.csv', skip_header_lines=1)
        | 'Parse Desempenho' >> beam.Map(lambda x: x.split(';'))
        | 'Map to KV Desempenho' >> beam.Map(lambda x: (x[0], {  # x[0] deve ser o ID_Aluno
            'Nome_Aluno': x[1],
            'Nota_Matematica': float(x[4]),
            'Nota_Portugues': float(x[5]),
            'Nota_Ciencias': float(x[6])
        }))
    )

    preferencias = (
        p
        | 'Read Preferencias' >> beam.io.ReadFromText('preferencias_alimentares.csv', skip_header_lines=1)
        | 'Parse Preferencias' >> beam.Map(lambda x: x.split(';'))
        | 'Map to KV Preferencias' >> beam.Map(lambda x: (x[0], {  # x[0] deve ser o ID_Aluno
            'Gosta_Frutas': convert_preference(x[3]),
            'Gosta_Vegetais': convert_preference(x[4]),
            'Gosta_Doces': convert_preference(x[5])
        }))
    )

    header = 'ID_Aluno;Nome_Aluno;Nota_Matematica;Nota_Portugues;Nota_Ciencias;Gosta_Frutas;Gosta_Vegetais;Gosta_Doces'
    #Integração dos dois datasets
    integrado = (
        {'desempenho': desempenho, 'preferencias': preferencias}
        | 'Merge Datasets' >> beam.CoGroupByKey()
        | 'Filter and Merge' >> beam.Map(lambda x: {
            'ID_Aluno': x[0],
            'Nome_Aluno': x[1]['desempenho'][0]['Nome_Aluno'],
            'Nota_Matematica': x[1]['desempenho'][0]['Nota_Matematica'],
            'Nota_Portugues': x[1]['desempenho'][0]['Nota_Portugues'],
            'Nota_Ciencias': x[1]['desempenho'][0]['Nota_Ciencias'],
            'Gosta_Frutas': x[1]['preferencias'][0]['Gosta_Frutas'],
            'Gosta_Vegetais': x[1]['preferencias'][0]['Gosta_Vegetais'],
            'Gosta_Doces': x[1]['preferencias'][0]['Gosta_Doces'],
        })
        | 'Preparar csv' >> beam.Map(preparar_csv)
        | 'Write Output' >> beam.io.WriteToText('integrated_output', file_name_suffix='.csv', header=header)
        #| "Mostrar resultados" >> beam.Map(print)
    )
#