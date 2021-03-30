import os
from datetime import datetime
import requests
import json
import time
import sys

#--------------------------------------------------------------------------------------------------
#INFORMANDO AS URL'S
url = 'http://awaire-air-quality/api/v1/aermod_outputs'
get_url = 'http://awaire-air-quality/api/v1/sodar_observations/last_date'
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#INFORMANDO O LOCAL DOS ARQUIVOS
caminho = '/home/sodarec/data_sender/pst_data/'
destiino = '/home/sodarec/data_sender/pst_data/inseridos/'
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#INFORMANDO O NOME DAS ENTRADAS NO DIRETÓRIO caminho
entradas = os.listdir(caminho)
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#INFORMANDO O ARRAY AUXILIAR
dados = []
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#SOLICITANDO A ULTIMA DATA_HORA ARMAZENADA NO BANCO
data_armazenada = requests.get(get_url)
data_armazenada = data_armazenada.text
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#REALIZANDO A FORMATAÇÃO DA DATA_HORA RECEBIDA E BUSCANDO SOMENTE O VALOR DA HORA CHEIA
hora_armazenada = datetime.strptime(str(data_armazenada), '"%Y-%m-%dT%H:%M:%S.000Z"')
hora_armazenada = hora_armazenada.strftime('%H')
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#ACESSANDO OS ARQUIVOS DO DIRETÓRIO caminho
for entrada in entradas:
#--------------------------------------------------------------------------------------------------



	#--------------------------------------------------------------------------------------------------
	#ACESSANDO OS ARQUIVOS .PST DO DIRETÓRIO caminho
	if entrada.endswith(".PST"):
	#--------------------------------------------------------------------------------------------------



		#--------------------------------------------------------------------------------------------------
		#LENDO ARQUIVOS .PST
		arquivo = open(caminho + entrada).readlines()
		#INÍCIO DA LEITURA DO ARQUIVO
		for i in range(len(arquivo)):



			#--------------------------------------------------------------------------------------------------
			#PULANDO O CABEÇALHO DO ARQUIVO
			linha = arquivo[i].split()
			if ('*' in arquivo[i][0]):
				continue
			#--------------------------------------------------------------------------------------------------



			#--------------------------------------------------------------------------------------------------
			#DETERMINANDO A DATA_HORA
			ano = '20' + linha[8][:2]
			mes = linha[8][2:4]
			dia = linha[8][4:6]
			hora = linha[8][6:8]
			data_hora = ano + '-' + mes +'-' + dia + ' ' + hora + ':00:00'
			#--------------------------------------------------------------------------------------------------



			#--------------------------------------------------------------------------------------------------
			#ADICIONANDO AO ARRAY dados INFORMAÇÕES REALACIONADAS A ULTIMA DATA HORA PRESENTE NO BANCO DE DADOS DE INFORMAÇOES DO SODAR
			if hora <= hora_armazenada:
				dic = {'x':round(float(linha[0])), 'y': round(float(linha[1])), 'concentration':linha[2], 'date_predicted':data_hora, 'aermod_input_id':3}
				dados.append(dic)
			#--------------------------------------------------------------------------------------------------
		#--------------------------------------------------------------------------------------------------
	#--------------------------------------------------------------------------------------------------
#--------------------------------------------------------------------------------------------------




#print(dados)
#--------------------------------------------------------------------------------------------------
#FAZENDO UM POST COM TODOS OS DADOS - EX: [{'x': -7000, 'y': -7000, 'concentration': '0.00775', 'date_predicted': '20012612', 'aermod_input_id':1}, {'x': -6500, 'y': -7000, 'concentration': '0.00754', 'date_predicted': '20012612', 'aermod_input_id':1}, ...]
print('Inserção de dados iniciada.')
headers = {'content-type': 'application/json'}
x = requests.post(url, data = json.dumps(dados), headers=headers)
print(x.text)
#--------------------------------------------------------------------------------------------------
# print(sys.getsizeof(json.dumps(dados)))