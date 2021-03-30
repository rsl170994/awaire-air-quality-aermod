# -*- coding: utf-8 -*-
import mysql.connector
import os
import shutil
import requests
from datetime import datetime


caminho = '/home/sodarec/data_sender/sodar_data/'
destino = '/home/sodarec/data_sender/sodar_data/inseridos/'
url = 'http://awaire-air-quality/api/v1/sodar_observations'
get_url = 'http://awaire-air-quality/api/v1/sodar_observations/last_date'
entries = os.listdir(caminho)


#--------------------------------------------------------------------------------------------------
#SOLICITANDO A ULTIMA DATA_HORA ARMAZENADA NO BANCO
data_armazenada = requests.get(get_url)
data_armazenada = data_armazenada.text
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#REALIZANDO A FORMATAÇÃO DA DATA_HORA RECEBIDA
data_armazenada = datetime.strptime(str(data_armazenada), '"%Y-%m-%dT%H:%M:%S.000Z"')
data_armazenada.strftime('%Y-%m-%d %H:%M:%S')
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#INFORMANDO A DATA_HORA ARMAZENADA NO BANCO NA FORMATAÇÃO CORRETA
data_armazenada = str(data_armazenada)
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#LISTANDO AS ENTRADAS NO DIRETÓRIO
for entry in entries:
#--------------------------------------------------------------------------------------------------



	#--------------------------------------------------------------------------------------------------
	#SELECIONANDO APENAS AS ENTRADAS TERMINAS EM '.mnd'
	if entry.endswith(".mnd"):
	#--------------------------------------------------------------------------------------------------
	


		#--------------------------------------------------------------------------------------------------	
		#ABRINDO ARQUIVO E LENDO LINHAS
		arquivo = open(caminho+entry).readlines()
		#--------------------------------------------------------------------------------------------------



		#--------------------------------------------------------------------------------------------------
		#INFORMANDO ARQUIVO QUE ESTA SENDO LIDO LIDO
		print ("Lendo arquivo:" + entry)
		#--------------------------------------------------------------------------------------------------



		#--------------------------------------------------------------------------------------------------
		#BUNSCANDO OS NOMES DAS VARIÁVEIS
		for a in range(len(arquivo)):
			linha = arquivo[a].split()
			if ("#PG" in linha):
				variaveis = "data" + arquivo[a+2] + arquivo[a]
				variaveis = variaveis.replace('#','').replace('*','_estrela').replace('{','').replace('}','_ass').replace('^','')
				variaveis = variaveis.split()
				break 
		#--------------------------------------------------------------------------------------------------



		#--------------------------------------------------------------------------------------------------
		#LENDO OS DADOS POR ALTURA DAQUELA DATA_HORA
		for b in range(len(arquivo)):
			linha = arquivo[b].split()
			if ("#PG" in linha):
				



				#--------------------------------------------------------------------------------------------------
				#BUSCANDO A DATA_HORA TRABALHADA
				contador_linha = b + 3 #INFORMANDO O DESLOCAMENTO DENTRO DO ARQUIVO PARA BUSCAR A DATA_HORA
				data_hora = arquivo[b-1].split() #INFORMANDO O DESLOCAMENTO DENTRO DO ARQUIVO PARA BUSCAR A DATA_HORA
				data_hora = data_hora[0] + ' ' + data_hora[1]
				#--------------------------------------------------------------------------------------------------



				#--------------------------------------------------------------------------------------------------
				#VERIFICANDO SE A DATA MAIS RECENTE DO BANCO É ANTERIOR A DA LEITURA
				if data_armazenada < data_hora:
					#--------------------------------------------------------------------------------------------------
					#BUSCANDO OS DADOS DE NÃO PERFIL
					dados_nao_perfil = arquivo[b+1].split() #INFORMANDO O DESLOCAMENTO DENTRO DO ARQUIVO PARA BUSCAR OS DADOS DE NÃO PERFIL
					while ((arquivo[contador_linha][0] != '\r')):
						dados_perfil = arquivo[contador_linha].split()
						#--------------------------------------------------------------------------------------------------
						#INFORMANDO A CONDIÇÃOD E PARADA - UMA QUEBRA DE LINHA CORRESPONDE AO FINAL DOS DADOS DE DATERMINADA HORA
						if arquivo[contador_linha][0] == '\n':
							break
						#--------------------------------------------------------------------------------------------------



						#--------------------------------------------------------------------------------------------------
						#COLOCANDO O VALOR DE CADA VARIÁVEL EM SEU LOCAL
						dic = {'sodar_id': 1, 'date': data_hora, 'z': dados_perfil[0], 'speed': dados_perfil[1], 'dir': dados_perfil[2], 'u_geo': dados_perfil[3], 'v_geo': dados_perfil[4], 'u': dados_perfil[5], 'v': dados_perfil[6], 'w': dados_perfil[7], 'sig_u': dados_perfil[8], 'sig_w': dados_perfil[12], 'shear': dados_perfil[23], 'shear_dir': dados_perfil[24], 'sig_theta': dados_perfil[28], 'ti': dados_perfil[29], 'pgz': dados_perfil[30], 'tke': dados_perfil[31], 'edr': dados_perfil[32], 'bck_raw': dados_perfil[33], 'bck': dados_perfil[34], 'bck_id': dados_perfil[35],  'error': dados_perfil[37], 'pg': dados_nao_perfil[0], 'h_range': dados_nao_perfil[1], 'h_inv': dados_nao_perfil[2], 'h_mixing': dados_nao_perfil[3], 'h': dados_nao_perfil[4], 'u_star': dados_nao_perfil[5], 'l_star': dados_nao_perfil[6]}
						#--------------------------------------------------------------------------------------------------



						#--------------------------------------------------------------------------------------------------
						#JOGANDO OS DADOS PARA O BANCO
						x = requests.post(url, data = dic)
						print(x.text)
						#--------------------------------------------------------------------------------------------------



						#--------------------------------------------------------------------------------------------------
						#INFORMANDO A CONDIÇÃOD E PARADAD - O TAMANHO DO ARQUIVO DETERMINA O FINAL DOS DADOS
						if contador_linha == len(arquivo):
							break
						#--------------------------------------------------------------------------------------------------
						contador_linha = contador_linha + 1
				else:
					continue
				#--------------------------------------------------------------------------------------------------
		#--------------------------------------------------------------------------------------------------
	#--------------------------------------------------------------------------------------------------
		#MOVENDO O ARQUIVO LIDO PARA OUTRA PASTA
		#shutil.move(str(caminho) + str(entry),str(destino) + str(entry))
#--------------------------------------------------------------------------------------------------


