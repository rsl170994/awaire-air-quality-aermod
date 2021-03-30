# -*- coding: utf-8 -*-
import mysql.connector
import os
import numpy as np
import pandas as pd
import datetime
#--------------------------------------------------------------------------------------------------
#INFORMANDO O LOCAL DO ARQUIVO - NÃO É UTILIZADO ATUALMENTE PARA NADA DENTRO DO PROGRAMA
caminho = 'G:/Meu Drive/data_getter/'
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
x = '2021-03-10 23:00:00' #datetime.datetime.now() RETIRAR A DATA ENTRE PARÊNTESES E COLOCAR A PARTE COMENTADA
# x = sys.argv[1]  #PEGENDO A DATA HORA INFORMADA PELO LINHA DE COMANDO. A DATA HORA DEVE SER INFORMADA ENTRE ''. EX: '2020-09-30 23:00:00'
x = datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S') #APAGAR A LINHA NA HORA DE IMPLEMENTAR
x = x.timetuple()
ano = x.tm_year
mes = x.tm_mon
dia = x.tm_mday
hora = x.tm_hour
DATA_HORA_REF = ("'" + str(ano) + '-' + str(mes) + '-' + str(dia) + ' ' + str(hora) + ":00:00'")
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#AUXILIAR PARA INSERÇÃO DOS DADOS DA EMS
aux_cont_2 = 0
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#INFORMANDO OS VALORES PARA DADOS AUSENTES NA LEITURA DO BANCO
#SIGTHETA
ausente_theta = [999.9, None]
#SIGW
ausente_w = [99.99, None]
#TEMPERATURA
ausente_t = [999.99, None]
#DIREÇÃO
ausente_dir = [999.9, None]
#VELOCIDADE
ausente_speed = [99.99, None]
#H_MIXING
ausente_h_mixing = [99999, None]
#SOLAR_RADIATION
ausente_solar_rad = [999.99, None]
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#INFORMANDO VALORES PARA DADOS AUSENTES NA PRODUÇÃO DO ARQUIVO DE SAÍDA
saida_hmixing = 9999
saida_rad = 9999
saida_theta = 99
saida_w = 99
saida_temp = 99
saida_dir = 99
saida_speed = 99
saida_speed_ems = 99
saida_dir_ems = 99
saida_temp_ems = 99
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#DEFININDO A POSIÇÃO DAS VARIÁVEIS NOS ARRAYS
#SODAR
posicao_diahora_sodar = 0
posicao_z_sodar = 1
posicao_sigtheta_sodar = 2
posicao_sigw_sodar = 3
posicao_t_sodar = 4
posicao_dir_sodar = 5
posicao_speed_sodar = 6
posicao_hmixing_sodar = 7
#RADIOSSONDA
posicao_diahora_radio = 0
posicao_z_radio = 1
posicao_t_radio = 2
posicao_dir_radio = 3
posicao_speed_radio = 4
posicao_baro_radio = 5
#EMS
posicao_diahora_ems = 0
posicao_solar_ems = 1
posicao_speed_ems = 2
posicao_dir_ems = 3
posicao_t_ems = 4
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#INFORMANDO AS ALTURAS TRABALHADAS NO SODAR
z_sodar = [[*range(40,211,10)],[*range(220,351,10)],[*range(400,601,50)]]
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#INFORMANDO OS VALORES BARÔMETRO
baro = [[1000], [975], [950]]
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#OS VALORES NO ARRAY z_sodar EM CADA POSIÇÃO, OU SEJA, z_sodar[0], z_sodar[1] E z_sodar[2], 
#ESTÃO ASSOCIADOS AS POSIÇÕES DO ARRAY baro, OU SEJA, z_sodar[0] ESTA PARA baro[0], ETC
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#QUNATIDADE DE HORAS NECESSÁRIAS PARA FORMAR O ARQUIVO
quant_h = 24
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#QUANTIDADE DE ALTURAS TRRABALHADAS NO SODAR (z) - VALOR CONHECIDO A PARTIR DO TAMANHO DO ARRAY alturas - len(alturas)
alturas = []
for m in range(len(z_sodar)):
	for n in range(len(z_sodar[m])):
		alturas.append(z_sodar[m][n])
z = len(alturas)
#--------------------------------------------------------------------------------------------------


#--------------------------------------------------------------------------------------------------
#INFORMANDO O NOME DO ARQUIVO GERADO
arquivo = 'SODAR.DAT'
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#FUNÇÃO PARA BUSCAR DADOS NO BANCO DE DADOS(SODAR)
def dados_sodar(DATA_HORA_REF_SODAR):
	#CONECTANDO AO BANCO DE DADOS
	mydb = mysql.connector.connect(
	host='awaire-air-quality-homologation.c7oeuhtcpoh8.eu-central-1.rds.amazonaws.com',
	user='awaire_air_quality',
	passwd='2fc971b5bf7831e050fdfe3e5ce5e1f2a4',
	database='awaire_air_quality_homologation'
	)

	try:
			#INFORMANDO A QUERY A SER EXECUTADA
			query = """SELECT
			date,
			z,
			sig_theta,
			sig_w,
			t,
			dir,
			speed,
			h_mixing
			FROM awaire_air_quality_homologation.sodar_observations
			WHERE date BETWEEN (""" + DATA_HORA_REF_SODAR + """ - INTERVAL 23 HOUR) AND (""" + DATA_HORA_REF_SODAR + """)
			AND ((z BETWEEN 40 AND 350)	OR (z = 400 OR z = 450 OR z = 500 OR z = 550 OR z = 600))
			AND (MINUTE(date) = 0) ORDER BY date ASC;"""
			
			print('Buscando dados do sodar em relação a data_hora: ' + DATA_HORA_REF_SODAR)

			#EXECUTANDO A QUERY INFORMADA
			mycursor = mydb.cursor()
			mycursor.execute(query)
			entradas = mycursor.fetchall()
			aux = list(entradas)
			return(entradas)


	except mysql.connector.Error as error:
		print("Erro ao solicitar dados ao banco de dados: {}".format(error))
		mycursor.close()

#ADICIONANDO DADOS DA FUNÇÃO 'dados_sodar' A UM ARRAY AUXILIAR
aux_sodar = dados_sodar(DATA_HORA_REF)
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#FUNÇÃO PARA BUSCAR DADOS NO BANCO DE DADOS(RADIOSONDA)
def dados_radiossonda(DATA_HORA_REF_SONDAGEM):
	#CONECTANDO AO BANCO DE DADOS
		mydb_1 = mysql.connector.connect(
		host='awaire-air-quality-homologation.c7oeuhtcpoh8.eu-central-1.rds.amazonaws.com',
		user='awaire_air_quality',
		passwd='2fc971b5bf7831e050fdfe3e5ce5e1f2a4',
		database='awaire_air_quality_homologation'
		)


		try:
				#INFORMANDO A QUERY A SER EXECUTADA
				query = """SELECT
				date,
				z,
				temperature,
				direction,
				speed,
				baro
				FROM awaire_air_quality_homologation.gfs_data
				WHERE date = (""" + DATA_HORA_REF_SONDAGEM + """ - INTERVAL 23 HOUR) AND (""" + DATA_HORA_REF_SONDAGEM + """) AND (baro = '1000' OR baro = '975' OR baro = '950')
				ORDER BY date ASC;"""

				print('Buscando dados da radiossonda em relação a data_hora: ' + DATA_HORA_REF_SONDAGEM)
				
				#EXECUTANDO A QUERY INFORMADA
				mycursor = mydb_1.cursor()
				mycursor.execute(query)
				entradas = mycursor.fetchall()
				aux = list(entradas)
				return(entradas)


		except mysql.connector.Error as error:
			print("Erro ao solicitar dados ao banco de dados: {}".format(error))
			mycursor.close()

#ADICIONANDO DADOS DA FUNÇÃO 'dados_radiossonda' A UM ARRAY AUXILIAR
aux_radiossonda = dados_radiossonda(DATA_HORA_REF)
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#ELABORANDO A QUERY A SER EXECUTADA NA FUNÇÃO dados_ems_ternium
query = """SELECT
	date,
	solar_radiation,
	wind_speed,
	wind_direction,
	temperature
	FROM awaire_air_quality_homologation.sms_llwas_observations
	WHERE ("""

for hr in range(24):
	#AJUSTANDO A DATA_HORA
	DATA_HORA_REF = ("'" + str(ano) + '-' + str(mes) + '-' + str(dia) + ' ' + str(hora-hr) + ":02:00'")
	#COLOCANDO EM UMA STRING AUXILIAR
	aux = 'date = (' + DATA_HORA_REF + ') OR '
	#ACRESCENTANDO A STRING A QUERY A SER EXECUTADA	
	query = query + str(aux) 
#REMOVENDO OS ULTIMOS 4 CARACTERES DA QUERY A SER EXECUTADA
query = query[:-4]
#ACRESCENTANDO MAIS CONDIÇÕES A QUERY
query = query + ") AND sms_llwas_id = 1 ORDER BY date ASC;"
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#FUNÇÃO PARA BUSCAR DADOS NO BANCO DE DADOS(EMS_TERNIUM)
def dados_ems_ternium(DATA_HORA_REF_EMS_TERNIUM):
	#CONECTANDO AO BANCO DE DADOS
		mydb_2 = mysql.connector.connect(
		host='awaire-air-quality-homologation.c7oeuhtcpoh8.eu-central-1.rds.amazonaws.com',
		user='awaire_air_quality',
		passwd='2fc971b5bf7831e050fdfe3e5ce5e1f2a4',
		database='awaire_air_quality_homologation'
		)		
		
		try:
			print('Buscando dados da EMS.')
			#EXECUTANDO A QUERY INFORMADA
			mycursor = mydb_2.cursor()
			mycursor.execute(query)
			entradas = mycursor.fetchall()
			aux = list(entradas)
			return(entradas)


		except mysql.connector.Error as error:
			print("Erro ao solicitar dados ao banco de dados: {}".format(error))
			mycursor.close()
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#ADICIONANDO DADOS DA FUNÇÃO 'dados_ems_ternium' A UM ARRAY AUXILIAR
aux_ems = dados_ems_ternium(query)
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#ADICIONANDO AS DATAS LIDAS DO BANCO DA DADOS A UM ARRAY AUXILIAR
datas_lidas = []
for a in range(len(aux_sodar)):
	if aux_sodar[a][posicao_diahora_sodar] in datas_lidas:
		continue
	datas_lidas.append(aux_sodar[a][posicao_diahora_sodar])
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#ADICIONANDO AS DATAS NECESSÁRIAS PARA FORMAÇÃO DO ARQUIVO sodar.dat A UM ARRAY AUXILIAR
data_need = []
for b in range(0,quant_h):
	data_hora_inicial = datetime.datetime(ano,mes,dia,hora,00,00) - datetime.timedelta(hours=b)
	for c in range(0,z):
		data_need.append([data_hora_inicial, alturas[c]])
data_need.sort()
#--------------------------------------------------------------------------------------------------


#------------------------------------------------------------------------------------
#ADICIONANDO OS DADOS DO BANCO DE DADOS E EVENTUAIS DATA_HORAS AUSENTES A UM ARRAY AUXILIAR - REFERENTE A EMS
for g in range(len(data_need)):
	#CHECANDO A AUSENCIA DE DATAS E HORÁRIOS NECESSÁRIOS, CASO AUSENTE, ACRESCENTA UMA LINHA COM A DATA_HORA AUSENTE
	if data_need[g][0] + datetime.timedelta(minutes = 2, seconds = 00) not in (aux_ems[h][0] for h in range(len(aux_ems))):
		aux_ems.append((data_need[g][posicao_diahora_ems] + datetime.timedelta(minutes = 2, seconds = 00), None, None, None, None))
#ORGANIZANDO O ARRAY
aux_ems = sorted(aux_ems)
#------------------------------------------------------------------------------------



#------------------------------------------------------------------------------------
#ADICIONANDO OS DADOS DO BANCO DE DADOS E EVENTUAIS DATA_HORAS AUSENTES A UM ARRAY AUXILIAR - REFERENTE A RADIOSSONDA
for i in range(len(data_need)):
	#CHECANDO A AUSENCIA DE DATAS E HORÁRIOS NECESSÁRIOS, CASO AUSENTE, ACRESCENTA UMA LINHA COM A DATA_HORA AUSENTE
	if data_need[i][0] not in (aux_radiossonda[j][posicao_diahora_radio] for j in range(len(aux_radiossonda))):
		aux_radiossonda.append((data_need[i][0], None, None, None, None, None))
#ORGANIZANDO O ARRAY
aux_radiossonda = sorted(aux_radiossonda)
#------------------------------------------------------------------------------------


#--------------------------------------------------------------------------------------------------
#ADICIONANDO OS DADOS DO BANCO DE DADOS E EVENTUAIS DATA_HORAS AUSENTES A UM ARRAY AUXILIAR
aux_sodar_2 = []
contador = 0


for d in range(len(data_need)):
	# print(contador)
	if contador <= (len(aux_sodar)-1):
		if (data_need[d][0] == aux_sodar[contador][posicao_diahora_sodar]) and (data_need[d][1] == aux_sodar[contador][posicao_z_sodar]): 
			# print(aux_sodar[contador])
			# print(aux_sodar[contador][posicao_diahora_sodar])
			# print(aux_sodar[contador][posicao_z_sodar])
			# print(contador)
			aux_sodar_2.append(aux_sodar[contador])
			contador = contador + 1
		else:
			
			# print(data_need[d][0])
			# print(data_need[d][1])
			aux_sodar_2.append((data_need[d][0], data_need[d][1], None, None, None, None, None, None))
	else:	
		aux_sodar_2.append((data_need[d][0], data_need[d][1], None, None, None, None, None, None))
		# print(data_need[d][0])
		# print(data_need[d][1])	

#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#RENOMEANDO ARRAY AUXILIAR
aux_sodar = aux_sodar_2
aux_sodar.sort()
#--------------------------------------------------------------------------------------------------



#--------------------------------------------------------------------------------------------------
#INFORMANDO O VALOR DO AUXILIAR
aux_cont = 0
#INFORMANDO O ARRAY DE CONTROLE
datas_armazenadas = []
arq = open(arquivo, 'w')
for e in range(len(aux_sodar)):
	#--------------------------------------------------------------------------------------------------
	#INFORMANDO A POSIÇÃO DA DATA_HORA NO ARRAY
	dia_hora = aux_sodar[e][posicao_diahora_sodar]
	#--------------------------------------------------------------------------------------------------



	#--------------------------------------------------------------------------------------------------
	#INFORMANDO A CONDIÇÃO DE PARADA
	if dia_hora in datas_armazenadas:
		continue
	#--------------------------------------------------------------------------------------------------
	


	#--------------------------------------------------------------------------------------------------
	#ADICIONANDO A DATA HORA TRABALHADA NO ARRAY DE CONTROLE
	datas_armazenadas.append(dia_hora)
	timetuple = aux_sodar[e][posicao_diahora_sodar].timetuple()
	#INFORMANDO O DIA DA LEITURA
	dia = timetuple.tm_mday
	#INFORMANDO O MES DA LEITURA
	mes = timetuple.tm_mon
	#INFORMANDO O ANO DA LEITURA
	ano = timetuple.tm_year
	# #AJUSTANDO O TAMANHO DO ARRAY ano
	ano = str(ano)
	ano = int(ano[2:])
	#INFORMANDO A HORA DA LEITURA
	hora = timetuple.tm_hour
	#INFORMANDO O MINUTO DA LEITURA
	minuto = timetuple.tm_min
	#ORGANIZANDO O CABEÇALHO DA DATA_HORA
	DATA_HORA_ARQUIVO = '{:2.0f}'.format(dia) + '{:3.0f}'.format(mes) + '{:3.0f}'.format(ano) + '{:3.0f}'.format(hora)
	#--------------------------------------------------------------------------------------------------
	
	

	#--------------------------------------------------------------------------------------------------
	#DEFININDO VALORES PARA DADOS DE NÃO PERFIL
	for f in range(e,len(aux_sodar)):
		if str(dia_hora) in str(aux_sodar[f][posicao_diahora_sodar]):
			#--------------------------------------------------------------------------------------------------
			#DEFININDO UM VALOR PARA H_MIXING - SODAR
			if (aux_sodar[f][posicao_hmixing_sodar] in ausente_h_mixing):
				h_mixing = saida_hmixing #DEFININDO VALOR AUSENTE PARA H_MIXING, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA RADIOSSONDA
			else:
				h_mixing = aux_sodar[f][posicao_hmixing_sodar] #VALOR DE H_MIXING ATUALIZADO COM VALORES DO SODAR
			#--------------------------------------------------------------------------------------------------



	#--------------------------------------------------------------------------------------------------			
	#DEFININDO UM VALOR PARA SOLAR_RADIATION - EMS
	for g in range(len(aux_ems)):
		if str(dia_hora + datetime.timedelta(minutes = 2, seconds = 00)) in str(aux_ems[g][posicao_diahora_ems]):
			if aux_ems[g][posicao_solar_ems] in ausente_solar_rad:
				solar_rad = saida_rad #INDICADOR DE VALOR AUSENTE
			else:
				solar_rad = aux_ems[g][posicao_solar_ems] #VALOR DE SOLAR_RADIATION ATUALIZADO COM DADOS DA EMS
	#--------------------------------------------------------------------------------------------------


	
	#--------------------------------------------------------------------------------------------------
	#ESCREVENDO INFORMAÇÕES DE DATA HORA, H_MIXING E SOLAR RADIATION NO ARQUIVO
	arq.write(DATA_HORA_ARQUIVO + f'{h_mixing:7.1f}' + f'{solar_rad:6.1f}' + '\n')
	aux_cont = aux_cont + 1
	#--------------------------------------------------------------------------------------------------


	#--------------------------------------------------------------------------------------------------
	#DEFININDO VALORES PARA VARIÁVEIS - SODAR
	for h in range(e,len(aux_sodar)):
		if str(dia_hora) in str(aux_sodar[h][posicao_diahora_sodar]):
			#--------------------------------------------------------------------------------------------------
			#DEFININDO O VALOR PARA SIGMA THETA
			if (aux_sodar[h][posicao_sigtheta_sodar] in ausente_theta):
				sig_theta = saida_theta #INDICADOR DE VALOR AUSENTE
			else:
				sig_theta = aux_sodar[h][posicao_sigtheta_sodar]
			#--------------------------------------------------------------------------------------------------



			#--------------------------------------------------------------------------------------------------
			#DEFININDO UM VALOR PARA SIGMA W
			if (aux_sodar[h][posicao_sigw_sodar] in ausente_w):
				sig_w = saida_w #INDICADOR DE VALOR AUSENTE
			else:
				sig_w = aux_sodar[h][posicao_sigw_sodar]
			#--------------------------------------------------------------------------------------------------



			#--------------------------------------------------------------------------------------------------
			#DEFININDO UM VALOR PARA TEMPERATURA
			#DEFININDO UM VALOR PARA TEMPERATURA NA ALTURA DE 40, 10, 200 M
			if (aux_sodar[h][posicao_t_sodar] in ausente_t) and (aux_sodar[h][posicao_z_sodar] in z_sodar[0]):
				temperatura = saida_temp #DEFININDO VALOR AUSENTE PARA TEMPERATURA, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA RADIOSSONDA
				for i in range(len(aux_radiossonda)):
					if (aux_sodar[h][posicao_diahora_sodar] == aux_radiossonda[i][posicao_diahora_radio]) and (aux_radiossonda[i][posicao_baro_radio] in baro[0]):
						temperatura = aux_radiossonda[i][2] #VALOR DE TEMPERATURA ATUALIZADO COM VALORES DA RADIOSSONDA

			#DEFININDO UM VALOR PARA TEMPERATURA NA ALTURA DE 300, 400 M
			elif (aux_sodar[h][posicao_t_sodar] in ausente_t) and (aux_sodar[h][posicao_z_sodar] in z_sodar[1]):
				temperatura = saida_temp #DEFININDO VALOR AUSENTE PARA TEMPERATURA, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA RADIOSSONDA
				for j in range(len(aux_radiossonda)):
					if (aux_sodar[h][posicao_diahora_sodar] == aux_radiossonda[j][posicao_diahora_radio]) and (aux_radiossonda[j][posicao_baro_radio] in baro[1]):
						temperatura = aux_radiossonda[j][2] #VALOR DE TEMPERATURA ATUALIZADO COM VALORES DA RADIOSSONDA

			#DEFININDO UM VALOR PARA TEMPERATURA NA ALTURA DE 500 M
			elif (aux_sodar[h][posicao_t_sodar] in ausente_t) and (aux_sodar[h][posicao_z_sodar] in z_sodar[2]): 
				temperatura = saida_temp #DEFININDO VALOR AUSENTE PARA TEMPERATURA, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA RADIOSSONDA
				for l in range(len(aux_radiossonda)):
					if (aux_sodar[h][posicao_diahora_sodar] == aux_radiossonda[l][posicao_diahora_radio]) and (aux_radiossonda[l][posicao_baro_radio] in baro[2]):
						temperatura = aux_radiossonda[l][2] #VALOR DE TEMPERATURA ATUALIZADO COM VALORES DA RADIOSSONDA

			#DEFININDO VALOR DE TEMPERATURA CASO SODAR APRESENTE INFORMAÇÕES, ISSO VALE PARA TODAS AS ALTURAS
			elif (aux_sodar[h][posicao_t_sodar] not in ausente_t):
				temperatura = aux_sodar[h][posicao_t_sodar]

			#DEFININDO UM VALOR PARA TEMPERATURA CASO TODAS AS CONDICIONANTES ANTERIOS NÃO TENHAM SUCESSO
			else:
				temperatura = saida_temp
			#--------------------------------------------------------------------------------------------------



			#--------------------------------------------------------------------------------------------------
			#DEFININDO UM VALOR PARA DIREÇÃO
			#DEFININDO UM VALOR PARA DIREÇÃO NA ALTURA DE 40, 10, 200 M
			if (aux_sodar[h][posicao_dir_sodar] in ausente_dir) and (aux_sodar[h][posicao_z_sodar] in z_sodar[0]):
				direct = saida_dir #DEFININDO VALOR AUSENTE PARA DIREÇÃO, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA RADIOSSONDA
				for m in range(len(aux_radiossonda)):
					if (aux_sodar[h][posicao_diahora_sodar] == aux_radiossonda[m][posicao_diahora_radio]) and (aux_radiossonda[m][posicao_baro_radio] in baro[0]):
						direct = aux_radiossonda[m][posicao_dir_radio] #VALOR DE DIREÇÃO ATUALIZADO COM VALORES DA RADIOSSONDA

			#DEFININDO UM VALOR PARA DIR NA ALTURA DE 300, 400 M
			elif (aux_sodar[h][posicao_dir_sodar] in ausente_dir) and (aux_sodar[h][posicao_z_sodar] in z_sodar[1]):
				direct = saida_dir #DEFININDO VALOR AUSENTE PARA DIREÇÃO, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA RADIOSSONDA
				for n in range(len(aux_radiossonda)):
					if (aux_sodar[h][posicao_diahora_sodar] == aux_radiossonda[n][posicao_diahora_radio]) and (aux_radiossonda[n][posicao_baro_radio] in baro[1]):
						direct = aux_radiossonda[n][posicao_dir_radio] #VALOR DE DIREÇÃO ATUALIZADO COM VALORES DA RADIOSSONDA

			#DEFININDO UM VALOR PARA DIR NA ALTURA DE 500 M
			elif (aux_sodar[h][posicao_dir_sodar] in ausente_dir) and (aux_sodar[h][posicao_z_sodar] in z_sodar[2]):
				direct = saida_dir #DEFININDO VALOR AUSENTE PARA DIREÇÃO, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA RADIOSSONDA
				for o in range(len(aux_radiossonda)):
					if (aux_sodar[h][posicao_diahora_sodar] == aux_radiossonda[o][posicao_diahora_radio]) and (aux_radiossonda[o][posicao_baro_radio] in baro[2]):
						direct = aux_radiossonda[o][posicao_dir_radio] #VALOR DE DIREÇÃO ATUALIZADO COM VALORES DA RADIOSSONDA

			#DEFININDO VALOR DE DIREÇÃO CASO SODAR APRESENTE INFORMAÇÕES, ISSO VALE PARA TODAS AS ALTURAS
			elif (aux_sodar[h][posicao_dir_sodar] not in ausente_dir):
				direct = aux_sodar[h][posicao_dir_sodar] #VALOR DE DIREÇÃO ATUALIZADO COM VALORES DO SODAR

			#DEFININDO UM VALOR PARA DIREÇÃO CASO TODAS AS CONDICIONANTES ANTERIOS NÃO TENHAM SUCESSO
			else:
				direct = saida_dir
			#--------------------------------------------------------------------------------------------------



			#--------------------------------------------------------------------------------------------------
			#DEFININDO UM VALOR PARA SPEED
			#DEFININDO UM VALOR PARA SPEED NA ALTURA DE 40, 10, 200 M
			if (aux_sodar[h][posicao_speed_sodar] in ausente_speed) and (aux_sodar[h][posicao_z_sodar] in z_sodar[0]):
				speed = saida_speed #DEFININDO VALOR AUSENTE PARA SPEED, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA RADIOSSONDA
				for p in range(len(aux_radiossonda)):
					if (aux_sodar[h][posicao_diahora_sodar] == aux_radiossonda[p][posicao_diahora_radio]) and (aux_radiossonda[p][posicao_baro_radio] in baro[0]):
						speed = aux_radiossonda[p][posicao_speed_radio] #VALOR DE SPEED ATUALIZADO COM VALORES DA RADIOSSONDA

			#DEFININDO UM VALOR PARA SPEED NA ALTURA DE 300, 400 M
			elif (aux_sodar[h][posicao_speed_sodar] in ausente_speed) and (aux_sodar[h][posicao_z_sodar] in z_sodar[1]):
				speed = saida_speed #DEFININDO VALOR AUSENTE PARA SPEED, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA RADIOSSONDA
				for q in range(len(aux_radiossonda)):
					if (aux_sodar[h][posicao_diahora_sodar] == aux_radiossonda[q][posicao_diahora_radio]) and (aux_radiossonda[q][posicao_baro_radio] in baro[1]):
						speed = aux_radiossonda[q][posicao_speed_radio] #VALOR DE SPEED ATUALIZADO COM VALORES DA RADIOSSONDA

			#DEFININDO UM VALOR PARA SPEED NA ALTURA DE 500 M
			elif (aux_sodar[h][posicao_speed_sodar] in ausente_speed) and (aux_sodar[h][posicao_z_sodar] in z_sodar[2]):
				speed = saida_speed #DEFININDO VALOR AUSENTE PARA SPEED, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA RADIOSSONDA
				for r in range(len(aux_radiossonda)):
					if (aux_sodar[h][posicao_diahora_sodar] == aux_radiossonda[r][posicao_diahora_radio]) and (aux_radiossonda[r][posicao_baro_radio] in baro[2]):
						speed = aux_radiossonda[r][posicao_speed_radio] #VALOR DE SPEED ATUALIZADO COM VALORES DA RADIOSSONDA

			#DEFININDO VALOR DE SPEED CASO SODAR APRESENTE INFORMAÇÕES, ISSO VALE PARA TODAS AS ALTURAS
			elif (aux_sodar[h][posicao_speed_sodar] not in ausente_speed):
				speed = aux_sodar[h][posicao_speed_sodar] #VALOR DE SPEED ATUALIZADO COM VALORES DO SODAR

			#DEFININDO UM VALOR PARA SPEED CASO TODAS AS CONDICIONANTES ANTERIOS NÃO TENHAM SUCESSO
			else:
				speed = saida_speed
			#--------------------------------------------------------------------------------------------------



			#--------------------------------------------------------------------------------------------------
			#DEFININDO UM VALOR PARA AS VARIÁVEIS DA ESTAÇÃO DE SUPERFÍCIE - EMS
			if aux_cont_2 >= quant_h:
				pass
			else:
				if str(dia_hora + datetime.timedelta(minutes = 2, seconds = 00)) in str(aux_ems[aux_cont_2][0]):
					#--------------------------------------------------------------------------------------------------
					#DEFININDO VALOR PARA SPEED
					if (aux_ems[aux_cont_2][posicao_speed_ems] in ausente_speed):
						speed_ems = saida_speed_ems #DEFININDO VALOR AUSENTE PARA SPEED, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA EMS
					else:
						speed_ems = aux_ems[aux_cont_2][posicao_speed_ems] #VALOR DE SPEED ATUALIZADO COM VALORES DA EMS
					#--------------------------------------------------------------------------------------------------



					#--------------------------------------------------------------------------------------------------
					#DEFININDO VALOR PARA DIR
					if (aux_ems[aux_cont_2][posicao_dir_ems] in ausente_dir):
						direct_ems = saida_dir_ems #DEFININDO VALOR AUSENTE PARA DIR, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA EMS
					else:
						direct_ems = aux_ems[aux_cont_2][posicao_dir_ems] #VALOR DE DIRECT ATUALIZADO COM VALORES DA EMS
					#--------------------------------------------------------------------------------------------------



					#--------------------------------------------------------------------------------------------------
					#DEFININDO UM VALOR PARA TEMPERATURE
					if (aux_ems[aux_cont_2][posicao_t_ems] in ausente_t):
						temperatura_ems = saida_temp_ems #DEFININDO VALOR AUSENTE PARA TEMPERATURA, ESSE VALOR SERA ATUALIZADO CASO EXISTA INFORMAÇÃO DA EMS
					else:
						temperatura_ems = aux_ems[aux_cont_2][posicao_t_ems] #VALOR DE DIRECT ATUALIZADO COM VALORES DA EMS
					#--------------------------------------------------------------------------------------------------
			#--------------------------------------------------------------------------------------------------


					
			#--------------------------------------------------------------------------------------------------
					#FORMATANDO AS INFORMAÇÕES DA EMS E ESCREVENDO AS INFORMAÇÕES NO ARQUIVO
					VARIAVEIS_ARQUIVO =	 f'{10:7.1f}' + f'{99:6.1f}' + f'{99:8.3f}' + f'{temperatura_ems:7.2f}' + f'{direct_ems:8.2f}' + f'{speed_ems:8.2f}'
					arq.write(str(DATA_HORA_ARQUIVO) + str(VARIAVEIS_ARQUIVO) + '\n')
					aux_cont_2 = aux_cont_2 + 1
			#--------------------------------------------------------------------------------------------------



			#--------------------------------------------------------------------------------------------------
			#INFORMANDO AS LEITURAS DE ALTURAS VARIADAS, PARA DADOS DO SODAR, PARA A MESMA DATA HORA E ESCREVENDO AS INFORMAÇÕES NO ARQUIVO
			VARIAVEIS_ARQUIVO =	 f'{aux_sodar[h][1]:7.1f}' + f'{sig_theta:6.1f}' + f'{sig_w:8.3f}' + f'{temperatura:7.2f}' + f'{direct:8.2f}' + f'{speed:8.2f}'
			arq.write(str(DATA_HORA_ARQUIVO) + str(VARIAVEIS_ARQUIVO) + '\n')
			#--------------------------------------------------------------------------------------------------



#FECHANDO ARQUIVO
arq.close()

