# -*- coding: utf-8 -*-
import mysql.connector
import os
import datetime
from time import strptime
import shutil
import sys
import os.path
import requests



# caminho = '/home/sodarec/data_sender/gfs_data/'
caminho = 'G:/Meu Drive/data_sender/gfs_data/'
# destino = '/home/sodarec/data_sender/gfs_data/inseridos/'
destino = 'G:/Meu Drive/data_sender/gfs_data/inseridos/'
url = 'http://awaire-air-quality/api/v1/gfs_data'
entradas = os.listdir(caminho)

#CONTÉM A DATA HORA ATUAL
now = datetime.datetime.now()

#INFORMANDO O NOME DO ARQUIVO
# profile = now.strftime("profile_%Y%m%d0%H.dat")
profile = 'profile_20210312017.dat'

#AUXILIAR NA CONTAGEM DE EVENTUAIS ERROS AO INSERIR DADOS NO BANCO
aux_shell = 0

#ARRAY PARA ARMAZENAR EVENTUAIS ERROS OCORRIDOS
erros = []

#INFORMANDO A POSI��O DAS VARIAVEIS NO ARRAY aux
posicao_diahora = 0
posicao_z = 1
posicao_speed = 2
posicao_dir = 3
posicao_temp = 4
posicao_dewpt = 5
posicao_baro = 6


#FUNÇÃO PARA RETORNAR O NÚMERO DO MÊS RECEBENDO AS TRÊS PRIMEIRA LETRAS
def mes_para_numero(mes):
	meses = {'JAN':'01', 'FEV':'02', 'MAR':'03', 'ABR':'04', 'MAI':'05', 'JUN':'06', 'JUL':'07', 'AGO':'08', 'SET':'09', 'OUT':'10', 'NOV':'11', 'DEZ':'12'}
	return(meses[mes])

#FUNÇÃO PARA RETORNAR O MÊS EM INGLÊS RECEBENDO O NÚMERO DO MÊS
def mes_para_numero_ingles(mes):
	meses = {'01':'JAN', '02':'FEB', '03':'MAR', '04':'APR', '05':'MAY', '06':'JUN', '07':'JUL', '08':'AUG', '09':'SEP', '10':'OCT', '11':'NOV', '12':'DEC'}
	return(meses[mes])

		

#FUNÇÃO PARA CONVERTER TEMPERATURA
def kelvin_para_celsius(temperatura):
	temperatura = temperatura - 273
	return(temperatura)


#ABRINDO O ARQUIVO TRABALHADO
for entrada in entradas:
	arquivo = caminho + entrada
	#CHECANDO SE arquivo É UMA PASTA OU ARQUIVO
	if os.path.isfile(arquivo):
		arquivo = open((caminho + entrada),encoding='utf-8', errors='ignore').readlines()
		if entrada == profile:
			print("Lendo arquivo:" + entrada)
			for i in range(len(arquivo)):
				linha = arquivo[i].split()
				#INFORMANDO ARRAY AUXILIAR
				aux = []

				if ('254' in linha[0]):
					#INFORMANDO A LINHA COM AS INFORMAÇÕES SOBRE HORA, DIA, MÊS E ANO DE MEDIÇÃO
					#INFORMANDO A POSIÇÃO DA LINHA LIDA
					posicao = i
					#INFORMANDO A HORA DA MEDIÇÃO
					hora = linha[1]
					#INFORMANDO O DIA DA MEDIÇÃO
					dia = linha[2]
					#INFORMANDO O MÊS DA MEDIÇÃO
					mes = linha[3]
					mes = mes_para_numero(str(mes))
					mes_ingles = mes_para_numero_ingles(mes)
					#INFORMANDO O ANO DA MEDIÇÃO
					ano = linha[4]
					#CONVERTENDO STRING TO DATETIME
					dia_hora = ano + '-' + mes + '-' + dia + ' ' + hora[1:] + ':00:00'
					dia_hora = datetime.datetime.strptime(dia_hora, '%Y-%m-%d %H:%M:%S')
										
				if ('4' == linha[0]):
					aux.append(str(dia_hora))
					#INFORMANDO A LINHA COM AS INFORMAÇÕES PRESSÃO, ALTURA, TEMPERATURA, PONTO DE ORVALHO, DIREÇÃO E VELOCIDADE
					#INFORMANDO A POSIÇÃO DA LINHA LIDA
					posicao = i
					#INFORMANDO A ALTURA
					altura = linha[2]
					aux.append(altura)
					#INFORMANDO A VELOCIDADE
					speed = linha[6]
					aux.append(speed)
					#INFORMANDO A DIREÇÃO
					direcao = linha[5]
					aux.append(direcao)
					#INFORMANDO A TEMPERATURA
					temperatura = linha[3]
					temperatura = kelvin_para_celsius(float(temperatura))
					aux.append(temperatura)
					#INFORMANDO O PONTO DE ORVALHO
					p_orvalho = linha[4]
					# SUBSTITUIR POR VALOR NULO None
					if linha[4] == 32767:
						aux.append(None)
					else:
						aux.append(linha[4])
					#INFORMANDO A PRESSÃO
					pressao = linha[1]
					aux.append(pressao)
					

										
					
					
					json = {'date':aux[posicao_diahora], 'z':aux[posicao_z], 'speed':aux[posicao_speed], 'direction':aux[posicao_dir],'temperature':aux[posicao_temp],'dewpt':aux[posicao_dewpt],'baro':aux[posicao_baro]} #

					x = requests.post(url, data = json)
					print(x.text)


								
			# #MOVENDO O ARQUIVO LIDO PARA OUTRA PASTA
			# shutil.move(str(caminho) + str(profile),str(destino) + str(profile))
		else:
			continue
	else:
		continue

if aux_shell != 0:
	print(str(aux_shell) + ' erro(s) ao executar o programa. Enumerados a seguir:')
	for i in range(len(erros)):
		print(erros[i])
	sys.exit(1)
else:
	print('Programa executado com sucesso')
	sys.exit(0)