import requests
url = 'http://awaire-air-quality/api/v1/aermod_inputs'
dic = {'period': '2020-01-26 06:00:00', 'pollutant_datum_id': 1}

x = requests.post(url, data = dic)
print(x.text)