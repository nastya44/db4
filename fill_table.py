import pymongo 
import csv
import os
import time
import json

RESULT_FILE = 'result.csv'

RES_COLUMNS = ['Year', '100-200' ,'Region']

COLUMNS_NAME = ['rows', 'year', '_id', 'Birth', 'SEXTYPENAME', 'REGNAME', 'AREANAME', 'TERNAME', 'REGTYPENAME', 'TerTypeName', 'ClassProfileNAME', 'ClassLangName', 'EONAME', 'EOTYPENAME', 'EORegName', 'EOAreaName', 'EOTerName', 'EOParent', 'UkrTest', 'UkrTestStatus', 'UkrBall100', 'UkrBall12', 'UkrBall', 'UkrAdaptScale', 'UkrPTName', 'UkrPTRegName', 'UkrPTAreaName', 'UkrPTTerName', 'histTest', 'HistLang', 'histTestStatus', 'histBall100', 'histBall12', 'histBall', 'histPTName', 'histPTRegName', 'histPTAreaName', 'histPTTerName', 'mathTest', 'mathLang', 'mathTestStatus', 'mathBall100', 'mathBall12', 'mathBall', 'mathPTName', 'mathPTRegName', 'mathPTAreaName', 'mathPTTerName', 'physTest', 'physLang', 'physTestStatus', 'physBall100', 'physBall12', 'physBall', 'physPTName', 'physPTRegName', 'physPTAreaName', 'physPTTerName', 'chemTest', 'chemLang', 'chemTestStatus', 'chemBall100', 'chemBall12', 'chemBall', 'chemPTName', 'chemPTRegName', 'chemPTAreaName', 'chemPTTerName', 'bioTest', 'bioLang', 'bioTestStatus', 'bioBall100', 'bioBall12', 'bioBall', 'bioPTName', 'bioPTRegName', 'bioPTAreaName', 'bioPTTerName', 'geoTest', 'geoLang', 'geoTestStatus', 'geoBall100', 'geoBall12', 'geoBall', 'geoPTName', 'geoPTRegName', 'geoPTAreaName', 'geoPTTerName', 'engTest', 'engTestStatus', 'engBall100', 'engBall12', 'engDPALevel', 'engBall', 'engPTName', 'engPTRegName', 'engPTAreaName', 'engPTTerName', 'fraTest', 'fraTestStatus', 'fraBall100', 'fraBall12', 'fraDPALevel', 'fraBall', 'fraPTName', 'fraPTRegName', 'fraPTAreaName', 'fraPTTerName', 'deuTest', 'deuTestStatus', 'deuBall100', 'deuBall12', 'deuDPALevel', 'deuBall', 'deuPTName', 'deuPTRegName', 'deuPTAreaName', 'deuPTTerName', 'spaTest', 'spaTestStatus', 'spaBall100', 'spaBall12', 'spaDPALevel', 'spaBall', 'spaPTName', 'spaPTRegName', 'spaPTAreaName', 'spaPTTerName']

COLUMNS = [1, 18, 19, 20, 21, 29, 30, 31, 39, 40, 41, 49, 50, 51, 59, 60, 61, 69, 70, 71, 79, 80, 81, 88, 89, 91, 98, 99, 101, 108, 109, 111, 118, 119, 121]

client = pymongo.MongoClient("mongodb+srv://mongodb:mongodb@cluster0.k261i.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client['db']
collection = db['zno']


executions_time = dict()

file_names = ['Odata2019File.csv']

years = ['2019']

years_count = 0
start_time = time.time()
print('Start')
try:
	for file_name in file_names:
		year = years[years_count]
		with open(os.path.join(file_name), encoding='cp1251') as file:
			csv_reader = csv.reader(file, delimiter=';')
			next(csv_reader)
			iD = 0
			size = list()
			RESULT = collection.find_one({'year' : year}, sort=[('rows', -1)])
			if RESULT:
				if 'rows' not in RESULT:
					continue
				for i in range(RESULT['rows'] + 1):
					next(csv_reader)
					iD += 1



			for STR in csv_reader:
				for i in range(len(STR)):
					if STR[i] == 'null':
						STR[i] = None
					else:
						if i in COLUMNS:
							STR[i] = STR[i].replace(',', '.')
							STR[i] = float(STR[i])
				iD += 1
				size.append(dict(zip(COLUMNS_NAME, [iD] + [year] + STR)))
				if not iD % 200:
					collection.insert_many(size)
					size = list()
			if size:
				collection.insert_many(size)
				collection.update_many({}, {'$unset': {'rows': 1}})
				size = list()
			years_count = years_count + 1
except Exception:
	print('Too big file.')
finally:
	print('Done')
	exec_time = time.time() - start_time
	print(f'File {file_name} was read. Exec time was {exec_time} sec.')
	executions_time[file_name] = exec_time

	if executions_time:
	    with open(os.path.join('time.txt'), 'w') as file:
	        file.write(json.dumps(executions_time))

	    print(f'The result was written in {RESULT_FILE}.')


