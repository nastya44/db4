import pymongo 
import csv
import os

RES_COLUMNS = ['Year', '100-200' ,'Region']

RESULT_FILE = 'result.csv'

client = pymongo.MongoClient("mongodb+srv://mongodb:mongodb@cluster0.k261i.mongodb.net/myFirstDatabase?retryWrites=true&w=majority")
db = client['db']
collection = db['zno']

with open(os.path.join(RESULT_FILE), 'w', newline='') as csvfile:
	csv_writer = csv.writer(csvfile)
	csv_writer.writerow(RES_COLUMNS)
	for year in ['2019']:
		res = collection.aggregate([{'$match': {'engTestStatus': 'Зараховано'}},{'$group': {'_id': {'region': '$REGNAME', 'year': '$year'},'avg': { '$avg': '$engBall100' }}}])
		for i in res:
			row = [str(i["_id"]["year"]),str(i["avg"]),i["_id"]["region"]]
			csv_writer.writerow(row)

print(f'The result was written in {RESULT_FILE}.')
