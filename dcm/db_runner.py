import os
import time
import json
from shutil import copy2 as copy

import pandas as pd

import sys
for shared_dir in ['../']:
	if shared_dir not in sys.path:
		sys.path.append(shared_dir)

class DbRunner():
	def __init__(self, db, name="", folder="../history/"):
		self.db = db
		self.logged_queries_filepath = "{}logged_queries_{}.json".format(folder, name)
		self.saved_queries_filepath = "{}saved_queries.json".format(folder)
		self.logged_results_dir = "{}logged_results_{}".format(folder, name)

	def get_timestamp(self):
		return int(time.time())

	def replay_query(self, timestamp, trycache=True, logquery=False, logresult=False):
		if os.path.isfile(self.logged_queries_filepath):
			with open(self.logged_queries_filepath, "r") as f:
				data = json.loads(f.read())
				if timestamp < 99:
					timestamp_key = sorted([k for k in data], reverse=True)[timestamp-1]
				else:
					timestamp_key = str(timestamp)
				if timestamp_key in data:
					q = data[timestamp_key]
					print(q["query"])
					print(q["params"])
					potential_filepath = "{}/{}.csv".format(self.logged_results_dir, timestamp_key)
					if trycache and os.path.isfile(potential_filepath):
						print("Loading results for timestamp {} from cache...".format(timestamp_key))
						return pd.read_csv(potential_filepath)
					return self.run_query(query=q["query"], params=q["params"], logquery=logquery, logresult=logresult, dbname=q["dbname"])
				else:
					print("Key (timestamp: {}) not found".format(timestamp_key))
		else:
			print("Log not found")

	def run_saved_query(self, name, params={}, logquery=True, logresult=False):
		if os.path.isfile(self.saved_queries_filepath):
			with open(self.saved_queries_filepath, "r") as f:
				data = json.loads(f.read())
				if name in data:
					q = data[name]
					print(q["query"])
					print(params)
					return self.run_query(query=q["query"], params=params, logquery=logquery, logresult=logresult, dbname=q["dbname"])
				else:
					print("Key (name: {}) not found".format(name))
		else:
			print("Log not found")

	def append_json(self, filepath, key, value):
		key = str(key)
		need_to_delete = []
		if os.path.isfile(filepath):
			bk_filepath = filepath+".bak"
			copy(filepath, bk_filepath)
			need_to_delete.append(bk_filepath)
			with open(bk_filepath, "r") as f:
				data = json.loads(f.read())
		else:
			data = {}
		data[key] = value
		with open(filepath, "w") as f:
			f.write(json.dumps(data, sort_keys=True, indent=2))
			f.write("\n")
		for f in need_to_delete:
			os.remove(f)

	def log_query(self, timestamp, query, dbname, params):
		self.append_json(self.logged_queries_filepath, timestamp, {"query": query, "dbname": dbname, "params": params})

	def save_query(self, name, query, dbname="presto"):
		self.append_json(self.saved_queries_filepath, name, {"query": query, "dbname": dbname})

	def export(self, df, filename, filedir=None):
		if filedir is None:
			filedir = self.logged_results_dir
		if not os.path.exists(filedir):
			os.makedirs(filedir)
		df.to_csv("{}/{}.csv".format(filedir, filename), index=False)

	def run_query(self, query, params={}, logquery=True, logresult=False, dbname="presto", name=None, dblpct=False):
		timestamp = self.get_timestamp()
		print("Timestamp: {}".format(timestamp))
		if dblpct:
			query = query.replace('%', '%%')
		query = query.strip()
		df = pd.read_sql_query(query, con=self.db.conns[dbname], params=params)
		if logquery or logresult:
			self.log_query(timestamp, query, dbname, params)
		if logresult:
			self.export(df, timestamp)
		if name:
			self.save_query(name, query, dbname)
		return df
