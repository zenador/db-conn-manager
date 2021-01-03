import os
import random

from sqlalchemy.engine import create_engine

import sys
for shared_dir in ['../']:
	if shared_dir not in sys.path:
		sys.path.append(shared_dir)

from config.connections import connDetails

class DbHelper():
	def __init__(self):
		self.conns = {}
		self.uris = {}
		self.engines = {}
		self.registry = {}
		self.transient = {}
		self.prepare_databases(connDetails)

	def set_transient(self, key, value):
		self.transient[key] = value

	def prepare_databases(self, conn_list):
		for name, details in conn_list.items():
			self.connect_to_database(name, details)

	def connect_to_database(self, name, details=None):
		if details:
			self.registry[name] = details
		else:
			details = self.registry[name]
		dbtype = details["type"]
		creds = details["creds"]
		if dbtype == "mysql":
			import pymysql
			self.conns[name] = pymysql.connect(local_infile=True, **creds)
			self.uris[name] = 'mysql+pymysql://{user}:{password}@{host}:{port}/{db}'.format(**creds)
			self.engines[name] = create_engine(self.uris[name])
		elif dbtype in ["postgres", "redshift"]:
			import psycopg2
			self.conns[name] = psycopg2.connect(**creds)
		elif dbtype == "presto":
			from pyhive import presto
			self.conns[name] = presto.connect(**creds)
			self.uris[name] = 'presto://{username}@{host}:{port}/hive/default'.format(**creds)
			self.engines[name] = create_engine(self.uris[name])
			# engines[name] = create_engine('presto://', creator=lambda: conns[name])
		elif dbtype == "hive":
			from pyhive import hive
			self.conns[name] = hive.connect(**creds)

	def execute_query(self, name, query, params={}, results=True, commit=False):
		conn = self.conns[name]
		cur = conn.cursor()
		cur.execute(query, params)
		if commit:
			conn.commit()
		if results:
			return cur.fetchall()

	def pd_to_sql(self, df, which_db, table, schema, if_exists="replace", temp_filename=None, indexes="", keep_file=False): # note that indexes will not be created with append
		if temp_filename is None:
			temp_filename = "temporaryfile_{}.csv".format(random.randint(0,999999999))

		# for some reason it hangs without this if the table already exists
		if if_exists == "replace":
			self.execute_query(which_db, """DROP TABLE IF EXISTS `{}`.`{}`""".format(schema, table))

		# this will create the empty table, and replace if requested
		df.iloc[:0].to_sql(table, schema=schema, con=self.engines[which_db], if_exists=if_exists, index=False)

		df.to_csv(temp_filename, index=False, encoding="utf-8")
		# this will append
		res = self.execute_query(which_db, """
			LOAD DATA LOCAL INFILE %(path)s
			INTO TABLE `{schema}`.`{table}`
			CHARACTER SET %(encoding)s
			FIELDS TERMINATED BY %(delimiter)s
			ENCLOSED BY %(quotechar)s
			ESCAPED BY %(escapechar)s
			LINES TERMINATED BY %(lineterminator)s
			IGNORE %(skiprows)s LINES
		""".format(table=table, schema=schema), {
			'lineterminator': u'\n', 
			'encoding': 'utf8', 
			'skiprows': 1, 
			'delimiter': ',', 
			'escapechar': '\\', 
			'quotechar': '"', 
			'path': "{}/{}".format(os.getcwd(), temp_filename)
		}, commit=True)
		if not keep_file:
			os.remove(temp_filename)

		if indexes and if_exists == "replace":
			self.execute_query(which_db, """CREATE INDEX auto_index ON `{}`.`{}`({})""".format(schema, table, indexes))

		return res
