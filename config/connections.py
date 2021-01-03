connDetails = { # always loaded
}

connDetailsPublic = {
	"rfam": { # https://docs.rfam.org/en/latest/database.html
		"type": "mysql",
		"creds": dict(
			host = 'mysql-rfam-public.ebi.ac.uk',
			user = 'rfamro',
			password = '',
			port = 4497,
			db = 'Rfam',
		),
	},
}

connDetailsExamples = {
	"1": {
		"type": "mysql",
		"creds": dict(
			host = '',
			user = '',
			password = '',
			port = 3306,
			db = '',
		),
	},
	"2": {
		"type": "redshift",
		"creds": dict(
			dbname='',
			user='',
			host='',
			password='',
			port=5439,
		),
	},
	"3": {
		"type": "presto",
		"creds": dict(
			host = '',
			port = 8889,
			username = '',
		),
	},
	"4": {
		"type": "hive",
		"creds": dict(
			host = '',
			port = 10000,
			username = '',
		),
	},
}
