from tabulate import tabulate

def sparkprint(df, headers='keys', tablefmt='psql', floatfmt=".0f", showindex=False):
	print(tabulate(df, headers=headers, tablefmt=tablefmt, floatfmt=floatfmt, showindex=showindex))

def sparkprintf(df, headers='keys', tablefmt='psql', floatfmt=".03f", showindex=False):
	print(tabulate(df, headers=headers, tablefmt=tablefmt, floatfmt=floatfmt, showindex=showindex))

def format_nicely(thing):
	for i in thing:
		print(i[0])

def format_nicely_mysql(thing):
	for i in thing[0]:
		print(i)
