import MySQLdb, sys, csv

#db = MySQLdb.connect(host="localhost", port=3306, user="root",passwd="",db="campaign_data") #campaign_data is name of database where fec_committee_contributions table from fec_committee_contributions.sql is stored
#db = MySQLdb.connect(host="localhost", port=3306, user="root",passwd="",db="fec")
db = MySQLdb.connect(host="localhost", port=3306, user="root",passwd="",db="fec_data")

def main():
	
	# needs to create DB, read initial file fec_committee_contributions.sql into DB

	initial_setup()
	read_in_super_PACs_list()											# reads .csv file of super PACs into the database
	compute_exclusivity_score()        				# 1st score			# bumps up scores of donations made exclusively to a given recipient
	read_in_report_type_weights()										# reads .csv file of report type weights to be used for report type score
	compute_report_type_frequency()										# computes how often each report type occurs for each PAC, split by parity
	compute_report_type_score()						# 2nd score			# bumps up scores according to how early in election cycle donations were made
	compute_periodicity_score()						# 3rd score			# bumps up scores if donations are made around the same time of the year	
	##compute_highest_donations_per_PAC(cursor)
	#compute_maxed_out_bonus(cursor)				# 4th score 		# bumps up scores if contributors maxed out on donations to corresponding recipient
	#compute_geographical_bonus(cursor)				# 5th score 		# bumps up scores according to geographical proximity
	##compute_overall_score(cursor)					# Sum of scores 	# computes weighted sum of all scores
	db.close()

def initial_setup():
	cursor = db.cursor()
	#cursor.execute("DROP DATABASE IF EXISTS fec_data;")
	#cursor.execute("CREATE DATABASE fec_data;")
	#cursor.execute("USE fec_data;")
	#sql = open("fec_committee_contributions.sql").read()
	#cursor.execute(sql)
	#cursor.execute("source 'fec_committee_contributions.sql';")

	cursor.execute("ALTER TABLE fec_committee_contributions ADD INDEX (fec_committee_id, other_id);")
	cursor.execute("ALTER TABLE fec_committee_contributions ADD INDEX (fec_committee_id, other_id, report_type);")
	cursor.execute("ALTER TABLE fec_committee_contributions ADD INDEX (transaction_type, entity_type, date);")
	print "Initial setup done:"
	cursor.close()

def read_in_super_PACs_list():
	filename = "superPACs.csv"
	with open(filename, 'rU') as f:
		rows = list(csv.reader(f))
		#print rows

	cursor = db.cursor()
	cursor.execute("DROP TABLE IF EXISTS super_PACs_list;")
	cursor.execute( """ CREATE TABLE super_PACs_list (
				filer_id CHAR(9) NOT NULL,
				filer_name CHAR(200));""")
	for r in rows:
		sql = "INSERT INTO super_PACs_list (filer_id, filer_name) VALUES ('%s','%s')" % (r[1], r[2])
		try:
   			cursor.execute(sql)
   			db.commit()
		except:
   			db.rollback()

   	cursor.execute("ALTER TABLE super_PACs_list ADD INDEX (filer_id);")

   	#cursor.execute("SELECT * FROM super_PACs_list;")
   	#super_PACs_list = cursor.fetchall()
   	print "Table super_PACs_list:"
   	#print super_PACs_list
   	cursor.close()

def compute_exclusivity_score():   # Creates exclusivity_scores table, computes exclusivity scores and stores them in table
	cursor = db.cursor()
	cursor.execute("DROP TABLE IF EXISTS exclusivity_scores;")
	cursor.execute( """ CREATE TABLE exclusivity_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				total_by_pac CHAR(10),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				amount CHAR(10),
				donation_date DATE,
				exclusivity_score FLOAT(10));""")
	sql1 = "LOCK TABLES exclusivity_scores WRITE, super_PACs_list AS T3 READ, super_PACs_list AS T4 READ, fec_committee_contributions AS T READ, fec_committee_contributions AS T2 READ;"
	sql2 = "INSERT INTO exclusivity_scores (fec_committee_id, contributor_name, total_by_pac, other_id, recipient_name, amount, donation_date, exclusivity_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.total_by_PAC, T2.other_id, T2.recipient_name, T2.amount, T2.date, T2.amount/T1.total_by_PAC AS exclusivity_score FROM (SELECT T.fec_committee_id, T.contributor_name, SUM(T.amount) AS total_by_PAC FROM fec_committee_contributions T WHERE T.transaction_type = '24K' AND T.entity_type = 'PAC' AND EXTRACT(YEAR FROM T.date) >= '2003' GROUP BY T.fec_committee_id ORDER BY NULL) AS T1, fec_committee_contributions T2 WHERE T1.fec_committee_id = T2.fec_committee_id AND T2.transaction_type = '24K' AND T2.entity_type = 'PAC' AND EXTRACT(YEAR FROM T2.date) >= '2003' AND T2.fec_committee_id NOT IN (SELECT filer_id FROM super_PACs_list AS T3) AND T2.other_id NOT IN (SELECT filer_id FROM super_PACs_list AS T4) GROUP BY T1.fec_committee_id, T2.other_id ORDER BY NULL;"
	sql3 = "UNLOCK TABLES;"
	try:
   		cursor.execute(sql1)
   		cursor.execute(sql2)
   		cursor.execute(sql3)
   		db.commit()
	except:
   		db.rollback()

   	cursor.execute("ALTER TABLE exclusivity_scores ADD INDEX (fec_committee_id, other_id);")

   	#cursor.execute("SELECT * FROM exclusivity_scores;")
	#exclusivity_scores = cursor.fetchall()  # Returns a tuple (PAC id, sum of all donations made by PAC)
	print "Exclusivity Scores:"
	#print exclusivity_scores
	cursor.close()

def read_in_report_type_weights():
	filename = "report_types.csv"
	with open(filename, 'rU') as f:
		rows = list(csv.reader(f))
		#print rows

	cursor = db.cursor()
	cursor.execute("DROP TABLE IF EXISTS report_type_weights;")
	cursor.execute( """ CREATE TABLE report_type_weights (
				report_type CHAR(4) NOT NULL,
				year_parity CHAR(5),
				weight INT(2));""")
	for r in rows:
		sql = "INSERT INTO report_type_weights (report_type, year_parity, weight) VALUES ('%s','%s','%s')" % (r[0], r[1], r[2])
		try:
   			cursor.execute(sql)
   			db.commit()
		except:
   			db.rollback()

   	cursor.execute("ALTER TABLE report_type_weights ADD INDEX (report_type, year_parity);")

   	#cursor.execute("SELECT * FROM report_type_weights;")
   	#report_type_weights = cursor.fetchall()
   	print "Table report type weights:"
   	#print report_type_weights
   	cursor.close()

def compute_report_type_frequency():
	cursor = db.cursor()
	cursor.execute("DROP TABLE IF EXISTS report_type_count_by_pair;")
	cursor.execute( """ CREATE TABLE report_type_count_by_pair (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				report_type CHAR(4),
				year_parity CHAR(5),
				d_date DATE, 
				count INT(10));""")

	cursor.execute("ALTER TABLE report_type_count_by_pair ADD INDEX (fec_committee_id, other_id);")

	sql1 = "LOCK TABLES report_type_count_by_pair WRITE, exclusivity_scores AS T1 READ, fec_committee_contributions AS T2 READ;"
	sql2 = "INSERT INTO report_type_count_by_pair (fec_committee_id, contributor_name, other_id, recipient_name, report_type, year_parity, d_date, count) SELECT T1.fec_committee_id, T1.contributor_name, T2.other_id, T2.recipient_name, T2.report_type, IF(MOD(EXTRACT(YEAR FROM T2.date), 2) = 0, 'even', 'odd') AS year_parity, T2.date, count(*) FROM fec_committee_contributions AS T2, exclusivity_scores AS T1 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id GROUP BY T2.fec_committee_id, T2.other_id, T2.report_type ORDER BY NULL;"
	sql3 = "UNLOCK TABLES;"
	try:
   		cursor.execute(sql1)
   		cursor.execute(sql2)
   		cursor.execute(sql3)
   		db.commit()
	except:
   		db.rollback()


   	cursor.close()

   	#cursor.execute("SELECT * FROM report_type_count_by_pair;")
   	#report_type_count_by_pair = cursor.fetchall()  
	print "Table report_type_count_by_pair:"
	#print report_type_count_by_pair

	cursor = db.cursor()

	cursor.execute("DROP TABLE IF EXISTS pairs_count;")
	cursor.execute( """ CREATE TABLE pairs_count (
				fec_committee_id CHAR(9) NOT NULL,
				other_id CHAR(9) NOT NULL,
				count INT(10));""")
	
	cursor.execute("ALTER TABLE pairs_count ADD INDEX (fec_committee_id, other_id);")

	sql1 = "LOCK TABLES pairs_count WRITE, exclusivity_scores AS T1 READ, fec_committee_contributions AS T2 READ;"
	sql2 = "INSERT INTO pairs_count (fec_committee_id, other_id, count) SELECT T1.fec_committee_id, T2.other_id, count(*) FROM exclusivity_scores T1, fec_committee_contributions T2 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id GROUP BY T1.fec_committee_id, T1.other_id ORDER BY NULL;"
	sql3 = "UNLOCK TABLES;"
	try:
   		cursor.execute(sql1)
   		cursor.execute(sql2)
   		cursor.execute(sql3)
   		db.commit()
	except:
   		db.rollback()

   	#cursor.execute("SELECT * FROM pairs_count;")
   	#pairs_count = cursor.fetchall()  
	print "Table pairs_count:"
	#print pairs_count

	cursor.close()

	cursor = db.cursor()

   	cursor.execute("DROP TABLE IF EXISTS report_type_frequency;")
	cursor.execute( """ CREATE TABLE report_type_frequency (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				report_type CHAR(4),  
				year_parity CHAR(5),
				d_date DATE, 
				report_type_count_by_pair CHAR(10),
				pairs_count INT(10),
				report_type_frequency FLOAT(10));""")

	cursor.execute("ALTER TABLE report_type_count_by_pair ADD INDEX (report_type, year_parity);")

	sql1 = "LOCK TABLES report_type_frequency WRITE, report_type_count_by_pair AS T1 READ, pairs_count AS T2 READ;"
	sql2 = "INSERT INTO report_type_frequency (fec_committee_id, contributor_name, other_id, recipient_name, report_type, year_parity, d_date, report_type_count_by_pair, pairs_count, report_type_frequency) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.report_type, T1.year_parity, T1.d_date, T1.count AS report_type_count_by_pair, T2.count AS pairs_count, T1.count/T2.count AS report_type_frequency FROM report_type_count_by_pair T1, pairs_count T2 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id;"
	sql3 = "UNLOCK TABLES;"
	try:
   		cursor.execute(sql1)
   		cursor.execute(sql2)
   		cursor.execute(sql3)
   		db.commit()
	except:
   		db.rollback()

   	#cursor.execute("SELECT * FROM report_type_frequency;")
   	#periodicity_scores = cursor.fetchall()  
	print "Table report_type_frequency:"
	#print report_type_frequency

	cursor.close()

def compute_report_type_score():
	cursor = db.cursor()
	cursor.execute("DROP TABLE IF EXISTS report_type_scores;")
	cursor.execute( """ CREATE TABLE report_type_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				report_type CHAR(4),
				year_parity CHAR(5),
				d_date DATE, 
				report_type_frequency FLOAT(10),
				weight INT(2),
				report_type_score FLOAT(10));""")
	sql1 = "LOCK TABLES report_type_scores WRITE, report_type_weights AS T1 READ, report_type_frequency AS T2 READ;"
	sql2 = "INSERT INTO report_type_scores (fec_committee_id, contributor_name, other_id, recipient_name, report_type, year_parity, d_date, report_type_frequency, weight, report_type_score) SELECT T3.fec_committee_id, T3.contributor_name, T3.other_id, T3.recipient_name, T3.report_type, T3.year_parity, T3.d_date, T3.report_type_frequency, T3.weight, SUM(T3.report_type_subscore) AS report_type_score FROM (SELECT T2.fec_committee_id, T2.contributor_name, T2.other_id, T2.recipient_name, T1.report_type, T1.year_parity, T2.d_date, T2.report_type_frequency, T1.weight, T2.report_type_frequency * T1.weight AS report_type_subscore FROM report_type_weights T1, report_type_frequency T2 WHERE T1.report_type = T2.report_type AND T1.year_parity = T2.year_parity) T3 GROUP BY T3.fec_committee_id, T3.other_id ORDER BY NULL;"
	sql3 = "UNLOCK TABLES;"
	try:
   		cursor.execute(sql1)
   		cursor.execute(sql2)
   		cursor.execute(sql3)
   		db.commit()
	except:
   		db.rollback()

   	# Needs to index table!

   	#cursor.execute("SELECT * FROM report_type_scores;")
   	#report_type_scores = cursor.fetchall()  
	print "Table report_type_scores:"
	#print report_type_scores
	cursor.close()

def compute_periodicity_score():
	cursor = db.cursor()
	cursor.execute("DROP TABLE IF EXISTS periodicity_scores;")
	cursor.execute( """ CREATE TABLE periodicity_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				periodicity_score FLOAT(10));""")
	sql1 = "LOCK TABLES periodicity_scores WRITE, super_PACs_list AS T2 READ, super_PACs_list AS T3 READ, fec_committee_contributions AS T1 READ;"
	sql2 = "INSERT INTO periodicity_scores (fec_committee_id, contributor_name, other_id, recipient_name, periodicity_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, IFNULL(1/VAR_POP(DAYOFYEAR(T1.date)), 0) AS periodicity_score FROM fec_committee_contributions T1 WHERE T1.transaction_type = '24K' AND T1.entity_type = 'PAC' AND EXTRACT(YEAR FROM T1.date) >= '2003' AND T1.fec_committee_id NOT IN (SELECT filer_id FROM super_PACs_list T2) AND T1.other_id NOT IN (SELECT filer_id FROM super_PACs_list T3) GROUP BY T1.fec_committee_id, T1.other_id ORDER BY NULL;"
	sql3 = "UNLOCK TABLES;"
	try:
   		cursor.execute(sql1)
   		cursor.execute(sql2)
   		cursor.execute(sql3)
   		db.commit()
	except:
   		db.rollback()

   	cursor.close()

   	# Needs to index table!

   	#cursor.execute("SELECT * FROM periodicity_scores;")
   	#pairs_count = cursor.fetchall()  
	print "Table periodicity_scores:"
	#print periodicity_scores

#def compute_highest_donations_per_PAC(cursor):

#def compute_maxed_out_bonus(cursor):

#def compute_geographical_bonus(cursor)

if __name__ == "__main__":
	if len(sys.argv) == 1:
		main()
	else:
		print "Try again."
