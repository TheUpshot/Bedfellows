import MySQLdb, sys, csv

#db = MySQLdb.connect(host="localhost", port=3306, user="root",passwd="",db="campaign_data") # make sure db argument matches name of database where fec_committee_contributions.sql is stored
db = MySQLdb.connect(host="localhost", port=3306, user="root",passwd="",db="fec")
#db = MySQLdb.connect(host="localhost", port=3306, user="root",passwd="",db="fec_data")

def main():
	cursor = db.cursor()

	initial_setup(cursor)
	create_super_PACs_list(cursor)												# reads .csv file of super PACs into the database
	compute_exclusivity_score(cursor)        				# 1st score			# bumps up scores of donations made exclusively to a given recipient
	read_in_report_type_weights(cursor)											# reads .csv file of report type weights to be used for report type score
	compute_report_type_frequency(cursor)										# computes how often each report type occurs for each PAC, split by parity
	compute_report_type_score(cursor)						# 2nd score			# bumps up scores according to how early in election cycle donations were made
	compute_periodicity_score(cursor)						# 3rd score			# bumps up scores if donations are made around the same time of the year	
	#compute_maxed_out_bonus(cursor)						# 4th score 		# bumps up scores if contributors maxed out on donations to corresponding recipient
	compute_race_focus_scores(cursor)						# 5th score 		# bumps up scores according to geographical proximity
	##compute_overall_score(cursor)							# Sum of scores 	# computes weighted sum of all scores

	db.close()

def initial_setup(cursor):
	cursor.execute("ALTER TABLE fec_committee_contributions ADD INDEX (fec_committee_id, other_id);")
	cursor.execute("ALTER TABLE fec_committee_contributions ADD INDEX (fec_committee_id, other_id, report_type);")
	cursor.execute("ALTER TABLE fec_committee_contributions ADD INDEX (transaction_type, entity_type, date);")

	cursor.execute("ALTER TABLE fec_committees ADD INDEX (fecid)")
	cursor.execute("ALTER TABLE fec_committees ADD INDEX (is_super_PAC)")

	# needs to index fec_candidates

	try:
		db.commit()
	except:
		db.rollback()

	print "Initial setup done"

def create_super_PACs_list(cursor):
	sql1 = "DROP TABLE IF EXISTS super_PACs_list;"
	sql2 = """ CREATE TABLE super_PACs_list (
				fecid CHAR(9) NOT NULL);"""
	sql3 = "LOCK TABLES super_PACs_list WRITE, fec_committees AS T READ;"
	sql4 = "INSERT INTO super_PACs_list (fecid) SELECT T.fecid FROM fec_committees T WHERE T.is_super_PAC = '1';"
	sql5 = "UNLOCK TABLES;"	
	sql6 = "ALTER TABLE super_PACs_list ADD INDEX (fecid);"

	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)

  	print "Table super_PACs_list"

def compute_exclusivity_score(cursor):   # Creates exclusivity_scores table, computes exclusivity scores and stores them in table
	sql1 = "DROP TABLE IF EXISTS total_donated_by_PAC;"
	sql2 =  """ CREATE TABLE total_donated_by_PAC (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				total_by_PAC FLOAT(10));"""
	sql3 = "LOCK TABLES total_donated_by_PAC WRITE, fec_committee_contributions AS T READ;"
	sql4 = "INSERT INTO total_donated_by_PAC (fec_committee_id, contributor_name, total_by_PAC) SELECT T.fec_committee_id, T.contributor_name, SUM(T.amount) AS total_by_PAC FROM fec_committee_contributions T WHERE T.transaction_type = '24K' AND T.entity_type = 'PAC' AND EXTRACT(YEAR FROM T.date) >= '2003' GROUP BY T.fec_committee_id ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"	
	sql6 = "ALTER TABLE total_donated_by_PAC ADD INDEX (fec_committee_id);"

	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)

	sql1 = "DROP TABLE IF EXISTS exclusivity_scores;"
	sql2 = """ CREATE TABLE exclusivity_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				total_by_pac CHAR(10),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				amount CHAR(10),
				donation_date DATE,
				exclusivity_score FLOAT(10));"""
	sql3 = "LOCK TABLES exclusivity_scores WRITE, total_donated_by_PAC AS T1 READ, fec_committee_contributions AS T2 READ, super_PACs_list T3 READ, super_PACs_list T4 READ;"
	sql4 = "INSERT INTO exclusivity_scores (fec_committee_id, contributor_name, total_by_pac, other_id, recipient_name, amount, donation_date, exclusivity_score) SELECT T.fec_committee_id, T.contributor_name, T.total_by_PAC, T.other_id, T.recipient_name, SUM(T.amount) AS total_amount, SUM(exclusivity_subscore) AS exclusivity_score FROM (SELECT T1.fec_committee_id, T1.contributor_name, T1.total_by_PAC, T2.other_id, T2.recipient_name, T2.amount, T2.date, T2.amount/T1.total_by_PAC AS exclusivity_subscore FROM fec_committee_contributions T2, total_donated_by_PAC T1 WHERE T1.fec_committee_id = T2.fec_committee_id AND T2.transaction_type = '24K' AND T2.entity_type = 'PAC' AND EXTRACT(YEAR FROM T2.date) >= '2003' AND T2.fec_committee_id NOT IN (SELECT fecid FROM super_PACs_list T3) AND T2.other_id NOT IN (SELECT fecid FROM super_PACs_list T4)) T GROUP BY T.fec_committee_id, T.other_id ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"
   	sql6 = "ALTER TABLE exclusivity_scores ADD INDEX (fec_committee_id, other_id);"

   	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)

	print "Table exclusivity_scores"

def read_in_report_type_weights(cursor):
	filename = "report_types.csv"
	with open(filename, 'rU') as f:
		rows = list(csv.reader(f))
	
	try:
		cursor.execute("DROP TABLE IF EXISTS report_type_weights;")
		cursor.execute( """ CREATE TABLE report_type_weights (
				report_type CHAR(4) NOT NULL,
				year_parity CHAR(5),
				weight INT(2));""")
		cursor.execute("LOCK TABLES report_type_weights WRITE;")
		db.commit()
	except:
		db.rollback()
	for r in rows:
		sql = "INSERT INTO report_type_weights (report_type, year_parity, weight) VALUES ('%s','%s','%s')" % (r[0], r[1], r[2])
		try:
   			cursor.execute(sql)
   			db.commit()
		except:
   			db.rollback()
   	cursor.execute("UNLOCK TABLES;")
   	try:
   		cursor.execute("ALTER TABLE report_type_weights ADD INDEX (report_type, year_parity);")
   		db.commit()
	except:
   		db.rollback()

   	print "Table report_type_weights"

def compute_report_type_frequency(cursor):
	sql1 = "DROP TABLE IF EXISTS report_type_count_by_pair;"
	sql2 = """ CREATE TABLE report_type_count_by_pair (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				report_type CHAR(4),
				year_parity CHAR(5),
				d_date DATE, 
				count INT(10));"""
	sql3 = "LOCK TABLES report_type_count_by_pair WRITE, exclusivity_scores AS T1 READ, fec_committee_contributions AS T2 READ;"
	sql4 = "INSERT INTO report_type_count_by_pair (fec_committee_id, contributor_name, other_id, recipient_name, report_type, year_parity, d_date, count) SELECT T1.fec_committee_id, T1.contributor_name, T2.other_id, T2.recipient_name, T2.report_type, IF(MOD(EXTRACT(YEAR FROM T2.date), 2) = 0, 'even', 'odd') AS year_parity, T2.date, count(*) FROM fec_committee_contributions AS T2, exclusivity_scores AS T1 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id GROUP BY T2.fec_committee_id, T2.other_id, T2.report_type ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE report_type_count_by_pair ADD INDEX (fec_committee_id, other_id);"

   	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)

 	print "Table report_type_count_by_pair"

	sql1 = "DROP TABLE IF EXISTS pairs_count;"
	sql2 = """ CREATE TABLE pairs_count (
				fec_committee_id CHAR(9) NOT NULL,
				other_id CHAR(9) NOT NULL,
				count INT(10));"""
	sql3 = "LOCK TABLES pairs_count WRITE, exclusivity_scores AS T1 READ, fec_committee_contributions AS T2 READ;"
	sql4 = "INSERT INTO pairs_count (fec_committee_id, other_id, count) SELECT T1.fec_committee_id, T2.other_id, count(*) FROM exclusivity_scores T1, fec_committee_contributions T2 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id GROUP BY T1.fec_committee_id, T1.other_id ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE pairs_count ADD INDEX (fec_committee_id, other_id);"

	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
 
	print "Table pairs_count"

   	sql1 = "DROP TABLE IF EXISTS report_type_frequency;"
	sql2 = """ CREATE TABLE report_type_frequency (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				report_type CHAR(4),  
				year_parity CHAR(5),
				d_date DATE, 
				report_type_count_by_pair CHAR(10),
				pairs_count INT(10),
				report_type_frequency FLOAT(10));"""
	sql3 = "LOCK TABLES report_type_frequency WRITE, report_type_count_by_pair AS T1 READ, pairs_count AS T2 READ;"
	sql4 = "INSERT INTO report_type_frequency (fec_committee_id, contributor_name, other_id, recipient_name, report_type, year_parity, d_date, report_type_count_by_pair, pairs_count, report_type_frequency) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.report_type, T1.year_parity, T1.d_date, T1.count AS report_type_count_by_pair, T2.count AS pairs_count, T1.count/T2.count AS report_type_frequency FROM report_type_count_by_pair T1, pairs_count T2 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE report_type_count_by_pair ADD INDEX (report_type, year_parity);"

	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)

	print "Table report_type_frequency"

def compute_report_type_score(cursor):
	sql1 = "DROP TABLE IF EXISTS report_type_scores;"
	sql2 = """ CREATE TABLE report_type_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				report_type_score FLOAT(10));"""
	sql3 = "LOCK TABLES report_type_scores WRITE, report_type_weights AS T1 READ, report_type_frequency AS T2 READ;"
	sql4 = "INSERT INTO report_type_scores (fec_committee_id, contributor_name, other_id, recipient_name, report_type_score) SELECT T3.fec_committee_id, T3.contributor_name, T3.other_id, T3.recipient_name, SUM(T3.report_type_subscore) AS report_type_score FROM (SELECT T2.fec_committee_id, T2.contributor_name, T2.other_id, T2.recipient_name, T1.report_type, T1.year_parity, T2.d_date, T2.report_type_frequency, T1.weight, T2.report_type_frequency * T1.weight AS report_type_subscore FROM report_type_weights T1, report_type_frequency T2 WHERE T1.report_type = T2.report_type AND T1.year_parity = T2.year_parity) T3 GROUP BY T3.fec_committee_id, T3.other_id ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE report_type_scores ADD INDEX (fec_committee_id, other_id);" # Might need to change this, see overall_score!

	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)

	print "Table report_type_scores"

def compute_periodicity_score(cursor):
	sql1 = "DROP TABLE IF EXISTS periodicity_scores;"
	sql2 = """ CREATE TABLE periodicity_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				periodicity_score FLOAT(10));"""
	sql3 = "LOCK TABLES periodicity_scores WRITE, super_PACs_list AS T2 READ, super_PACs_list AS T3 READ, fec_committee_contributions AS T1 READ;"
	sql4 = "INSERT INTO periodicity_scores (fec_committee_id, contributor_name, other_id, recipient_name, periodicity_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, IFNULL(1/VAR_POP(DAYOFYEAR(T1.date)), 0) AS periodicity_score FROM fec_committee_contributions T1 WHERE T1.transaction_type = '24K' AND T1.entity_type = 'PAC' AND EXTRACT(YEAR FROM T1.date) >= '2003' AND T1.fec_committee_id NOT IN (SELECT fecid FROM super_PACs_list T2) AND T1.other_id NOT IN (SELECT fecid FROM super_PACs_list T3) GROUP BY T1.fec_committee_id, T1.other_id ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE periodicity_scores ADD INDEX (fec_committee_id, other_id);" # Might need to change this, see overall_score!

	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)

	print "Table periodicity_scores"

#def compute_maxed_out_bonus(cursor):

def compute_race_focus_scores(cursor):
	sql1 = "DROP TABLE IF EXISTS races_list;"
	sql2 = """ CREATE TABLE races_list (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				fec_candidate_id CHAR(9) NOT NULL,
				candidate_name CHAR(200),
				district CHAR(3),
				office_state CHAR(3),
				branch CHAR(2),
				cycle CHAR(5));"""
	sql3 = "LOCK TABLES races_list WRITE, fec_committee_contributions AS T1 READ, fec_committees AS T2 READ, fec_candidates AS T3 READ, super_PACs_list AS T4 READ, super_PACs_list AS T5 READ;"
	sql4 = "INSERT INTO races_list (fec_committee_id, contributor_name, other_id, recipient_name, fec_candidate_id, candidate_name, district, office_state, branch, cycle) SELECT DISTINCT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T2.fec_candidate_id, T3.name as candidate_name, T3.district, T3.office_state, T3.branch, T3.cycle FROM fec_committee_contributions T1, fec_committees T2, fec_candidates T3 WHERE T2.fec_candidate_id = T3.fecid AND T1.other_id = T2.fecid AND T1.transaction_type = '24K' AND T1.entity_type = 'PAC' AND EXTRACT(YEAR FROM T1.date) >= '2003' AND T1.fec_committee_id NOT IN (SELECT fecid FROM super_PACs_list T4) AND T1.other_id NOT IN (SELECT fecid FROM super_PACs_list T5) AND T2.fec_candidate_id REGEXP '^[HPS]';"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE races_list ADD INDEX (district, office_state, branch);"

	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)

	sql1 = "DROP TABLE IF EXISTS race_focus_scores;"
	sql2 = """ CREATE TABLE race_focus_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				race_focus_score FLOAT(10));"""
	sql3 = "LOCK TABLES race_focus_scores WRITE, races_list AS T READ;"
	sql4 = "INSERT INTO race_focus_scores (fec_committee_id, contributor_name, race_focus_score) SELECT * FROM (SELECT T1.fec_committee_id, T1.contributor_name, 1/COUNT(*) AS race_focus_score FROM (SELECT T.fec_committee_id, T.contributor_name, T.district, T.office_state, T.branch FROM races_list T GROUP BY T.fec_committee_id, T.district, T.office_state, T.branch ORDER BY NULL) T1 GROUP BY T1.fec_committee_id ORDER BY NULL) T2 ORDER BY T2.race_focus_score DESC;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE race_focus_scores ADD INDEX (fec_committee_id);"

	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)

	print "Table race_focus_scores"

def commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6):
	try:
   		cursor.execute(sql1)
   		cursor.execute(sql2)
   		cursor.execute(sql3)
   		cursor.execute(sql4)
   		cursor.execute(sql5)
   		cursor.execute(sql6)
   		db.commit()
	except:
   		db.rollback()

if __name__ == "__main__":
	if len(sys.argv) == 1:
		main()
	else:
		print "Try again."
