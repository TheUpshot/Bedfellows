import MySQLdb, sys, csv
from sys import stdout

def main():
	cursor = db.cursor()

	#initial_setup(cursor)
	#create_super_PACs_list(cursor)												# reads .csv file of super PACs into the database
	#compute_exclusivity_score(cursor)        				# 1st score			# bumps up scores of donations made exclusively to a given recipient
	compute_report_type_score(cursor)						# 2nd score			# bumps up scores according to how early in election cycle donations were made
	compute_periodicity_score(cursor)						# 3rd score			# bumps up scores if donations are made around the same time of the year	
	compute_race_focus_scores(cursor)						# 4th score 		# bumps up scores according to geographical proximity
	compute_maxed_out_scores(cursor)						# 5th score 		# bumps up scores if contributors maxed out on donations to corresponding recipient
	#compute_overall_score(cursor)							# Sum of scores 	# computes weighted sum of all scores
	db.close()

def initial_setup(cursor):
	cursor.execute("ALTER TABLE fec_committee_contributions ADD INDEX (fec_committee_id, other_id);")
	cursor.execute("ALTER TABLE fec_committee_contributions ADD INDEX (fec_committee_id, other_id, report_type);")
	cursor.execute("ALTER TABLE fec_committee_contributions ADD INDEX (cycle, fec_committee_id, other_id);")
	cursor.execute("ALTER TABLE fec_committee_contributions ADD INDEX (transaction_type, entity_type, date);")
	cursor.execute("ALTER TABLE fec_committees ADD INDEX (fecid);")
	cursor.execute("ALTER TABLE fec_committees ADD INDEX (is_super_PAC);")
	cursor.execute("ALTER TABLE fec_committees ADD INDEX (committee_type);")
	cursor.execute("ALTER TABLE fec_committees ADD INDEX (cycle);")
	cursor.execute("ALTER TABLE fec_candidates ADD INDEX (fecid);")
	try:
		db.commit()
	except:
		db.rollback()
	print "Initial setup done"


def create_super_PACs_list(cursor):
	# Reads into database table with ID's of super PACs to be excluded from this analysis.
	sql1 = "DROP TABLE IF EXISTS super_PACs_list;"
	sql2 = """ CREATE TABLE super_PACs_list (
				fecid CHAR(9) NOT NULL);"""
	sql3 = "LOCK TABLES super_PACs_list WRITE, fec_committees AS T READ;"
	sql4 = "INSERT INTO super_PACs_list (fecid) SELECT T.fecid FROM fec_committees T WHERE T.is_super_PAC = '1';"
	sql5 = "UNLOCK TABLES;"	
	sql6 = "ALTER TABLE super_PACs_list ADD INDEX (fecid);"
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
  	print "Table super_PACs_list"


def compute_exclusivity_score(cursor):  
	# First, computes total amount donated by a given PAC contributor across all recipients. 
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
	print "Table total_donated_by_PAC"

	# Then, computes exclusivity score for a given contributor/recipient pair. Score is calculated as follows: amount given to recipient as percentage of total donated by contributor.
	# Analysis focuses on donations bound by the following constraints: transaction type '24K', entity type 'PAC', year 2003 or later, contributor and recipient not present in list of super PACs.
	# No need for normalization because scores are capped at 1, so 0-1 scale is already enforced.
	sql1 = "DROP TABLE IF EXISTS exclusivity_scores;"
	sql2 = """ CREATE TABLE exclusivity_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				total_by_pac CHAR(10),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				amount CHAR(10),
				exclusivity_score FLOAT(10));"""
	sql3 = "LOCK TABLES exclusivity_scores WRITE, total_donated_by_PAC AS T1 READ, fec_committee_contributions AS T2 READ, super_PACs_list T3 READ, super_PACs_list T4 READ;"
	sql4 = "INSERT INTO exclusivity_scores (fec_committee_id, contributor_name, total_by_pac, other_id, recipient_name, amount, exclusivity_score) SELECT T.fec_committee_id, T.contributor_name, T.total_by_PAC, T.other_id, T.recipient_name, IF(T.amount > 1 , 1, SUM(T.amount)) AS total_amount, SUM(exclusivity_subscore) AS exclusivity_score FROM (SELECT T1.fec_committee_id, T1.contributor_name, T1.total_by_PAC, T2.other_id, T2.recipient_name, T2.amount, T2.date, T2.amount/T1.total_by_PAC AS exclusivity_subscore FROM fec_committee_contributions T2, total_donated_by_PAC T1 WHERE T1.fec_committee_id = T2.fec_committee_id AND T2.transaction_type = '24K' AND T2.entity_type = 'PAC' AND EXTRACT(YEAR FROM T2.date) >= '2003' AND T2.fec_committee_id NOT IN (SELECT fecid FROM super_PACs_list T3) AND T2.other_id NOT IN (SELECT fecid FROM super_PACs_list T4)) T GROUP BY T.fec_committee_id, T.other_id ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"
   	sql6 = "ALTER TABLE exclusivity_scores ADD INDEX (fec_committee_id, other_id);"
   	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table exclusivity_scores"


def compute_report_type_score(cursor):
	# First, reads into database .csv file containing report type weights to be used for report type score.
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

	# Next, computes how often each report type occurs for each PAC, split by parity.
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

 	# Then, counts how many times each contributor/recipient occurs in database (i.e. how many times contributor donated to recipient.) Join with exclusivity_scores is to make sure we only look at donations associated with transaction type, entity type, date and superPAC constraints.
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

 	# Then, computes how often each report type occurs for a given contributor/recipient pair.
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

	# For each pair and report type, computes report type subscore as frequency of subscore for the pair times weight associated with report type. Overall score is simply sum of all subscores associated with a pair.
	sql1 = "DROP TABLE IF EXISTS unnormalized_report_type_scores;"
	try:
   		cursor.execute(sql1)
   		db.commit()
   		print "sql1 worked"
	except MySQLdb.Error, e:
   		db.rollback()
   		sys.stderr.write(str(e))
   		sys.exit(1)
	sql2 = """ CREATE TABLE unnormalized_report_type_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				report_type_score FLOAT(10));"""
	try:
   		cursor.execute(sql2)
   		db.commit()
   		print "sql2 worked"
	except MySQLdb.Error, e:
   		db.rollback()
   		sys.stderr.write(str(e))
   		sys.exit(1)
	sql3 = "LOCK TABLES unnormalized_report_type_scores WRITE, report_type_weights AS T1 READ, report_type_frequency AS T2 READ;"
	try:
   		cursor.execute(sql3)
   		db.commit()
   		print "sql3 worked"
	except MySQLdb.Error, e:
   		db.rollback()
   		sys.stderr.write(str(e))
   		sys.exit(1)
	sql4 = "INSERT INTO unnormalized_report_type_scores (fec_committee_id, contributor_name, other_id, recipient_name, report_type_score) SELECT T3.fec_committee_id, T3.contributor_name, T3.other_id, T3.recipient_name, SUM(T3.report_type_subscore) AS report_type_score FROM (SELECT T2.fec_committee_id, T2.contributor_name, T2.other_id, T2.recipient_name, T1.report_type, T1.year_parity, T2.d_date, T2.report_type_frequency, T1.weight, T2.report_type_frequency * T1.weight AS report_type_subscore FROM report_type_weights T1, report_type_frequency T2 WHERE T1.report_type = T2.report_type AND T1.year_parity = T2.year_parity) T3 GROUP BY T3.fec_committee_id, T3.other_id ORDER BY NULL;"
	try:
   		cursor.execute(sql4)
   		db.commit()
   		print "sql4 worked"
	except MySQLdb.Error, e:
   		db.rollback()
   		sys.stderr.write(str(e))
   		sys.exit(1)	
	sql5 = "UNLOCK TABLES;"
	try:
   		cursor.execute(sql5)
   		db.commit()
   		print "sql5 worked"
	except MySQLdb.Error, e:
   		db.rollback()
   		sys.stderr.write(str(e))
   		sys.exit(1)
	sql6 = "ALTER TABLE unnormalized_report_type_scores ADD INDEX (report_type_score);" 
	try:
   		cursor.execute(sql6)
   		db.commit()
   		print "sql6 worked"
	except MySQLdb.Error, e:
   		db.rollback()
   		sys.stderr.write(str(e))
   		sys.exit(1)
	#commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table unnormalized_report_type_scores"

	# Finds maximum score in unnormalized_report_type_scores table.
	sql1 = "DROP TABLE IF EXISTS max_report_type_score;"
	sql2 = """ CREATE TABLE max_report_type_score (
				max_report_type_score FLOAT(10));"""
	sql3 = "LOCK TABLES max_report_type_score WRITE, unnormalized_report_type_scores AS T READ;"
	sql4 = "INSERT INTO max_report_type_score (max_report_type_score) SELECT MAX(report_type_score) AS max_report_type_score FROM unnormalized_report_type_scores T;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "" 
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table max_report_type_score"

	# Finally, finds final scores by normalizing scores in table unnormalized_report_type_scores. Normalization is done by simply dividing all scores by maximum score stored in max_report_type_score table, so as to ensure scores fall in a scale from 0 to 1.
	sql1 = "DROP TABLE IF EXISTS report_type_scores;"
	sql2 = """ CREATE TABLE report_type_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				report_type_score FLOAT(10));"""
	sql3 = "LOCK TABLES report_type_scores WRITE, unnormalized_report_type_scores AS T1 READ, max_report_type_score AS T2 READ;"
	sql4 = "INSERT INTO report_type_scores (fec_committee_id, contributor_name, other_id, recipient_name, report_type_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.report_type_score/T2.max_report_type_score AS report_type_score FROM unnormalized_report_type_scores T1, max_report_type_score T2;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE report_type_scores ADD INDEX (fec_committee_id, other_id);"
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table report_type_scores"

def compute_periodicity_score(cursor):
	# Computes unnormalized periodicity score as inverse of variance of dataset made up of donation dates associated with a given pair, where dates are mapped into a DAYOFYEAR data point (i.e. days passed since Jan 1st.)
	sql1 = "DROP TABLE IF EXISTS unnormalized_periodicity_scores;"
	sql2 = """ CREATE TABLE unnormalized_periodicity_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				periodicity_score FLOAT(10));"""
	sql3 = "LOCK TABLES unnormalized_periodicity_scores WRITE, super_PACs_list AS T2 READ, super_PACs_list AS T3 READ, fec_committee_contributions AS T1 READ;"
	sql4 = "INSERT INTO unnormalized_periodicity_scores (fec_committee_id, contributor_name, other_id, recipient_name, periodicity_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, IFNULL(1/VAR_POP(DAYOFYEAR(T1.date)), 0) AS periodicity_score FROM fec_committee_contributions T1 WHERE T1.transaction_type = '24K' AND T1.entity_type = 'PAC' AND EXTRACT(YEAR FROM T1.date) >= '2003' AND T1.fec_committee_id NOT IN (SELECT fecid FROM super_PACs_list T2) AND T1.other_id NOT IN (SELECT fecid FROM super_PACs_list T3) GROUP BY T1.fec_committee_id, T1.other_id ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE unnormalized_periodicity_scores ADD INDEX (periodicity_score);" # Might need to change this, see overall_score!
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table unnormalized_periodicity_scores"

	# Finds maximum score in unnormalized_periodicity_scores table.
	sql1 = "DROP TABLE IF EXISTS max_periodicity_score;"
	sql2 = """ CREATE TABLE max_periodicity_score (
				max_periodicity_score FLOAT(10));"""
	sql3 = "LOCK TABLES max_periodicity_score WRITE, unnormalized_periodicity_scores AS T READ;"
	sql4 = "INSERT INTO max_periodicity_score (max_periodicity_score) SELECT MAX(periodicity_score) AS max_periodicity_score FROM unnormalized_periodicity_scores T;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "" 
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table max_periodicity_score"

	# Finally, finds final scores by normalizing scores in table unnormalized_periodicity_scores. Normalization is done by simply dividing all scores by maximum score stored in max_periodicity_score table, so as to ensure scores fall in a scale from 0 to 1.
	sql1 = "DROP TABLE IF EXISTS periodicity_scores;"
	sql2 = """ CREATE TABLE periodicity_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				periodicity_score FLOAT(10));"""
	sql3 = "LOCK TABLES periodicity_scores WRITE, unnormalized_periodicity_scores AS T1 READ, max_periodicity_score AS T2 READ;"
	sql4 = "INSERT INTO periodicity_scores (fec_committee_id, contributor_name, other_id, recipient_name, periodicity_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.periodicity_score/T2.max_periodicity_score AS periodicity_score FROM unnormalized_periodicity_scores T1, max_periodicity_score T2;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE periodicity_scores ADD INDEX (fec_committee_id, other_id);" # Might need to change this, see overall_score!
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table periodicity_scores"


def compute_race_focus_scores(cursor):
	# Lists all races associated with a given contributor/recipient pair, where race is defined by attributes district, office state, branch and cycle.
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
	print "Table races_list"

	# Computes race focus score as inverse of number of races a contributor donates to. Note that this score is not associated with a contributor/recipient pair, but simply with a contributor.
	# No need for normalization due to methodology adopted. Since score is given the inverse of the count of number of races a PAC donates to, scores already fall on a 0-1 scale.
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


def compute_maxed_out_scores(cursor):
	# Creates 'contributor_types' table in which each contributor (uniquely identified by 'fec_committee_id', also described by 'contributor_name') is assigned a 'contributor_type'.
	# Possible values of 'contributor_type' are: 'national_party', 'other_party', 'multi_pac', 'non_multi_pac'. 
	# Classification is based on the following rules:
	# If committee type is 'X' or 'Y', then contributor is either national party or other party. We use national parties' fecid's to make the distinction as follows: fecid's 'C00003418', 'C00163022', 'C00027466', 'C00075820', 'C00000935', 'C00042366', 'C00010603' are known to be national parties, all others are classified as 'other_party'.
   	# If committee type is one of 'N', 'Q', 'F', then contributor is either multicandidate pac or non multicandidate pac. We use multiqualify date to distinguish between multicand and non multicand pacs.
   	# We ignore all contributors associated with other committee types.
	sql1 = "DROP TABLE IF EXISTS contributor_types;"
	sql2 = """ CREATE TABLE contributor_types (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				cycle CHAR(5),
				contributor_type CHAR(15));"""
	sql3 = "LOCK TABLES contributor_types WRITE, fec_committees AS T READ;"
	sql4 = "INSERT INTO contributor_types (fec_committee_id, contributor_name, cycle, contributor_type) SELECT T.fecid, T.name, T.cycle, CASE WHEN (T.committee_type = 'X' OR T.committee_type = 'Y') THEN CASE WHEN (T.fecid = 'C00003418' OR T.fecid = 'C00163022' OR T.fecid = 'C00027466' OR T.fecid = 'C00075820' OR T.fecid = 'C00000935' OR T.fecid = 'C00042366' OR T.fecid = 'C00010603') THEN 'national_party' ELSE 'other_party' END ELSE CASE WHEN (T.cycle >= EXTRACT(YEAR FROM T.multiqualify_date)) THEN 'multi_pac'ELSE 'non_multi_pac' END END AS contributor_type FROM fec_committees T WHERE T.committee_type = 'X' OR T.committee_type = 'Y' OR T.committee_type = 'N' OR T.committee_type = 'Q' OR T.committee_type = 'F' GROUP BY T.fecid, T.cycle ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE contributor_types ADD INDEX (fec_committee_id, cycle);"
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table contributor_types"

	# Creates 'recipient_types' table in which each recipient (uniquely identified by 'other_id', also described by 'recipient_name') is assigned a 'recipient_type'.
	# Possible values of 'recipient_type' are: 'national_party', 'other_party', 'pac', 'candidate'.
	# Classification is based on the following rules:
	# If committee type is one of 'H', 'S', 'P', 'A', 'B' then recipient is a candidate.
	# If committee type is 'X' or  'Y' then contributors' rules also apply for recipients, see rules for contributor types above.
	# If committee type is one of 'N', 'Q', 'F', 'G', then recipient is a pac.
   	# We ignore all recipients associated with other committee types.
	sql1 = "DROP TABLE IF EXISTS recipient_types;"
	sql2 = """ CREATE TABLE recipient_types (
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				cycle CHAR(5),
				recipient_type CHAR(15));"""
	sql3 = "LOCK TABLES recipient_types WRITE, fec_committees AS T READ;"
	sql4 = "INSERT INTO recipient_types (other_id, recipient_name, cycle, recipient_type) SELECT T.fecid, T.name, T.cycle, CASE WHEN (T.committee_type = 'X' OR T.committee_type = 'Y') THEN CASE WHEN (T.fecid = 'C00003418' OR T.fecid = 'C00163022' OR T.fecid = 'C00027466' OR T.fecid = 'C00075820' OR T.fecid = 'C00000935' OR T.fecid = 'C00042366' OR T.fecid = 'C00010603') THEN 'national_party' ELSE 'other_party' END ELSE CASE WHEN (T.committee_type = 'H' OR T.committee_type = 'S' OR T.committee_type = 'P' OR T.committee_type = 'A' OR T.committee_type = 'B') THEN 'candidate' ELSE 'pac' END END AS contributor_type FROM fec_committees T WHERE T.committee_type = 'X' OR T.committee_type = 'Y' OR T.committee_type = 'H' OR T.committee_type = 'S' OR T.committee_type = 'P' OR T.committee_type = 'A' OR T.committee_type = 'B' OR T.committee_type = 'N' OR T.committee_type = 'Q' OR T.committee_type = 'F' OR T.committee_type = 'G' GROUP BY T.fecid, T.cycle ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE recipient_types ADD INDEX (other_id, cycle);"
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table recipient_types"

	# Reads file limits.csv into database. File contains contribution limits for all combinations of contributor/recipient types
	filename = "limits.csv"
	with open(filename, 'rU') as f:
		rows = list(csv.reader(f))
	try:
		cursor.execute("DROP TABLE IF EXISTS contribution_limits;")
		cursor.execute( """ CREATE TABLE contribution_limits (
				contributor_type CHAR(15) NOT NULL,
				recipient_type CHAR(15) NOT NULL,
				cycle CHAR(5),
				contribution_limit FLOAT(8));""")
		cursor.execute("LOCK TABLES contribution_limits WRITE;")
		db.commit()
	except:
		db.rollback()
	for index, row in enumerate(rows):
		if index == 0:
			recipient_type_1 = row[2];
			recipient_type_2 = row[3];
			recipient_type_3 = row[4];
			recipient_type_4 = row[5];
		else:
			sql1 = "INSERT INTO contribution_limits (contributor_type, recipient_type, cycle, contribution_limit) VALUES ('%s','%s','%s','%s')" % (row[0], recipient_type_1, row[1], row[2])
			sql2 = "INSERT INTO contribution_limits (contributor_type, recipient_type, cycle, contribution_limit) VALUES ('%s','%s','%s','%s')" % (row[0], recipient_type_2, row[1], row[3])
			sql3 = "INSERT INTO contribution_limits (contributor_type, recipient_type, cycle, contribution_limit) VALUES ('%s','%s','%s','%s')" % (row[0], recipient_type_3, row[1], row[4])
			sql4 = "INSERT INTO contribution_limits (contributor_type, recipient_type, cycle, contribution_limit) VALUES ('%s','%s','%s','%s')" % (row[0], recipient_type_4, row[1], row[5])
			try:
	   			cursor.execute(sql1)
	   			cursor.execute(sql2)
	   			cursor.execute(sql3)
	   			cursor.execute(sql4)
	   			db.commit()
			except:
	   			db.rollback()
   	cursor.execute("UNLOCK TABLES;")
   	try:
   		cursor.execute("ALTER TABLE contribution_limits ADD INDEX (contributor_type, recipient_type, cycle);")
   		db.commit()
	except:
   		db.rollback()
   	print "Table contribution_limits"

   	# Joins table containing contributor types and recipient types with the contributions table.
	sql1 = "DROP TABLE IF EXISTS joined_contr_recpt_types;"
	sql2 = """ CREATE TABLE joined_contr_recpt_types (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				contributor_type CHAR(15),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				recipient_type CHAR(15),
				cycle CHAR(5),
				date DATE,
				amount FLOAT(10));"""
	sql3 = "LOCK TABLES joined_contr_recpt_types WRITE, fec_committee_contributions AS T1 READ, contributor_types AS T2 READ, recipient_types AS T3 READ, super_PACs_list AS T4 READ, super_PACs_list AS T5 READ;"
	sql4 = "INSERT INTO joined_contr_recpt_types (fec_committee_id, contributor_name, contributor_type, other_id, recipient_name, recipient_type, cycle, date, amount) SELECT T1.fec_committee_id, T1.contributor_name, T2.contributor_type, T1.other_id, T1.recipient_name, T3.recipient_type, T1.cycle, T1.date, T1.amount FROM fec_committee_contributions T1, contributor_types T2, recipient_types T3 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T3.other_id AND T1.transaction_type = '24K' AND T1.entity_type = 'PAC' AND EXTRACT(YEAR FROM T1.date) >= '2003' AND T1.fec_committee_id NOT IN (SELECT fecid FROM super_PACs_list T4) AND T1.other_id NOT IN (SELECT fecid FROM super_PACs_list T5) GROUP BY T1.fec_committee_id, T1.other_id, T1.date, T1.amount ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE joined_contr_recpt_types ADD INDEX (contributor_type, recipient_type, cycle);"
	#commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table joined_contr_recpt_types"

	# Associates each contributor/recipient pair with a contribution limit based on info from the contribution_limits table and computes maxed out subscore as quotient of amount donated over contribution limit.
	sql1 = "DROP TABLE IF EXISTS maxed_out_subscores;"
	sql2 = """ CREATE TABLE maxed_out_subscores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				contributor_type CHAR(15),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				recipient_type CHAR(15),
				cycle CHAR(5),
				date DATE,
				amount FLOAT(10),
				contribution_limit FLOAT(8),
				maxed_out_subscore FLOAT(10));"""
	sql3 = "LOCK TABLES maxed_out_subscores WRITE, joined_contr_recpt_types AS T1 READ, contribution_limits AS T2 READ;"
	sql4 = "INSERT INTO maxed_out_subscores (fec_committee_id, contributor_name, contributor_type, other_id, recipient_name, recipient_type, cycle, date, amount, contribution_limit, maxed_out_subscore) SELECT DISTINCT T1.fec_committee_id, T1.contributor_name, T1.contributor_type, T1.other_id, T1.recipient_name, T1.recipient_type, T1.cycle, T1.date, T1.amount, T2.contribution_limit, T1.amount/T2.contribution_limit AS maxed_out_subscore FROM joined_contr_recpt_types T1, contribution_limits T2 WHERE T1.contributor_type = T2.contributor_type AND T1.recipient_type = T2.recipient_type AND T1.cycle = T2.cycle;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE maxed_out_subscores ADD INDEX (fec_committee_id, other_id, cycle);"
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table maxed_out_subscores"

	# Computes unnormalized maxed out score for a given contributor/recipient pair by summing over all subscore associated with pair.
	sql1 = "DROP TABLE IF EXISTS unnormalized_maxed_out_scores;"
	sql2 = """ CREATE TABLE unnormalized_maxed_out_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				contributor_type CHAR(15),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				recipient_type CHAR(15),
				maxed_out_score FLOAT(10));"""
	sql3 = "LOCK TABLES unnormalized_maxed_out_scores WRITE, maxed_out_subscores AS T1 READ;"
	sql4 = "INSERT INTO unnormalized_maxed_out_scores (fec_committee_id, contributor_name, contributor_type, other_id, recipient_name, recipient_type, maxed_out_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.contributor_type, T1.other_id, T1.recipient_name, T1.recipient_type, SUM(T1.maxed_out_subscore) AS maxed_out_score FROM maxed_out_subscores T1 GROUP BY T1.fec_committee_id, T1.other_id ORDER BY NULL;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE unnormalized_maxed_out_scores ADD INDEX (maxed_out_score);"
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table unnormalized_maxed_out_scores"

	# Finds maximum score in unnormalized_maxed_out_scores table.
	sql1 = "DROP TABLE IF EXISTS max_maxed_out_score;"
	sql2 = """ CREATE TABLE max_maxed_out_score (
				max_maxed_out_score FLOAT(10));"""
	sql3 = "LOCK TABLES max_maxed_out_score WRITE, unnormalized_maxed_out_scores AS T READ;"
	sql4 = "INSERT INTO max_maxed_out_score (max_maxed_out_score) SELECT MAX(maxed_out_score) AS max_maxed_out_score FROM unnormalized_maxed_out_scores T;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "" 
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table max_maxed_out_score"

	# Finally, finds final scores by normalizing scores in table unnormalized_maxed_out_scores. Normalization is done by simply dividing all scores by maximum score stored in max_maxed_out_score table, so as to ensure scores fall in a scale from 0 to 1.
	sql1 = "DROP TABLE IF EXISTS maxed_out_scores;"
	sql2 = """ CREATE TABLE maxed_out_scores (
				fec_committee_id CHAR(9) NOT NULL,
				contributor_name CHAR(200),
				contributor_type CHAR(15),
				other_id CHAR(9) NOT NULL,
				recipient_name CHAR(200),
				recipient_type CHAR(15),
				maxed_out_score FLOAT(10));"""
	sql3 = "LOCK TABLES maxed_out_scores WRITE, unnormalized_maxed_out_scores AS T1 READ, max_maxed_out_score AS T2 READ;"
	sql4 = "INSERT INTO maxed_out_scores (fec_committee_id, contributor_name, contributor_type, other_id, recipient_name, recipient_type, maxed_out_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.contributor_type, T1.other_id, T1.recipient_name, T1.recipient_type, T1.maxed_out_score/T2.max_maxed_out_score AS maxed_out_score FROM unnormalized_maxed_out_scores T1, max_maxed_out_score T2;"
	sql5 = "UNLOCK TABLES;"
	sql6 = "ALTER TABLE maxed_out_scores ADD INDEX (fec_committee_id, other_id);"
	commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6)
	print "Table maxed_out_scores"


def commit_changes(cursor, sql1, sql2, sql3, sql4, sql5, sql6):
	try:
   		cursor.execute(sql1)
   		cursor.execute(sql2)
   		cursor.execute(sql3)
   		cursor.execute(sql4)
   		cursor.execute(sql5)
   		if sql6 != "":
   			cursor.execute(sql6)
   		db.commit()
	except MySQLdb.Error, e:
   		db.rollback()
   		sys.stderr.write(str(e))
   		sys.exit(1)


def usage():
	sys.stderr.write("""
		Usage:
		python main.py <Name of Database> \n
		Example:
		python main.py fec
		\n""")

if __name__ == "__main__":
	if len(sys.argv) == 2:
		db = MySQLdb.connect(host="localhost", port=3306, user="root",passwd="",db=sys.argv[1]) # make sure db argument matches name of database where fec_committee_contributions.sql is stored
		main()
	else:
		usage()
		sys.exit(1)
