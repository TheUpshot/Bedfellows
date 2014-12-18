import csv, sys
from sys import stdout
from pandas import *
import MySQLdb
import numpy as np

from main import commit_changes, handle_error, check_contributor_id, check_recipient_id

INFINITY = 9999999999999


def main(db):
    cursor = db.cursor()
    print "Overall scores"

    option = raw_input("Do you want to compute scores or perform a similarity analysis of scores already computed? Enter 'compute', 'analyze' or 'both'.\n")

    if option == "compute" or option == "both":
        initial_setup(db, cursor)
        compute_exclusivity_scores(db, cursor)                     # 1st score         # bumps up scores of donations made exclusively to a given recipient
        compute_report_type_scores(db, cursor)                      # 2nd score         # bumps up scores according to how early in election cycle donations were made
        compute_periodicity_scores(db, cursor)                     # 3rd score         # bumps up scores if donations are made around the same time of the year
        compute_maxed_out_scores(db, cursor)                       # 4th score         # bumps up scores if contributors maxed out on donations to corresponding recipient
        compute_length_scores(db, cursor)                           # 5th score         # bumps up scores if contributor has been donating to recipient for a long time
        compute_race_focus_scores(db, cursor)                      # 6th score         # bumps up scores according to geographical proximity
        compute_final_scores(db, cursor)                           # Sum of scores     # computes weighted sum of all scores

    if option == "analyze" or option == "both":
        similarity_analysis(db, cursor)

    db.close()


def initial_setup(db, cursor):
    # Reads into database table with ID's of super PACs to be excluded from this analysis.
    csv_data = csv.reader(file('superPACs.csv', 'rU'))
    sql = []
    cursor.execute("DROP TABLE IF EXISTS super_PACs_list;")
    cursor.execute("""CREATE TABLE super_PACs_list (
                num CHAR(9) NOT NULL,
                fecid CHAR(11) NOT NULL,
                name VARCHAR(255) NOT NULL,
                filer VARCHAR(255) NOT NULL);""")
    sql.append("LOCK TABLES super_PACs_list WRITE, fec_committees AS T READ;")
    cursor.execute("LOAD DATA LOCAL INFILE 'superPACs.csv' into TABLE super_PACs_list fields terminated by ',' OPTIONALLY ENCLOSED BY '\"' lines terminated by '\r';")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE super_PACs_list ADD INDEX fecid (fecid);")
    commit_changes(db, cursor, sql)
    print "Table super_PACs_list"

    # Adds indexes to fec_committee_contributions before saving a constrained subset as fec_contributions.
    try:
        cursor.execute("ALTER TABLE fec_committee_contributions ADD INDEX combo (transaction_type, entity_type, date, fec_committee_id, other_id);")
    except MySQLdb.Error, e:
        handle_error(db, e)

    # Constrains FEC's fec_committee_contributions table to our needs: select subset of attributes that will be useful in queries, constrain on transaction type '24K', entity type 'PAC', year 2003 or later, contributor and recipient not present in list of super PACs.
    sql = []
    sql.append("DROP TABLE IF EXISTS fec_contributions;")
    sql.append(""" CREATE TABLE fec_contributions (
                fec_committee_id CHAR(9) NOT NULL,
                report_type CHAR(5),
                contributor_name CHAR(200),
                date DATE,
                amount CHAR(10),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                cycle CHAR(5));""")
    sql.append("LOCK TABLES fec_contributions WRITE, fec_committee_contributions AS T READ, super_PACs_list AS T1 READ, super_PACs_list AS T2 READ;")
    sql.append("INSERT INTO fec_contributions (fec_committee_id, report_type, contributor_name, date, amount, other_id, recipient_name, cycle) SELECT T.fec_committee_id, T.report_type, T.contributor_name, T.date, T.amount, T.other_id, T.recipient_name, T.cycle FROM fec_committee_contributions T WHERE T.transaction_type = '24K' AND (T.entity_type = 'PAC' OR T.entity_type = 'CCM') AND EXTRACT(YEAR FROM T.date) >= '2003' AND T.fec_committee_id NOT IN (SELECT fecid FROM super_PACs_list T1) AND T.other_id NOT IN (SELECT fecid FROM super_PACs_list T2);")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE fec_contributions ADD INDEX pair (fec_committee_id, other_id);")
    commit_changes(db, cursor, sql)
    print "Table fec_contributions"

    try:
        setup_initial_indexes(db, cursor)
    except MySQLdb.Error, e:
        handle_error(db, e)
    print "Initial setup done"


def setup_initial_indexes(db, cursor):
    sql = []
    sql.append("ALTER TABLE fec_contributions ADD INDEX pair_report_type (fec_committee_id, other_id, report_type);")
    sql.append("ALTER TABLE fec_contributions ADD INDEX cycle_pair (cycle, fec_committee_id, other_id);")
    sql.append("ALTER TABLE fec_contributions ADD INDEX other_id (other_id);")
    sql.append("ALTER TABLE fec_contributions ADD INDEX pair_cycle_date (fec_committee_id, cycle, other_id, contributor_name, recipient_name, date, amount);")
    sql.append("ALTER TABLE fec_committees ADD INDEX fecid (fecid);")
    sql.append("ALTER TABLE fec_committees ADD INDEX pair (fec_candidate_id, fecid);")
    sql.append("ALTER TABLE fec_committees ADD INDEX super_pac (is_super_PAC);")
    sql.append("ALTER TABLE fec_committees ADD INDEX committee_type (committee_type);")
    sql.append("ALTER TABLE fec_committees ADD INDEX cycle (cycle);")
    sql.append("ALTER TABLE fec_candidates ADD INDEX combo (fecid, name, district, office_state, branch, cycle);")
    commit_changes(db, cursor, sql)


def compute_exclusivity_scores(db, cursor):
    # First, computes total amount donated by a given PAC contributor across all recipients.
    sql = []
    sql.append("DROP TABLE IF EXISTS total_donated_by_contributor;")
    sql.append( """ CREATE TABLE total_donated_by_contributor (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                total_by_PAC FLOAT(20));""")
    sql.append("LOCK TABLES total_donated_by_contributor WRITE, fec_contributions AS T READ;")
    sql.append("INSERT INTO total_donated_by_contributor (fec_committee_id, contributor_name, total_by_PAC) SELECT T.fec_committee_id, T.contributor_name, SUM(T.amount) AS total_by_PAC FROM fec_contributions T GROUP BY T.fec_committee_id ORDER BY NULL;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE total_donated_by_contributor ADD INDEX combo (fec_committee_id, contributor_name, total_by_PAC);")
    commit_changes(db, cursor, sql)
    print "Table total_donated_by_contributor"

    # Then, computes exclusivity score for a given contributor/recipient pair. Score is calculated as follows: amount given to recipient as percentage of total donated by contributor.
    # No need for normalization because scores are capped at 1, so 0-1 scale is already enforced.
    sql = []
    sql.append("DROP TABLE IF EXISTS exclusivity_scores;")
    sql.append(""" CREATE TABLE exclusivity_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                total_by_pac CHAR(10),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                amount CHAR(10),
                exclusivity_score FLOAT(20));""")
    sql.append("LOCK TABLES exclusivity_scores WRITE, total_donated_by_contributor AS T1 READ, fec_contributions AS T2 READ;")
    sql.append("INSERT INTO exclusivity_scores (fec_committee_id, contributor_name, total_by_pac, other_id, recipient_name, amount, exclusivity_score) SELECT T.fec_committee_id, T.contributor_name, T.total_by_PAC, T.other_id, T.recipient_name, SUM(T.amount) AS total_amount, IF(SUM(exclusivity_subscore) > 1, 1, SUM(exclusivity_subscore)) AS exclusivity_score FROM (SELECT T1.fec_committee_id, T1.contributor_name, T1.total_by_PAC, T2.other_id, T2.recipient_name, T2.amount, T2.date, T2.amount/T1.total_by_PAC AS exclusivity_subscore FROM fec_contributions T2, total_donated_by_contributor T1 WHERE T1.fec_committee_id = T2.fec_committee_id) T GROUP BY T.fec_committee_id, T.other_id ORDER BY NULL;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE exclusivity_scores ADD INDEX pairs (fec_committee_id, other_id, contributor_name);")
    commit_changes(db, cursor, sql)
    print "Table exclusivity_scores"


def compute_report_type_scores(db, cursor):
    # First, reads into database .csv file containing report type weights to be used for report type score.
    filename = "report_types.csv"
    with open(filename, 'rU') as f:
        rows = list(csv.reader(f))
    try:
        cursor.execute("DROP TABLE IF EXISTS report_type_weights;")
        cursor.execute( """ CREATE TABLE report_type_weights (
                report_type CHAR(5) NOT NULL,
                year_parity CHAR(5),
                weight INT(2));""")
        cursor.execute("LOCK TABLES report_type_weights WRITE;")
        db.commit()
    except MySQLdb.Error, e:
        handle_error(db, e)
    for index, r in enumerate(rows):
    	try:
            if index != 0:
               cursor.execute("INSERT INTO report_type_weights (report_type, year_parity, weight) VALUES ('%s','%s','%s')" % (r[0], r[1], r[2]))
               db.commit()
        except MySQLdb.Error, e:
            handle_error(db, e)
    try:
        cursor.execute("UNLOCK TABLES;")
        cursor.execute("ALTER TABLE report_type_weights ADD INDEX weights (report_type, year_parity, weight);")
        db.commit()
    except MySQLdb.Error, e:
        handle_error(db, e)
    print "Table report_type_weights"

    # Next, computes how often each report type occurs for each pair, split by parity.
    sql = []
    sql.append("DROP TABLE IF EXISTS report_type_count_by_pair;")
    sql.append(""" CREATE TABLE report_type_count_by_pair (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                report_type CHAR(4),
                year_parity CHAR(5),
                d_date DATE,
                count INT(10));""")
    sql.append("LOCK TABLES report_type_count_by_pair WRITE, exclusivity_scores AS T1 READ, fec_contributions AS T2 READ;")
    sql.append("INSERT INTO report_type_count_by_pair (fec_committee_id, contributor_name, other_id, recipient_name, report_type, year_parity, d_date, count) SELECT T1.fec_committee_id, T1.contributor_name, T2.other_id, T2.recipient_name, T2.report_type, IF(MOD(EXTRACT(YEAR FROM T2.date), 2) = 0, 'even', 'odd') AS year_parity, T2.date, count(*) FROM fec_contributions AS T2, exclusivity_scores AS T1 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id GROUP BY T2.fec_committee_id, T2.other_id, T2.report_type ORDER BY NULL;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE report_type_count_by_pair ADD INDEX (fec_committee_id, other_id);")
    commit_changes(db, cursor, sql)
    print "Table report_type_count_by_pair"

    # Then, counts how many times each contributor/recipient pair occurs in database (i.e. how many times contributor donated to recipient.)
    sql = []
    sql.append("DROP TABLE IF EXISTS pairs_count;")
    sql.append(""" CREATE TABLE pairs_count (
                fec_committee_id CHAR(9) NOT NULL,
                other_id CHAR(9) NOT NULL,
                count INT(10));""")
    sql.append("LOCK TABLES pairs_count WRITE, fec_contributions AS T READ;")
    sql.append("INSERT INTO pairs_count (fec_committee_id, other_id, count) SELECT T.fec_committee_id, T.other_id, count(*) FROM fec_contributions T GROUP BY T.fec_committee_id, T.other_id ORDER BY NULL;" )
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE pairs_count ADD INDEX (fec_committee_id, other_id, count);")
    commit_changes(db, cursor, sql)
    print "Table pairs_count"

    # Then, computes how often each report type occurs for a given contributor/recipient pair.
    sql = []
    sql.append("DROP TABLE IF EXISTS report_type_frequency;")
    sql.append(""" CREATE TABLE report_type_frequency (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                report_type CHAR(4),
                year_parity CHAR(5),
                d_date DATE,
                report_type_count_by_pair CHAR(10),
                pairs_count INT(10),
                report_type_frequency FLOAT(20));""")
    sql.append("LOCK TABLES report_type_frequency WRITE, report_type_count_by_pair AS T1 READ, pairs_count AS T2 READ;")
    sql.append("INSERT INTO report_type_frequency (fec_committee_id, contributor_name, other_id, recipient_name, report_type, year_parity, d_date, report_type_count_by_pair, pairs_count, report_type_frequency) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.report_type, T1.year_parity, T1.d_date, T1.count AS report_type_count_by_pair, T2.count AS pairs_count, T1.count/T2.count AS report_type_frequency FROM report_type_count_by_pair T1, pairs_count T2 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE report_type_frequency ADD INDEX (report_type, year_parity, fec_committee_id, contributor_name, other_id, recipient_name, report_type_frequency);")
    commit_changes(db, cursor, sql)
    print "Table report_type_frequency"

    # For each pair and report type, computes report type subscore as frequency of subscore for the pair times weight associated with report type. Overall score is simply sum of all subscores associated with a pair.
    sql = []
    sql.append("DROP TABLE IF EXISTS unnormalized_report_type_scores;")
    sql.append(""" CREATE TABLE unnormalized_report_type_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                report_type_score FLOAT(20));""")
    sql.append("LOCK TABLES unnormalized_report_type_scores WRITE, report_type_weights AS T1 READ, report_type_frequency AS T2 READ;")
    sql.append("INSERT INTO unnormalized_report_type_scores (fec_committee_id, contributor_name, other_id, recipient_name, report_type_score) SELECT T3.fec_committee_id, T3.contributor_name, T3.other_id, T3.recipient_name, SUM(T3.report_type_subscore) AS report_type_score FROM (SELECT T2.fec_committee_id, T2.contributor_name, T2.other_id, T2.recipient_name, T2.report_type_frequency * T1.weight AS report_type_subscore FROM report_type_weights T1, report_type_frequency T2 WHERE T1.report_type = T2.report_type AND T1.year_parity = T2.year_parity) T3 GROUP BY T3.fec_committee_id, T3.other_id ORDER BY NULL;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE unnormalized_report_type_scores ADD INDEX (fec_committee_id, contributor_name, other_id, recipient_name, report_type_score);")
    commit_changes(db, cursor, sql)
    print "Table unnormalized_report_type_scores"

    # Finds maximum score in unnormalized_report_type_scores table.
    sql = []
    sql.append("DROP TABLE IF EXISTS max_report_type_score;")
    sql.append(""" CREATE TABLE max_report_type_score (
                max_report_type_score FLOAT(20));""")
    sql.append("LOCK TABLES max_report_type_score WRITE, unnormalized_report_type_scores AS T READ;")
    sql.append("INSERT INTO max_report_type_score (max_report_type_score) SELECT MAX(report_type_score) AS max_report_type_score FROM unnormalized_report_type_scores T;")
    sql.append("UNLOCK TABLES;")
    commit_changes(db, cursor, sql)
    print "Table max_report_type_score"

    # Finally, finds final scores by normalizing scores in table unnormalized_report_type_scores. Normalization is done by simply dividing all scores by maximum score stored in max_report_type_score table, so as to ensure scores fall in a scale from 0 to 1.
    sql = []
    sql.append("DROP TABLE IF EXISTS report_type_scores;")
    sql.append(""" CREATE TABLE report_type_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                report_type_score FLOAT(20));""")
    sql.append("LOCK TABLES report_type_scores WRITE, unnormalized_report_type_scores AS T1 READ, max_report_type_score AS T2 READ;")
    sql.append("INSERT INTO report_type_scores (fec_committee_id, contributor_name, other_id, recipient_name, report_type_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.report_type_score/T2.max_report_type_score AS report_type_score FROM unnormalized_report_type_scores T1, max_report_type_score T2;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE report_type_scores ADD INDEX (fec_committee_id, other_id);")
    commit_changes(db, cursor, sql)
    print "Table report_type_scores"

def compute_periodicity_scores(db, cursor):
    # Computes unnormalized periodicity score as inverse of variance of dataset made up of donation dates associated with a given pair, where dates are mapped into a DAYOFYEAR data point (i.e. days passed since Jan 1st.)
    sql = []
    sql.append("DROP TABLE IF EXISTS unnormalized_periodicity_scores;")
    sql.append(""" CREATE TABLE unnormalized_periodicity_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                stddev_pop FLOAT(20),
                day_diff INT(4),
                periodicity_score FLOAT(20));""")
    sql.append("LOCK TABLES unnormalized_periodicity_scores WRITE, fec_contributions AS T1 READ;")
    sql.append("INSERT INTO unnormalized_periodicity_scores (fec_committee_id, contributor_name, other_id, recipient_name, stddev_pop, day_diff, periodicity_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, STDDEV_POP(DAYOFYEAR(T1.date)) AS stddev_pop, MAX(DAYOFYEAR(T1.date)) - MIN(DAYOFYEAR(T1.date)) AS day_diff, IF(STDDEV_POP(DAYOFYEAR(T1.date)) = 0, IF(COUNT(DISTINCT(T1.date)) > 1, 1.0, 0), IFNULL(1.0/STDDEV_POP(DAYOFYEAR(T1.date)), 0)) AS periodicity_score FROM fec_contributions T1 GROUP BY T1.fec_committee_id, T1.other_id ORDER BY NULL;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE unnormalized_periodicity_scores ADD INDEX (fec_committee_id, contributor_name, other_id, recipient_name, periodicity_score);")
    commit_changes(db, cursor, sql)
    print "Table unnormalized_periodicity_scores"

    # Finds maximum score in unnormalized_periodicity_scores table.
    sql = []
    sql.append("DROP TABLE IF EXISTS cap_unnormalized_score;")
    sql.append(""" CREATE TABLE cap_unnormalized_score (
                cap_unnormalized_score FLOAT(20));""")
    sql.append("LOCK TABLES cap_unnormalized_score WRITE, unnormalized_periodicity_scores AS T READ;")
    sql.append("INSERT INTO cap_unnormalized_score (cap_unnormalized_score) SELECT MIN(T.periodicity_score) FROM unnormalized_periodicity_scores T WHERE day_diff = 1;")
    sql.append("UNLOCK TABLES;")
    commit_changes(db, cursor, sql)
    print "Table cap_unnormalized_score"

    # Finally, finds final scores by normalizing scores in table unnormalized_periodicity_scores. Normalization is done by simply dividing all scores by maximum score stored in cap_unnormalized_score table, so as to ensure scores fall in a scale from 0 to 1.
    sql = []
    sql.append("DROP TABLE IF EXISTS periodicity_scores;")
    sql.append(""" CREATE TABLE periodicity_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                periodicity_score FLOAT(20));""")
    sql.append("LOCK TABLES periodicity_scores WRITE, unnormalized_periodicity_scores AS T1 READ, cap_unnormalized_score AS T2 READ;")
    sql.append("INSERT INTO periodicity_scores (fec_committee_id, contributor_name, other_id, recipient_name, periodicity_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, IF(T1.periodicity_score/T2.cap_unnormalized_score > 1, 1, T1.periodicity_score/T2.cap_unnormalized_score) AS periodicity_score FROM unnormalized_periodicity_scores T1, cap_unnormalized_score T2;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE periodicity_scores ADD INDEX (fec_committee_id, other_id);")
    commit_changes(db, cursor, sql)
    print "Table periodicity_scores"


def compute_maxed_out_scores(db, cursor):
    # Creates 'contributor_types' table in which each contributor (uniquely identified by 'fec_committee_id', also described by 'contributor_name') is assigned a 'contributor_type'.
    # Possible values of 'contributor_type' are: 'national_party', 'other_party', 'multi_pac', 'non_multi_pac'.
    # Classification is based on the following rules:
    # If committee type is 'X' or 'Y', then contributor is either national party or other party. We use national parties' fecid's to make the distinction as follows: fecid's 'C00003418', 'C00163022', 'C00027466', 'C00075820', 'C00000935', 'C00042366', 'C00010603' are known to be national parties, all others are classified as 'other_party'.
    # If committee type is one of 'N', 'Q', 'F', then contributor is either multicandidate pac or non multicandidate pac. We use multiqualify date to distinguish between multicand and non multicand pacs.
    # We ignore all contributors associated with other committee types.
    sql = []
    sql.append("DROP TABLE IF EXISTS contributor_types;")
    sql.append(""" CREATE TABLE contributor_types (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                cycle CHAR(5),
                contributor_type CHAR(15));""")
    sql.append("LOCK TABLES contributor_types WRITE, fec_committees AS T READ;")
    sql.append("INSERT INTO contributor_types (fec_committee_id, contributor_name, cycle, contributor_type) SELECT T.fecid, T.name, T.cycle, CASE WHEN (T.committee_type = 'X' OR T.committee_type = 'Y') THEN CASE WHEN (T.fecid = 'C00003418' OR T.fecid = 'C00163022' OR T.fecid = 'C00027466' OR T.fecid = 'C00075820' OR T.fecid = 'C00000935' OR T.fecid = 'C00042366' OR T.fecid = 'C00010603') THEN 'national_party' ELSE 'other_party' END ELSE CASE WHEN (T.cycle >= EXTRACT(YEAR FROM T.multiqualify_date)) THEN 'multi_pac'ELSE 'non_multi_pac' END END AS contributor_type FROM fec_committees T WHERE T.committee_type = 'X' OR T.committee_type = 'Y' OR T.committee_type = 'N' OR T.committee_type = 'Q' OR T.committee_type = 'F' GROUP BY T.fecid, T.cycle ORDER BY NULL;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE contributor_types ADD INDEX (fec_committee_id, cycle);")
    commit_changes(db, cursor, sql)
    print "Table contributor_types"

    # Creates 'recipient_types' table in which each recipient (uniquely identified by 'other_id', also described by 'recipient_name') is assigned a 'recipient_type'.
    # Possible values of 'recipient_type' are: 'national_party', 'other_party', 'pac', 'candidate'.
    # Classification is based on the following rules:
    # If committee type is one of 'H', 'S', 'P', 'A', 'B' then recipient is a candidate.
    # If committee type is 'X' or  'Y' then contributors' rules also apply for recipients, see rules for contributor types above.
    # If committee type is one of 'N', 'Q', 'F', 'G', then recipient is a pac.
    # We ignore all recipients associated with other committee types.
    sql = []
    sql.append("DROP TABLE IF EXISTS recipient_types;")
    sql.append(""" CREATE TABLE recipient_types (
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                cycle CHAR(5),
                recipient_type CHAR(15));""")
    sql.append("LOCK TABLES recipient_types WRITE, fec_committees AS T READ;")
    sql.append("INSERT INTO recipient_types (other_id, recipient_name, cycle, recipient_type) SELECT T.fecid, T.name, T.cycle, CASE WHEN (T.committee_type = 'X' OR T.committee_type = 'Y') THEN CASE WHEN (T.fecid = 'C00003418' OR T.fecid = 'C00163022' OR T.fecid = 'C00027466' OR T.fecid = 'C00075820' OR T.fecid = 'C00000935' OR T.fecid = 'C00042366' OR T.fecid = 'C00010603') THEN 'national_party' ELSE 'other_party' END ELSE CASE WHEN (T.committee_type = 'H' OR T.committee_type = 'S' OR T.committee_type = 'P' OR T.committee_type = 'A' OR T.committee_type = 'B') THEN 'candidate' ELSE 'pac' END END AS contributor_type FROM fec_committees T WHERE T.committee_type = 'X' OR T.committee_type = 'Y' OR T.committee_type = 'H' OR T.committee_type = 'S' OR T.committee_type = 'P' OR T.committee_type = 'A' OR T.committee_type = 'B' OR T.committee_type = 'N' OR T.committee_type = 'Q' OR T.committee_type = 'F' OR T.committee_type = 'G' GROUP BY T.fecid, T.cycle ORDER BY NULL;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE recipient_types ADD INDEX (other_id, cycle);")
    commit_changes(db, cursor, sql)
    print "Table recipient_types"

    # Reads file limits.csv into database. File contains contribution limits for all combinations of contributor/recipient types.
    filename = "limits.csv"
    with open(filename, 'rU') as f:
        rows = list(csv.reader(f))
    try:
        cursor.execute("DROP TABLE IF EXISTS contribution_limits;")
        cursor.execute( """ CREATE TABLE contribution_limits (
                contributor_type CHAR(15) NOT NULL,
                recipient_type CHAR(15) NOT NULL,
                cycle CHAR(5),
                contribution_limit FLOAT(10));""")
        cursor.execute("LOCK TABLES contribution_limits WRITE;")
        db.commit()
    except MySQLdb.Error, e:
        handle_error(db, e)
    for index, row in enumerate(rows):
        if index == 0:
            recipient_type_1 = row[2]
            recipient_type_2 = row[3]
            recipient_type_3 = row[4]
            recipient_type_4 = row[5]
        else:
            if row[3] == "no limit":
                row[3] = INFINITY
            if row[4] == "no limit":
                row[4] = INFINITY
            cursor.execute("INSERT INTO contribution_limits (contributor_type, recipient_type, cycle, contribution_limit) VALUES ('%s','%s','%s','%s')" % (row[0], recipient_type_1, row[1], row[2]))
            cursor.execute("INSERT INTO contribution_limits (contributor_type, recipient_type, cycle, contribution_limit) VALUES ('%s','%s','%s','%s')" % (row[0], recipient_type_2, row[1], row[3]))
            cursor.execute("INSERT INTO contribution_limits (contributor_type, recipient_type, cycle, contribution_limit) VALUES ('%s','%s','%s','%s')" % (row[0], recipient_type_3, row[1], row[4]))
            cursor.execute("INSERT INTO contribution_limits (contributor_type, recipient_type, cycle, contribution_limit) VALUES ('%s','%s','%s','%s')" % (row[0], recipient_type_4, row[1], row[5]))
            try:
                db.commit()
            except MySQLdb.Error, e:
                handle_error(db, e)
    try:
        cursor.execute("UNLOCK TABLES;")
        cursor.execute("ALTER TABLE contribution_limits ADD INDEX (contributor_type, recipient_type, cycle, contribution_limit);")
        db.commit()
    except MySQLdb.Error, e:
        handle_error(db, e)
    print "Table contribution_limits"

    # Joins table containing contributor types and recipient types with the contributions table.
    sql = []
    sql.append("DROP TABLE IF EXISTS joined_contr_recpt_types;")
    sql.append(""" CREATE TABLE joined_contr_recpt_types (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                contributor_type CHAR(15),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                recipient_type CHAR(15),
                cycle CHAR(5),
                date DATE,
                amount FLOAT(20));""")
    sql.append("LOCK TABLES joined_contr_recpt_types WRITE, fec_contributions AS T1 READ, contributor_types AS T2 READ, recipient_types AS T3 READ;")
    sql.append("INSERT INTO joined_contr_recpt_types (fec_committee_id, contributor_name, contributor_type, other_id, recipient_name, recipient_type, cycle, date, amount) SELECT T1.fec_committee_id, T1.contributor_name, T2.contributor_type, T1.other_id, T1.recipient_name, T3.recipient_type, T1.cycle, T1.date, T1.amount FROM fec_contributions T1, contributor_types T2, recipient_types T3 WHERE T1.fec_committee_id = T2.fec_committee_id  AND T1.cycle = T2.cycle AND T1.cycle = T3.cycle AND T1.other_id = T3.other_id;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE joined_contr_recpt_types ADD INDEX (contributor_type, recipient_type, cycle);")
    commit_changes(db, cursor, sql)
    print "Table joined_contr_recpt_types"

    # Associates each contributor/recipient pair with a contribution limit based on info from the contribution_limits table and computes maxed out subscore as quotient of amount donated over contribution limit.
    sql = []
    sql.append("DROP TABLE IF EXISTS maxed_out_subscores;")
    sql.append(""" CREATE TABLE maxed_out_subscores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                contributor_type CHAR(15),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                recipient_type CHAR(15),
                cycle CHAR(5),
                date DATE,
                amount FLOAT(20),
                contribution_limit FLOAT(10),
                maxed_out_subscore FLOAT(20));""")
    sql.append("LOCK TABLES maxed_out_subscores WRITE, joined_contr_recpt_types AS T1 READ, contribution_limits AS T2 READ;")
    sql.append("INSERT INTO maxed_out_subscores (fec_committee_id, contributor_name, contributor_type, other_id, recipient_name, recipient_type, cycle, date, amount, contribution_limit, maxed_out_subscore) SELECT T1.fec_committee_id, T1.contributor_name, T1.contributor_type, T1.other_id, T1.recipient_name, T1.recipient_type, T1.cycle, T1.date, T1.amount, T2.contribution_limit, T1.amount/T2.contribution_limit AS maxed_out_subscore FROM joined_contr_recpt_types T1, contribution_limits T2 WHERE T1.contributor_type = T2.contributor_type AND T1.recipient_type = T2.recipient_type AND T1.cycle = T2.cycle;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE maxed_out_subscores ADD INDEX (fec_committee_id, other_id, cycle);")
    commit_changes(db, cursor, sql)
    print "Table maxed_out_subscores"

    # Remove from consideration contributors and recipients with subscore > 1, as we're not interested in these cases.
    sql = []
    sql.append("DROP TABLE IF EXISTS inbound_maxed_out_subscores;")
    sql.append(""" CREATE TABLE inbound_maxed_out_subscores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                contributor_type CHAR(15),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                recipient_type CHAR(15),
                cycle CHAR(5),
                date DATE,
                amount FLOAT(20),
                contribution_limit FLOAT(10),
                maxed_out_subscore FLOAT(20));""")
    sql.append("LOCK TABLES inbound_maxed_out_subscores WRITE, maxed_out_subscores AS T READ, maxed_out_subscores AS T1 READ, maxed_out_subscores AS T2 READ;")
    sql.append("INSERT INTO inbound_maxed_out_subscores (fec_committee_id, contributor_name, contributor_type, other_id, recipient_name, recipient_type, cycle, date, amount, contribution_limit, maxed_out_subscore) SELECT * FROM maxed_out_subscores T WHERE T.fec_committee_id NOT IN (SELECT T1.fec_committee_id FROM maxed_out_subscores T1 WHERE maxed_out_subscore > 1) AND T.other_id NOT IN (SELECT T2.other_id FROM maxed_out_subscores T2 WHERE maxed_out_subscore > 1);")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE inbound_maxed_out_subscores ADD INDEX (fec_committee_id, other_id, cycle);")
    commit_changes(db, cursor, sql)
    print "Table inbound_maxed_out_subscores"

    # Computes unnormalized maxed out score for a given contributor/recipient pair by summing over all subscore associated with pair.
    sql = []
    sql.append("DROP TABLE IF EXISTS unnormalized_maxed_out_scores;")
    sql.append(""" CREATE TABLE unnormalized_maxed_out_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                contributor_type CHAR(15),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                recipient_type CHAR(18),
                maxed_out_score FLOAT(20));""")
    sql.append("LOCK TABLES unnormalized_maxed_out_scores WRITE, inbound_maxed_out_subscores AS T1 READ;")
    sql.append("INSERT INTO unnormalized_maxed_out_scores (fec_committee_id, contributor_name, contributor_type, other_id, recipient_name, recipient_type, maxed_out_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.contributor_type, T1.other_id, T1.recipient_name, T1.recipient_type, SUM(T1.maxed_out_subscore) AS maxed_out_score FROM inbound_maxed_out_subscores T1 GROUP BY T1.fec_committee_id, T1.other_id ORDER BY NULL;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE unnormalized_maxed_out_scores ADD INDEX (fec_committee_id, contributor_name, contributor_type, other_id, recipient_name, recipient_type, maxed_out_score);")
    commit_changes(db, cursor, sql)
    print "Table unnormalized_maxed_out_scores"

    # Finds maximum score in unnormalized_maxed_out_scores table.
    sql = []
    sql.append("DROP TABLE IF EXISTS max_maxed_out_score;")
    sql.append(""" CREATE TABLE max_maxed_out_score (
                max_maxed_out_score FLOAT(20));""")
    sql.append("LOCK TABLES max_maxed_out_score WRITE, unnormalized_maxed_out_scores AS T READ;")
    sql.append("INSERT INTO max_maxed_out_score (max_maxed_out_score) SELECT MAX(maxed_out_score) AS max_maxed_out_score FROM unnormalized_maxed_out_scores T;")
    sql.append("UNLOCK TABLES;")
    commit_changes(db, cursor, sql)
    print "Table max_maxed_out_score"

    # Finally, finds final scores by normalizing scores in table unnormalized_maxed_out_scores. Normalization is done by simply dividing all scores by maximum score stored in max_maxed_out_score table, so as to ensure scores fall in a scale from 0 to 1.
    sql = []
    sql.append("DROP TABLE IF EXISTS maxed_out_scores;")
    sql.append(""" CREATE TABLE maxed_out_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                contributor_type CHAR(15),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                recipient_type CHAR(18),
                maxed_out_score FLOAT(20));""")
    sql.append("LOCK TABLES maxed_out_scores WRITE, unnormalized_maxed_out_scores AS T1 READ, max_maxed_out_score AS T2 READ;")
    sql.append("INSERT INTO maxed_out_scores (fec_committee_id, contributor_name, contributor_type, other_id, recipient_name, recipient_type, maxed_out_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.contributor_type, T1.other_id, T1.recipient_name, T1.recipient_type, T1.maxed_out_score/T2.max_maxed_out_score AS maxed_out_score FROM unnormalized_maxed_out_scores T1, max_maxed_out_score T2;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE maxed_out_scores ADD INDEX (fec_committee_id, other_id);")
    commit_changes(db, cursor, sql)
    print "Table maxed_out_scores"


def compute_length_scores(db, cursor):
    # Computes unnormalized length score for a given contributor/recipient pairs as number of days since first and last donation associated with pair.
    sql = []
    sql.append("DROP TABLE IF EXISTS unnormalized_length_scores;")
    sql.append(""" CREATE TABLE unnormalized_length_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                max_date DATE,
                min_date DATE,
                length_score FLOAT(20));""")
    sql.append("LOCK TABLES unnormalized_length_scores WRITE, fec_contributions AS T READ;")
    sql.append("INSERT INTO unnormalized_length_scores (fec_committee_id, contributor_name, other_id, recipient_name, max_date, min_date, length_score) SELECT T.fec_committee_id, T.contributor_name, T.other_id, T.recipient_name, MAX(T.date) AS max_date, MIN(T.date) AS min_date, DATEDIFF(MAX(T.date), MIN(T.date)) AS length_score FROM fec_contributions T GROUP BY T.fec_committee_id, T.other_id ORDER BY NULL;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE unnormalized_length_scores ADD INDEX (fec_committee_id, contributor_name, other_id, recipient_name, max_date, min_date, length_score);")
    commit_changes(db, cursor, sql)
    print "Table unnormalized_length_scores"

    # Finds maximum score in unnormalized_length_scores table.
    sql = []
    sql.append("DROP TABLE IF EXISTS max_length_score;")
    sql.append(""" CREATE TABLE max_length_score (
                max_length_score FLOAT(20));""")
    sql.append("LOCK TABLES max_length_score WRITE, unnormalized_length_scores AS T READ;")
    sql.append("INSERT INTO max_length_score (max_length_score) SELECT MAX(length_score) AS max_length_score FROM unnormalized_length_scores T;")
    sql.append("UNLOCK TABLES;")
    commit_changes(db, cursor, sql)
    print "Table max_length_score"

    # Finally, finds final scores by normalizing scores in table unnormalized_maxed_out_scores. Normalization is done by simply dividing all scores by maximum score stored in max_maxed_out_score table, so as to ensure scores fall in a scale from 0 to 1.
    sql = []
    sql.append("DROP TABLE IF EXISTS length_scores;")
    sql.append(""" CREATE TABLE length_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                max_date DATE,
                min_date DATE,
                length_score FLOAT(20));""")
    sql.append("LOCK TABLES length_scores WRITE, unnormalized_length_scores AS T1 READ, max_length_score AS T2 READ;")
    sql.append("INSERT INTO length_scores (fec_committee_id, contributor_name, other_id, recipient_name, max_date, min_date, length_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.max_date, T1.min_date, T1.length_score/T2.max_length_score AS length_score FROM unnormalized_length_scores T1, max_length_score T2;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE length_scores ADD INDEX (fec_committee_id, other_id);")
    commit_changes(db, cursor, sql)
    print "Table length_scores"


def compute_race_focus_scores(db, cursor):
    # Lists all races associated with a given contributor/recipient pair, where race is defined by attributes district, office state, branch and cycle.
    sql = []
    sql.append("DROP TABLE IF EXISTS races_list;")
    sql.append(""" CREATE TABLE races_list (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                fec_candidate_id CHAR(9) NOT NULL,
                candidate_name CHAR(200),
                district CHAR(3),
                office_state CHAR(3),
                branch CHAR(2),
                cycle CHAR(5));""")
    sql.append("LOCK TABLES races_list WRITE, fec_contributions AS T1 READ, fec_committees AS T2 READ, fec_candidates AS T3 READ;")
    sql.append("INSERT INTO races_list (fec_committee_id, contributor_name, other_id, recipient_name, fec_candidate_id, candidate_name, district, office_state, branch, cycle) SELECT DISTINCT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T2.fec_candidate_id, T3.name as candidate_name, T3.district, T3.office_state, T3.branch, T3.cycle FROM fec_contributions T1, fec_committees T2, fec_candidates T3 WHERE T2.fec_candidate_id = T3.fecid AND T1.cycle = T2.cycle AND T2.cycle = T3.cycle AND T1.other_id = T2.fecid AND T2.fec_candidate_id REGEXP '^[HPS]';")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE races_list ADD INDEX (fec_committee_id, cycle, district, office_state, branch, contributor_name);")
    commit_changes(db, cursor, sql)
    print "Table races_list"

    # Computes race focus score as inverse of number of races a contributor donates to. Note that this score is not associated with a contributor/recipient pair, but simply with a contributor.
    # No need for normalization due to methodology adopted. Since score is given the inverse of the count of number of races a PAC donates to, scores already fall on a 0-1 scale.
    sql = []
    sql.append("DROP TABLE IF EXISTS race_focus_scores;")
    sql.append(""" CREATE TABLE race_focus_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                race_focus_score FLOAT(20));""")
    sql.append("LOCK TABLES race_focus_scores WRITE, races_list AS T READ;")
    sql.append("INSERT INTO race_focus_scores (fec_committee_id, contributor_name, race_focus_score) SELECT T1.fec_committee_id, T1.contributor_name, 1/COUNT(*) AS race_focus_score FROM (SELECT T.fec_committee_id, T.contributor_name, T.district, T.office_state, T.branch FROM races_list T GROUP BY T.fec_committee_id, T.district, T.office_state, T.branch ORDER BY NULL) T1 GROUP BY T1.fec_committee_id ORDER BY NULL;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE race_focus_scores ADD INDEX (fec_committee_id, race_focus_score);")
    commit_changes(db, cursor, sql)
    print "Table race_focus_scores"


def compute_final_scores(db, cursor):
    # First, reads into database .csv file containing score weights to be used for computing final score.
    filename = "score_weights.csv"
    with open(filename, 'rU') as f:
        rows = list(csv.reader(f))
    try:
        cursor.execute("DROP TABLE IF EXISTS score_weights;")
        cursor.execute( """ CREATE TABLE score_weights (
                score_type CHAR(30) NOT NULL,
                weight FLOAT(5));""")
        cursor.execute("LOCK TABLES score_weights WRITE;")
        db.commit()
    except MySQLdb.Error, e:
        handle_error(db, e)
    for r in rows:
        try:
            cursor.execute("INSERT INTO score_weights (score_type, weight) VALUES ('%s','%s')" % (r[0], r[1]))
            db.commit()
        except MySQLdb.Error, e:
            handle_error(db, e)
    try:
        cursor.execute("UNLOCK TABLES;")
        cursor.execute("ALTER TABLE score_weights ADD INDEX (score_type);")
        db.commit()
    except MySQLdb.Error, e:
        handle_error(db, e)
    print "Table score_weights"

    # Then, finds final scores by computing the weighted average of the five scores computed above: exclusivity_scores, report_type_scores, periodicity_scores, maxed_out_scores, race_focus_scores.
    # We start by joining the first four score tables: exclusivity_scores, report_type_scores, periodicity_scores, maxed_out_scores. We handle race_focus_score separately because this table assigns scores to contributors, rather than contributor/recipient pairs as the others.
    # Weights used are as defined in the score_weights table.
    sql = []
    sql.append("DROP TABLE IF EXISTS five_scores;")
    sql.append(""" CREATE TABLE five_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                exclusivity_score FLOAT(20),
                report_type_score FLOAT(20),
                periodicity_score FLOAT(20),
                maxed_out_score FLOAT(20),
                length_score FLOAT(20),
                five_score FLOAT(20));""")
    sql.append("LOCK TABLES five_scores WRITE, exclusivity_scores AS T1 READ, report_type_scores AS T2 READ, periodicity_scores AS T3 READ, maxed_out_scores AS T4 READ, length_scores AS T5 READ, score_weights AS T6 READ, score_weights AS T7 READ, score_weights AS T8 READ, score_weights AS T9 READ, score_weights as T10 READ;")
    sql.append("INSERT INTO five_scores (fec_committee_id, contributor_name, other_id, recipient_name, exclusivity_score, report_type_score, periodicity_score, maxed_out_score, length_score, five_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, IFNULL(T1.exclusivity_score, 0) AS exclusivity_score, IFNULL(T2.report_type_score, 0) AS report_type_score, IFNULL(T3.periodicity_score, 0) AS periodicity_score, IFNULL(T4.maxed_out_score, 0) AS maxed_out_score, IFNULL(T5.length_score, 0) AS length_score, IFNULL(T1.exclusivity_score, 0) * (SELECT T6.weight FROM score_weights T6 WHERE T6.score_type = 'exclusivity_score') + IFNULL(T2.report_type_score, 0) * (SELECT T7.weight FROM score_weights T7 WHERE T7.score_type = 'report_type_score') + IFNULL(T3.periodicity_score, 0) * (SELECT T8.weight FROM score_weights T8 WHERE T8.score_type = 'periodicity_score') + IFNULL(T4.maxed_out_score, 0) * (SELECT T9.weight FROM score_weights T9 WHERE T9.score_type = 'maxed_out_score') + IFNULL(T5.length_score, 0) * (SELECT T10.weight FROM score_weights T10 WHERE T10.score_type = 'length_score') AS five_score FROM exclusivity_scores T1 LEFT OUTER JOIN report_type_scores T2 ON T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id LEFT OUTER JOIN periodicity_scores T3 ON T1.fec_committee_id = T3.fec_committee_id AND T1.other_id = T3.other_id LEFT OUTER JOIN maxed_out_scores T4 ON T1.fec_committee_id = T4.fec_committee_id AND T1.other_id = T4.other_id LEFT OUTER JOIN length_scores T5 ON T1.fec_committee_id = T5.fec_committee_id AND T1.other_id = T5.other_id;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE five_scores ADD INDEX (fec_committee_id, contributor_name, other_id, recipient_name, five_score);")
    commit_changes(db, cursor, sql)
    print "Table five_scores"

    # Finally, we add weighted race_focus_scores to the five_scores table and get the full final score.
    sql = []
    sql.append("DROP TABLE IF EXISTS final_scores;")
    sql.append(""" CREATE TABLE final_scores (
                fec_committee_id CHAR(9) NOT NULL,
                contributor_name CHAR(200),
                other_id CHAR(9) NOT NULL,
                recipient_name CHAR(200),
                count INT(10),
                exclusivity_score FLOAT(20),
                report_type_score FLOAT(20),
                periodicity_score FLOAT(20),
                maxed_out_score FLOAT(20),
                length_score FLOAT(20),
                race_focus_score FLOAT(20),
                final_score FLOAT(20));""")
    sql.append("LOCK TABLES final_scores WRITE, five_scores AS T1 READ, race_focus_scores AS T2 READ, score_weights AS T3 READ, pairs_count AS T4 READ;")
    sql.append("INSERT INTO final_scores (fec_committee_id, contributor_name, other_id, recipient_name, count, exclusivity_score, report_type_score, periodicity_score, maxed_out_score, length_score, race_focus_score, final_score) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T4.count, T1.exclusivity_score, T1.report_type_score, T1.periodicity_score, T1.maxed_out_score, T1.length_score, IFNULL(T2.race_focus_score, 0) AS race_focus_score, T1.five_score + IFNULL(T2.race_focus_score, 0) * (SELECT T3.weight FROM score_weights T3 WHERE T3.score_type = 'race_focus_score') AS final_score FROM five_scores T1 LEFT OUTER JOIN race_focus_scores T2 ON T1.fec_committee_id = T2.fec_committee_id LEFT OUTER JOIN pairs_count T4 ON T1.fec_committee_id = T4.fec_committee_id AND T1.other_id = T4.other_id;")
    sql.append("UNLOCK TABLES;")
    sql.append("ALTER TABLE final_scores ADD INDEX (fec_committee_id, other_id);")
    commit_changes(db, cursor, sql)
    print "Table final_scores"


def similarity_analysis(db, cursor):
    RANK_THRESHOLD = 10

    # First, compute weighted adjacency matrix
    cursor.execute("SELECT fec_committee_id, other_id, final_score FROM final_scores;")
    try:
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        handle_error(db, e)
    ratings_map = {}
    for r in rows:
        if (r[0] not in ratings_map):
            ratings_map[r[0]] = {}
        ratings_map[r[0]][r[1]] = r[2]
    adj_matrix = DataFrame(ratings_map).T.fillna(0)
    adj_matrix_T = adj_matrix.T
    print "Adjacency matrix computed."

    cursor.execute("SELECT fec_committee_id, other_id, exclusivity_score, report_type_score, periodicity_score, maxed_out_score, length_score, race_focus_score FROM final_scores;")
    try:
        rows = cursor.fetchall()
    except MySQLdb.Error, e:
        handle_error(db, e)

    pair_score_map = {}
    for r in rows:
        pair_score_map[(r[0],r[1])] = ([r[2],r[3],r[4],r[5],r[6],r[7]])
    print "Pair-score dictionary computed."

    # Now use adjacency matrix to perform similarity analysis.
    while (True):
        # Ask user to input kind of similarity analysis to be performed.
        # Options: 1. Find contributors similar to a given contributor, 2. Find recipients similar to a given recipient, 3. Find pairs similar to a given pair.
        analysis = raw_input("What kind of similarity analysis are you interested in? Type the number representing any of the following: \n 1. Find contributors similar to a given contributor. \n 2. Find recipients similar to a given recipient. \n 3. Find pairs similar to a given pair. \n Bedfellows is currently set to display a list of top " + str(RANK_THRESHOLD) + " results. If you would like to change this setting, type 4. \n Type anything else to exit. \n")

        # Measure cosine similarity between different contributors. Each contributor is represented by a vector of final scores of pairs it belongs to.
        if analysis == "1":
            fec_committee_id = raw_input("Enter contributor committee's id: \n")
            try:
                check_contributor_id(fec_committee_id)
            except:
                print "Invalid committee id, try again."
                fec_committee_id = raw_input("Enter contributor committee's id: \n")
            try: 
                cosine_sim = {}
                for j in range(1,adj_matrix.shape[0]):
                    cosine_sim[adj_matrix.ix[j].name] = np.dot(adj_matrix.ix[fec_committee_id],adj_matrix.ix[j])/(np.linalg.norm(adj_matrix.ix[fec_committee_id])*np.linalg.norm(adj_matrix.ix[j])) #cosine similarity as distance metric

                cursor.execute("SELECT contributor_name FROM fec_contributions WHERE fec_committee_id = '" + fec_committee_id + "';")
                contributor_name = cursor.fetchone()[0]
            except MySQLdb.Error, e:
                handle_error(db, e)
            print "Top " + str(RANK_THRESHOLD) + " contributors most similar to " + fec_committee_id + " " + contributor_name + " along with cosine similarity scores are:"

            for index, w in enumerate(sorted(cosine_sim, key=cosine_sim.get, reverse=True)):
                if w != fec_committee_id:
                    cursor.execute("SELECT contributor_name FROM fec_contributions WHERE fec_committee_id = '" + w + "';")
                    try:
                        contributor_name = cursor.fetchone()[0]
                    except MySQLdb.Error, e:
                        handle_error(db, e)
                    print w, contributor_name, cosine_sim[w]
                if index >= RANK_THRESHOLD:
                    break

        # Measure cosine similarity between different recipients. Each recipient is represented by a vector of final scores of pairs it belongs to.
        elif analysis == "2":
            other_id = raw_input("Enter recipient committee's id: \n")
            try:
                check_recipient_id(other_id)
            except:
                print "Invalid committee id, try again."
                other_id = raw_input("Enter recipient committee's id: \n")
            try:
                cosine_sim = {}
                for j in range(1,adj_matrix_T.shape[0]):
                    cosine_sim[adj_matrix_T.ix[j].name] = np.dot(adj_matrix_T.ix[other_id],adj_matrix_T.ix[j])/(np.linalg.norm(adj_matrix_T.ix[other_id])*np.linalg.norm(adj_matrix_T.ix[j])) #cosine similarity as distance metric

                cursor.execute("SELECT recipient_name FROM fec_contributions WHERE other_id = '" + other_id + "';")
                recipient_name = cursor.fetchone()[0]
            except MySQLdb.Error, e:
                handle_error(db, e)
            print "Top " + str(RANK_THRESHOLD) + " recipients most similar to " + other_id + " " + recipient_name + " along with cosine similarity scores are:"

            for index, w in enumerate(sorted(cosine_sim, key=cosine_sim.get, reverse=True)):
                if w != other_id:
                    cursor.execute("SELECT recipient_name FROM fec_contributions WHERE other_id = '" + w + "';")
                    try:
                       recipient_name = cursor.fetchone()[0]
                    except MySQLdb.Error, e:
                       handle_error(db, e)
                    print w, recipient_name, cosine_sim[w]
                if index >= RANK_THRESHOLD:
                    break

        elif analysis == "3":
            # In this case, pairs will be represented by a vector made up of the six scores used in the computational of final score.
            fec_committee_id = raw_input("Enter contributor's fec_committee_id: \n")
            try:
                check_contributor_id(fec_committee_id)
            except:
                print "Invalid committee id, try again."
                fec_committee_id = raw_input("Enter contributor's committee's id: \n")

            cursor.execute("SELECT contributor_name FROM fec_contributions WHERE fec_committee_id = '" + fec_committee_id + "';")
            try:
                contributor_name = cursor.fetchone()[0]
            except MySQLdb.Error, e:
                handle_error(db, e)

            other_id = raw_input("Enter recipient's id: \n")
            try:
                check_recipient_id(other_id)
            except:
                print "Invalid committee id, try again."
                other_id = raw_input("Enter recipient's committee's id: \n")
            cursor.execute("SELECT recipient_name FROM fec_contributions WHERE other_id = '" + other_id + "';")
            try:
                recipient_name = cursor.fetchone()[0]
            except MySQLdb.Error, e:
                handle_error(db, e)

            try:
                key = (fec_committee_id,other_id)
                cosine_sim = {}
                for p in pair_score_map:
                    cosine_sim[p] = np.dot(pair_score_map[key],pair_score_map[p])/(np.linalg.norm(pair_score_map[key])*np.linalg.norm(pair_score_map[p])) #cosine similarity as distance metric

                print "Top " + str(RANK_THRESHOLD) + " contributor-recipient pairs most similar to pair " + fec_committee_id + " " + contributor_name + " and " + other_id + " " + recipient_name + " along with cosine similarity scores are:"

                for index, w in enumerate(sorted(cosine_sim, key=cosine_sim.get, reverse=True)):
                    if w != key:
                        cursor.execute("SELECT contributor_name FROM fec_contributions WHERE fec_committee_id = '" + w[0] + "';")
                        try:
                            contributor_name = cursor.fetchone()[0]
                        except MySQLdb.Error, e:
                            handle_error(db, e)
                        cursor.execute("SELECT recipient_name FROM fec_contributions WHERE other_id = '" + w[1] + "';")
                        try:
                            recipient_name = cursor.fetchone()[0]
                        except MySQLdb.Error, e:
                            handle_error(db, e)
                        print w[0], contributor_name, w[1], recipient_name, cosine_sim[w]
                    if index >= RANK_THRESHOLD:
                        break
            except:
                print "Bedfellows couldn't find contributor-recipient pair entered as input in the database.\n"

        elif analysis == "4":
            while True:
                try:
                    RANK_THRESHOLD = int(raw_input("How many results would you like to display? Enter an integer number. \n"))
                    break
                except:
                    print "Invalid input. Please try again."

        # This hidden option is used to measure internal consistency of scores through Cronbach's Alpha. Still in development.
        elif analysis == "5":
            # Then, finds final scores by computing the weighted average of the five scores computed above: exclusivity_scores, report_type_scores, periodicity_scores, maxed_out_scores, length_scores.
            # We start by joining the first four score tables: exclusivity_scores, report_type_scores, periodicity_scores, maxed_out_scores. We handle race_focus_score separately because this table assigns scores to contributors, rather than contributor/recipient pairs as the others.
            # Weights used are as defined in the score_weights table.
            sql = []
            sql.append("DROP TABLE IF EXISTS five_sum;")
            sql.append(""" CREATE TABLE five_sum (
                        fec_committee_id CHAR(9) NOT NULL,
                        contributor_name CHAR(200),
                        other_id CHAR(9) NOT NULL,
                        recipient_name CHAR(200),
                        exclusivity_score FLOAT(20),
                        report_type_score FLOAT(20),
                        periodicity_score FLOAT(20),
                        maxed_out_score FLOAT(20),
                        length_score FLOAT(20),
                        five_sum FLOAT(20);""")
            sql.append("LOCK TABLES five_sum WRITE, exclusivity_scores AS T1 READ, report_type_scores AS T2 READ, periodicity_scores AS T3 READ, maxed_out_scores AS T4 READ, length_scores AS T5 READ, score_weights AS T6 READ, score_weights AS T7 READ, score_weights AS T8 READ, score_weights AS T9 READ, score_weights as T10 READ;")
            sql.append("INSERT INTO five_sum (fec_committee_id, contributor_name, other_id, recipient_name, exclusivity_score, report_type_score, periodicity_score, maxed_out_score, length_score, five_sum) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, IFNULL(T1.exclusivity_score, 0) AS exclusivity_score, IFNULL(T2.report_type_score, 0) AS report_type_score, IFNULL(T3.periodicity_score, 0) AS periodicity_score, IFNULL(T4.maxed_out_score, 0) AS maxed_out_score, IFNULL(T5.length_score, 0) AS length_score, IFNULL(T1.exclusivity_score, 0) + IFNULL(T2.report_type_score, 0) + IFNULL(T3.periodicity_score, 0) + IFNULL(T4.maxed_out_score, 0) + IFNULL(T5.length_score, 0) AS five_sum FROM exclusivity_scores T1 LEFT OUTER JOIN report_type_scores T2 ON T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id LEFT OUTER JOIN periodicity_scores T3 ON T1.fec_committee_id = T3.fec_committee_id AND T1.other_id = T3.other_id LEFT OUTER JOIN maxed_out_scores T4 ON T1.fec_committee_id = T4.fec_committee_id AND T1.other_id = T4.other_id LEFT OUTER JOIN length_scores T5 ON T1.fec_committee_id = T5.fec_committee_id AND T1.other_id = T5.other_id;")
            sql.append("UNLOCK TABLES;")
            sql.append("ALTER TABLE five_sum ADD INDEX (fec_committee_id, contributor_name, other_id, recipient_name, five_sum);")
            commit_changes(db, cursor, sql)
            print "Table five_sum"

            # Finally, we add weighted race_focus_scores to the five_sum table and get the full final score.
            sql = []
            sql.append("DROP TABLE IF EXISTS final_sum;")
            sql.append(""" CREATE TABLE final_sum (
                        fec_committee_id CHAR(9) NOT NULL,
                        contributor_name CHAR(200),
                        other_id CHAR(9) NOT NULL,
                        recipient_name CHAR(200),
                        count INT(10),
                        exclusivity_score FLOAT(20),
                        report_type_score FLOAT(20),
                        periodicity_score FLOAT(20),
                        maxed_out_score FLOAT(20),
                        length_score FLOAT(20),
                        race_focus_score FLOAT(20),
                        final_sum FLOAT(20);""")
            sql.append("LOCK TABLES final_sum WRITE, five_sum AS T1 READ, race_focus_scores AS T2 READ, score_weights AS T3 READ, pairs_count AS T4 READ;")
            sql.append("INSERT INTO final_sum (fec_committee_id, contributor_name, other_id, recipient_name, count, exclusivity_score, report_type_score, periodicity_score, maxed_out_score, length_score, race_focus_score, final_sum) SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T4.count, T1.exclusivity_score, T1.report_type_score, T1.periodicity_score, T1.maxed_out_score, T1.length_score, IFNULL(T2.race_focus_score, 0) AS race_focus_score, T1.five_sum + IFNULL(T2.race_focus_score, 0) AS final_sum FROM five_sum T1 LEFT OUTER JOIN race_focus_scores T2 ON T1.fec_committee_id = T2.fec_committee_id LEFT OUTER JOIN pairs_count T4 ON T1.fec_committee_id = T4.fec_committee_id AND T1.other_id = T4.other_id;")
            sql.append("UNLOCK TABLES;")
            sql.append("ALTER TABLE final_sum ADD INDEX (fec_committee_id, other_id);")
            commit_changes(db, cursor, sql)
            print "Table final_sum"

            cursor.execute("SELECT VAR_POP(exclusivity_score), VAR_POP(report_type_score), VAR_POP(periodicity_score), VAR_POP(maxed_out_score), VAR_POP(length_score), VAR_POP(race_focus_score), VAR_POP(final_sum), VAR_POP(four_sum), VAR_POP(two_sum) FROM final_sum;")
            try:
               variances = cursor.fetchone()
               print variances
            except MySQLdb.Error, e:
               handle_error(db, e)
            var_exclusivity = variances[0]
            print "var_exclusivity = " + str(var_exclusivity)
            var_report_type = variances[1]
            print "var_report_type = " + str(var_report_type)
            var_periodicity = variances[2]
            print "var_periodicity = " + str(var_periodicity)
            var_maxed_out = variances[3]
            print "var_maxed_out = " + str(var_maxed_out)
            var_length = variances[4]
            print "var_length = " + str(var_length)
            var_race_focus = variances[5]
            print "var_race_focus = " + str(var_race_focus)
            var_final = variances[6]
            print "var_final = " + str(var_final)

            K = 6.0   #number of scores computed
            sum_of_6_score_vars = var_exclusivity + var_report_type + var_periodicity + var_maxed_out + var_length + var_race_focus
            print "sum of 6 variances: var_exclusivity + var_report_type + var_periodicity + var_maxed_out + var_length + var_race_focus"
            print sum_of_6_score_vars

            cronbach_alpha = (K/(K-1.0))*(1.0-((sum_of_6_score_vars)/var_final))
            print "cronbach's alpha: "
            print cronbach_alpha

        else:
            break


if __name__ == "__main__":
    if len(sys.argv) == 2:
        main(sys.argv[1])
    else:
        usage()
        sys.exit(1)
