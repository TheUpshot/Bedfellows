import MySQLdb, sys, csv
from sys import stdout


def main():
    cursor = db.cursor()
    explain(cursor)
    db.close()

def explain(cursor):
    sql = []
    print "super_PACs_list:"
    cursor.execute("EXPLAIN SELECT T.fecid FROM fec_committees T WHERE T.is_super_PAC = '1';")
    db.commit()
    print cursor.fetchall()
    print "fec_contributions:"
    db.commit()
    print cursor.fetchall()
    cursor.execute("EXPLAIN SELECT T.fec_committee_id, T.report_type, T.contributor_name, T.date, T.amount, T.other_id, T.recipient_name, T.cycle FROM fec_committee_contributions T WHERE T.transaction_type = '24K' AND T.entity_type = 'PAC' AND EXTRACT(YEAR FROM T.date) >= '2003' AND T.fec_committee_id NOT IN (SELECT fecid FROM super_PACs_list T1) AND T.other_id NOT IN (SELECT fecid FROM super_PACs_list T2);")
    db.commit()
    print cursor.fetchall()
    print "total_donated_by_PAC:"
    cursor.execute("EXPLAIN SELECT T.fec_committee_id, T.cycle, T.contributor_name, SUM(T.amount) AS total_by_PAC FROM fec_contributions T GROUP BY T.fec_committee_id, T.cycle ORDER BY NULL;")
    db.commit()
    print cursor.fetchall()
    print "exclusivity_scores:"
    cursor.execute("EXPLAIN SELECT T.fec_committee_id, T.contributor_name, T.total_by_PAC, T.other_id, T.recipient_name, T.cycle, SUM(T.amount) AS total_amount, IF(SUM(exclusivity_subscore) > 1, 1, SUM(exclusivity_subscore)) AS exclusivity_score FROM (SELECT T1.fec_committee_id, T1.contributor_name, T1.total_by_PAC, T2.other_id, T2.recipient_name, T2.cycle, T2.amount, T2.date, T2.amount/T1.total_by_PAC AS exclusivity_subscore FROM fec_contributions T2, total_donated_by_PAC T1 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.cycle = T2.cycle) T GROUP BY T.fec_committee_id, T.other_id, T.cycle ORDER BY NULL;")
    db.commit()
    print cursor.fetchall()
    print "report_type_count_by_pair:"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T2.other_id, T2.recipient_name, T2.cycle, T2.report_type, IF(MOD(EXTRACT(YEAR FROM T2.date), 2) = 0, 'even', 'odd') AS year_parity, T2.date, count(*) FROM fec_contributions AS T2, exclusivity_scores AS T1 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id GROUP BY T2.fec_committee_id, T2.other_id, T2.cycle, T2.report_type ORDER BY NULL;")
    db.commit()
    print cursor.fetchall()
    print "pairs_count:"
    cursor.execute("EXPLAIN SELECT T.fec_committee_id, T.other_id, T.cycle, count(*) FROM fec_contributions T GROUP BY T.fec_committee_id, T.other_id, T.cycle ORDER BY NULL;")
    db.commit()
    print cursor.fetchall()
    print "report_type_frequency:"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.cycle, T1.report_type, T1.year_parity, T1.d_date, T1.count AS report_type_count_by_pair, T2.count AS pairs_count, T1.count/T2.count AS report_type_frequency FROM report_type_count_by_pair T1, pairs_count T2 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id AND T1.cycle = T2.cycle;")
    db.commit()
    print cursor.fetchall()
    print "unnormalized_report_type_scores:"
    cursor.execute("EXPLAIN SELECT T3.fec_committee_id, T3.contributor_name, T3.other_id, T3.recipient_name, T3.cycle, SUM(T3.report_type_subscore) AS report_type_score FROM (SELECT T2.fec_committee_id, T2.contributor_name, T2.other_id, T2.recipient_name, T2.cycle, T2.report_type_frequency * T1.weight AS report_type_subscore FROM report_type_weights T1, report_type_frequency T2 WHERE T1.report_type = T2.report_type AND T1.year_parity = T2.year_parity) T3 GROUP BY T3.fec_committee_id, T3.other_id, T3.cycle ORDER BY NULL;")
    db.commit()
    print cursor.fetchall()
    print "max_report_type_score:"
    cursor.execute("EXPLAIN SELECT MAX(report_type_score) AS max_report_type_score FROM unnormalized_report_type_scores T;")
    db.commit()
    print cursor.fetchall()
    print "report_type_score:"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.cycle, T1.report_type_score/T2.max_report_type_score AS report_type_score FROM unnormalized_report_type_scores T1, max_report_type_score T2;")
    db.commit()
    print cursor.fetchall()
    print "unnormalized_periodicity_scores"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.cycle, IF(VAR_POP(DAYOFYEAR(T1.date)) = 0, IF(COUNT(DISTINCT(T1.date)) > 1, 1, 0), IFNULL(1/VAR_POP(DAYOFYEAR(T1.date)), 0)) AS periodicity_score FROM fec_contributions T1 GROUP BY T1.fec_committee_id, T1.other_id, T1.cycle ORDER BY NULL;")
    db.commit()
    print cursor.fetchall()
    print "max_periodicity_score:"
    cursor.execute("EXPLAIN SELECT MAX(periodicity_score) AS max_periodicity_score FROM unnormalized_periodicity_scores T;")
    db.commit()
    print cursor.fetchall()
    print "periodicity_scores:"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.cycle, T1.periodicity_score/T2.max_periodicity_score AS periodicity_score FROM unnormalized_periodicity_scores T1, max_periodicity_score T2;")
    db.commit()
    print cursor.fetchall()
    print "contributor_types:"
    cursor.execute("EXPLAIN SELECT T.fecid, T.name, T.cycle, CASE WHEN (T.committee_type = 'X' OR T.committee_type = 'Y') THEN CASE WHEN (T.fecid = 'C00003418' OR T.fecid = 'C00163022' OR T.fecid = 'C00027466' OR T.fecid = 'C00075820' OR T.fecid = 'C00000935' OR T.fecid = 'C00042366' OR T.fecid = 'C00010603') THEN 'national_party' ELSE 'other_party' END ELSE CASE WHEN (T.cycle >= EXTRACT(YEAR FROM T.multiqualify_date)) THEN 'multi_pac'ELSE 'non_multi_pac' END END AS contributor_type FROM fec_committees T WHERE T.committee_type = 'X' OR T.committee_type = 'Y' OR T.committee_type = 'N' OR T.committee_type = 'Q' OR T.committee_type = 'F' GROUP BY T.fecid, T.cycle ORDER BY NULL;")
    db.commit()
    print cursor.fetchall()
    print "recipient_types:"
    cursor.execute("EXPLAIN SELECT T.fecid, T.name, T.cycle, CASE WHEN (T.committee_type = 'X' OR T.committee_type = 'Y') THEN CASE WHEN (T.fecid = 'C00003418' OR T.fecid = 'C00163022' OR T.fecid = 'C00027466' OR T.fecid = 'C00075820' OR T.fecid = 'C00000935' OR T.fecid = 'C00042366' OR T.fecid = 'C00010603') THEN 'national_party' ELSE 'other_party' END ELSE CASE WHEN (T.committee_type = 'H' OR T.committee_type = 'S' OR T.committee_type = 'P' OR T.committee_type = 'A' OR T.committee_type = 'B') THEN 'candidate' ELSE 'pac' END END AS contributor_type FROM fec_committees T WHERE T.committee_type = 'X' OR T.committee_type = 'Y' OR T.committee_type = 'H' OR T.committee_type = 'S' OR T.committee_type = 'P' OR T.committee_type = 'A' OR T.committee_type = 'B' OR T.committee_type = 'N' OR T.committee_type = 'Q' OR T.committee_type = 'F' OR T.committee_type = 'G' GROUP BY T.fecid, T.cycle ORDER BY NULL;")
    db.commit()
    print cursor.fetchall()
    print "joined_contr_recpt_types:"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T2.contributor_type, T1.other_id, T1.recipient_name, T3.recipient_type, T1.cycle, T1.date, T1.amount FROM fec_contributions T1, contributor_types T2, recipient_types T3 WHERE T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T3.other_id AND T1.cycle = T2.cycle AND T1.cycle = T3.cycle;")
    db.commit()
    print cursor.fetchall()
    print "maxed_out_subscores:"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T1.contributor_type, T1.other_id, T1.recipient_name, T1.recipient_type, T1.cycle, T1.date, T1.amount, T2.contribution_limit, T1.amount/T2.contribution_limit AS maxed_out_subscore FROM joined_contr_recpt_types T1, contribution_limits T2 WHERE T1.contributor_type = T2.contributor_type AND T1.recipient_type = T2.recipient_type AND T1.cycle = T2.cycle;")
    db.commit()
    print cursor.fetchall()
    print "unnormalized_maxed_out_scores:"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T1.contributor_type, T1.other_id, T1.recipient_name, T1.cycle, T1.recipient_type, SUM(T1.maxed_out_subscore) AS maxed_out_score FROM maxed_out_subscores T1 GROUP BY T1.fec_committee_id, T1.other_id, T1.cycle ORDER BY NULL;")
    db.commit()
    print cursor.fetchall()
    print "max_maxed_out_score:"
    cursor.execute("EXPLAIN SELECT MAX(maxed_out_score) AS max_maxed_out_score FROM unnormalized_maxed_out_scores T;")
    db.commit()
    print cursor.fetchall()
    print "maxed_out_scores:"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T1.contributor_type, T1.other_id, T1.recipient_name, T1.cycle, T1.recipient_type, T1.maxed_out_score/T2.max_maxed_out_score AS maxed_out_score FROM unnormalized_maxed_out_scores T1, max_maxed_out_score T2;")
    db.commit()
    print cursor.fetchall()
    print "unnormalized_length_scores:"
    cursor.execute("EXPLAIN SELECT T.fec_committee_id, T.contributor_name, T.other_id, T.recipient_name, T.cycle, MAX(T.date) AS max_date, MIN(T.date) AS min_date, DATEDIFF(MAX(T.date), MIN(T.date)) AS length_score FROM fec_contributions T GROUP BY T.fec_committee_id, T.other_id, T.cycle ORDER BY NULL; ")
    db.commit()
    print cursor.fetchall()
    print "max_length_score:"
    cursor.execute("EXPLAIN SELECT MAX(length_score) AS max_length_score FROM unnormalized_length_scores T;")
    db.commit()
    print cursor.fetchall()
    print "length_scores:"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.cycle, T1.max_date, T1.min_date, T1.length_score/T2.max_length_score AS length_score FROM unnormalized_length_scores T1, max_length_score T2;")
    db.commit()
    print cursor.fetchall()
    print "races_list:"
    cursor.execute("EXPLAIN SELECT DISTINCT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T2.fec_candidate_id, T3.name as candidate_name, T3.district, T3.office_state, T3.branch, T3.cycle FROM fec_contributions T1, fec_committees T2, fec_candidates T3 WHERE T2.fec_candidate_id = T3.fecid AND T1.other_id = T2.fecid AND T2.fec_candidate_id REGEXP '^[HPS]';")
    db.commit()
    print cursor.fetchall()
    print "race_focus_scores:"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T1.cycle, 1/COUNT(*) AS race_focus_score FROM (SELECT T.fec_committee_id, T.contributor_name, T.cycle, T.district, T.office_state, T.branch FROM races_list T GROUP BY T.fec_committee_id, T.cycle, T.district, T.office_state, T.branch ORDER BY NULL) T1 GROUP BY T1.fec_committee_id, T1.cycle ORDER BY NULL;")
    db.commit()
    print cursor.fetchall()
    print "five_scores:"
    cursor.execute("EXPLAIN SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.cycle, IFNULL(T1.exclusivity_score, 0) AS exclusivity_score, IFNULL(T2.report_type_score, 0) AS report_type_score, IFNULL(T3.periodicity_score, 0) AS periodicity_score, IFNULL(T4.maxed_out_score, 0) AS maxed_out_score, IFNULL(T5.length_score, 0) AS length_score, IFNULL(T1.exclusivity_score, 0) * (SELECT T6.weight FROM score_weights T6 WHERE T6.score_type = 'exclusivity_score') + IFNULL(T2.report_type_score, 0) * (SELECT T7.weight FROM score_weights T7 WHERE T7.score_type = 'report_type_score') + IFNULL(T3.periodicity_score, 0) * (SELECT T8.weight FROM score_weights T8 WHERE T8.score_type = 'periodicity_score') + IFNULL(T4.maxed_out_score, 0) * (SELECT T9.weight FROM score_weights T9 WHERE T9.score_type = 'maxed_out_score') + IFNULL(T5.length_score, 0) * (SELECT T10.weight FROM score_weights T10 WHERE T10.score_type = 'length_score') AS five_score FROM exclusivity_scores T1 JOIN report_type_scores T2 ON T1.fec_committee_id = T2.fec_committee_id AND T1.other_id = T2.other_id AND T1.cycle = T2.cycle JOIN periodicity_scores T3 ON T1.fec_committee_id = T3.fec_committee_id AND T1.other_id = T3.other_id AND T1.cycle = T3.cycle JOIN maxed_out_scores T4 ON T1.fec_committee_id = T4.fec_committee_id AND T1.other_id = T4.other_id AND T1.cycle = T4.cycle JOIN length_scores T5 ON T1.fec_committee_id = T5.fec_committee_id AND T1.other_id = T5.other_id AND T1.cycle = T5.cycle;")
    db.commit()
    print cursor.fetchall()
    print "final_scores:"
    cursor.execute("EXPLAIN SELECT * FROM (SELECT T1.fec_committee_id, T1.contributor_name, T1.other_id, T1.recipient_name, T1.cycle, T1.five_score, IFNULL(T2.race_focus_score, 0) AS race_focus_score, T1.five_score + IFNULL(T2.race_focus_score, 0) * (SELECT T3.weight FROM score_weights T3 WHERE T3.score_type = 'race_focus_score') AS final_score FROM five_scores T1 LEFT OUTER JOIN race_focus_scores T2 ON T1.fec_committee_id = T2.fec_committee_id AND T1.cycle = T2.cycle) T;")
    db.commit()
    print cursor.fetchall()
    #commit_changes(cursor, sql)


def commit_changes(cursor, sql):
    try:
        for q in sql:
            cursor.execute(q)
        db.commit()
    except MySQLdb.Error, e:
        handle_error(e)


def handle_error(e):
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
