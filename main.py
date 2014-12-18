import re
import sys, csv
import MySQLdb


def commit_changes(db, cursor, sql):
    try:
        for q in sql:
            cursor.execute(q)
        db.commit()
    except MySQLdb.Error, e:
        handle_error(db, e)

def handle_error(db, e):
    if e[0] == 1061: # index exists, we can move on.
        pass
    else:
        db.rollback()
        sys.stderr.write(str(e))
        sys.exit(1)

def check_contributor_id(id):
    regexp = re.compile("C\d+")
    match = regexp.match(id)
    try:
        match.group()
    except:
        raise ValueError("Invalid committee id")

def check_recipient_id(id):
    regexp = re.compile("[C|H|P|S|V]\d+")
    match = regexp.match(id)
    try:
        match.group()
    except:
        raise ValueError("Invalid committee id")

def usage():
    sys.stderr.write("""
        Usage:
        python main.py <Flag: 'overall' or 'cycle'> <Name of Database> \n
        Examples:
        python main.py overall fec
        python main.py cycle new
		\n""")

if __name__ == "__main__":
    if len(sys.argv) == 3:
        db = MySQLdb.connect(host="localhost", port=3306, user="root",passwd="",db=sys.argv[2]) # make sure db argument matches name of database where fec_committee_contributions.sql is stored
        if sys.argv[1] == 'overall':
           import overall
           overall.main(db)
        elif sys.argv[1] == 'cycle':
           import groupedbycycle
           groupedbycycle.main(db)
        else:
           usage()
           sys.exit(1)
    else:
       usage()
       sys.exit(1)
