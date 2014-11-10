import sys, main_overall, main_groupedbycycle

if __name__ == "__main__":
    if len(sys.argv) == 3:
        if sys.argv[1] == 'overall':
           main_overall.main(sys.argv[2])
        elif sys.argv[1] == 'cycle':
           main_groupedbycycle.main(sys.argv[2])
        else:
           usage()
           sys.exit(1)
    else:
       usage()
       sys.exit(1)

def usage():
    sys.stderr.write("""
        Usage:
        python main.py <Method: 'overall' or 'cycle'> <Name of Database> \n
        Examples:
        python main.py overall fec
        python main.py cycle new
		\n""")
