PAC_affinity_scores
===================

        Usage:
        python main.py <Flag: 'overall' or 'cycle'> <Name of Database> 

        Examples:
        python main.py overall fec
        python main.py cycle new
	
Note that it is required that a database containing tables fec_committee_contributions, fec_candidates and fec_committees be specified as 3rd parameter.

Script main.py simply calls main_overall.py or main_groupedbycycle.py as specified by 2nd paramater (which must be either 'overall' or 'cycle').
