Affinity
===================

Affinity is a Python library uses Federal Election Commission data of political action committee contributions to other committees to calculate seven scores measuring the length and breadth of the relationship between donors and recipients. It also provides a way to see similar donors, recipients or pairs. It is intended as reporting tool for journalists and researchers interested in campaign finance data.

Affinity is a command-line tool, and requires a local or remote MySQL database (it defaults to local, but users can change the connection string in `main.py`). It also requires three tables from the FEC: candidates, committees and the committee-to-committee transaction file. These files should match the layouts from the FEC with the exception of an additional `cycle` field. Affinity expects these tables to be named fec_candidates, fec_committees and fec_committee_contributions. When computing the scores, Affinity will create a number of tables in the database; most of them are not large.

In addition to the expected database tables, Affinity comes with several files listing campaign contribution limits, score weights and super PACs (which are excluded from the analysis since they do not make contributions to candidates). Of these, the list of super PACs may need to be updated depending on when the library is used.

### Requirements

In addition to MySQL, Affinity makes use of Python packages numpy, scipy and pandas. Full list in [requirements.txt](requirements.txt). You should use a tool like virtualenv to maintain control over your Python environment.

### Installation

Clone this repository and create a virtualenv in that directory. Then, `pip install -r requirements.txt`.

### Usage

Affinity is a command-line application:

```
    python main.py <Flag: 'overall' or 'cycle'> <Name of Database>

    Examples:
    python main.py overall fec
    python main.py cycle new
```

Note that it is required that a database containing tables fec_committee_contributions, fec_candidates and fec_committees be specified as the  third parameter.

Script main.py simply calls overall.py or groupedbycycle.py as specified by 2nd paramater (which must be either `overall` or `cycle`).

Choosing `overall` will give you the option of computing scores, analyzing existing scores, or both:

```
    Do you want to compute scores or perform a similarity analysis of scores already computed? Enter 'compute', 'analyze' or 'both' accordingly.

```

Entering `compute` will result in the script creating and populating any needed tables, and may take several minutes.


### CONTRIBUTORS

Affinity was developed by Nikolas Iubel during an internship with the Interactive News team at The New York Times, and is edited by Derek Willis of The Times.

### LICENSE

Affinity is licensed under the MIT License. See [LICENSE](LICENSE) for details.
