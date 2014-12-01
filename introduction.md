
# Bedfellows


### Introduction

Political Action Committees (PACs) must report every donation made to another federal committee to the Federal Election Commission, yet the nature of the relationship between PAC contributors and recipients can be obscure. It is no easy feat to make the jump from the millions of entries in the FEC data to the story told by the contribution history associated with a contributor-recipient pair. A descriptive snapshot of the pair's contribution history would go a long way towards improving accountability of political committee contributions. That's where Bedfellows comes in.

To provide a measure of the dynamics of PAC contributions at the level of contributor-recipient pairs, The Upshot's Derek Willis and I envisioned a score that models contributions at that relationship level. The model could be defined any number of ways, but we settled on a score between 0 and 1 assigned to every possible contributor-recipient pair, with 0 signifying that contributor has no association whatsoever with recipient, and a 1 signifying that contributor and recipient are more closely related than any other pair.

Bedfellows is a command-line tool that calculates scores for the donor-recipient relationship and provides a similarity score so users can see donors, recipients and pairs that are most like each other. It is meant to be run locally for data exploration; it is not currently optimized for use as a web application.

We cannot map all the information associated with a contributor-recipient pair into a decimal number between 0 and 1 without first defining how exactly to measure the strength of the affinity of contributor-recipient pairs. These definitions are essentially editorial: as journalists, we rely on our knowledge of the beat to decide which metrics to focus on. What follows is an account of the decision-making process that amounted to the computation of PAC affinity scores.

### Data, Tools and Initial Setup

The campaign finance data we use is an enhanced version of three files made available by the FEC, listing committees, candidates and committee-to-committee transactions (the "itoth" file).

Our tools of choice are the open-source relational database MySQL and the Python library mysqldb. For the sake of convenience, we encapsulate all queries used to compute the scores into a Python script that connects with the database through mysqldb. Code and starter files are available on GitHub along with usage instructions.

The Python scripts assume that the database we're using already contains `fec_committee_contributions`, `fec_committees` and `fec_candidates`.

Before we start querying the database, we tailor the data to our needs in the `initial_setup` function. To do so, we first add indexes to the `fec_committee_contributions` table and then subset the table based on a specific kind of donation. We are interested in committee-to-committee donations, where committees can be PACs, candidate committees or party committees.

To narrow down the data to committee-to-committee donations, we adopt the following constraints:

1.     Donations of transaction type '24K', i.e. contributions made to non-affiliate. These transactions refer to contributions made by committees, which are the ones we are interested in focusing on.
2.     Donations where entity type is 'PAC' or 'CCM'. The codes refer to Political Action Committee or Candidate Committee, respectively.
3.     We limit the analysis to donations made from 2003 on, since the contribution limit regulations differ significantly before then.
4.     We also remove super PACs from consideration because we are interested in donations bound by contribution limits, and because super PACs do not make candidate contributions.

This subset is stored in `fec_contributions`, which is the table all subsequent queries are primarily built on top of. We add indexes to tables `fec_contributions`, `fec_candidates` and `fec_committees` as well as every table we create in the process in order to speed up subsequent queries. Most indexes are added on attributes `fec_committee_id` and `other_id`. Attribute `fec_committee_id` uniquely identifies contributors, whereas other_id uniquely identifies recipients.

The bulk of our code is split into two scripts: `overall.py` and `groupedbycycle.py`. The former computes overall relationship scores, i.e. scores across all election cycles since 2003, whereas the latter computes scores for each election cycle separately. The `main.py` script invokes one of the two scripts according to the first parameter it receives (either 'overall' or 'cycle'.) The second parameter required is the name of the database where the `fec_candidate_contributions`, `fec_candidates` and `fec_committees` tables are stored.

### The Model

What matters most in defining how invested contributors are in a campaign: the length of the relationship with donation recipients, or the amount donated? The number or the timing of donations? Absolute or relative number of donations? What exactly do we mean by timing anyway – are we talking about how often or how early they occur? These questions raise a fundamental point: no single metric will single-handedly describe the affinity between contributors and donors.

Our method is to combine several metrics into the relationship score. Not only does this strategy encompass a number of ways in which the strength of the relationship can manifest itself, it also increases the robustness of the score. But a core question remains: what exactly should these metrics be? The following is the list of metrics we have decided to incorporate into the scores. The hope is that most if not all of them are intuitive measures.

1.     Length of the relationship is an obvious first pick: The longer contributor has donated, the stronger a relationship it has with recipient. This metric is captured in the length score.
2.     Timing of donations also matters: The earlier in the election cycle a contributor donates to a campaign, the stronger a commitment to the campaign it displays. Uncertainty about a campaign's prospects is higher early in the election cycle. The scores of early donations are bumped up through the report type score.
3.     Periodicity of donations is next: Periodic donations made around the same time each year are an indicator of strength, since recipients can expect to count on these donations. This kind of periodic pattern in the timing of donations is rewarded in the computation of the periodicity score.
4.     Amount donated should also factor into the score: The more money a contributor gives, the more invested in the recipient it is. It is not enough to look at the absolute figure - much more telling is what percentage share of the contribution limit allowed by the FEC the donation represents. We want to measure how close contributor was to donating as much as it lawfully could. This is the rationale behind the maxed-out score.
5.     Exclusivity of the relationship is arguably relevant, too: The more selective contributors are in choosing recipients, the more invested they are in the respective campaigns. Contributors that donate to campaigns all across the country are less invested in specific recipients than contributors that donate exclusively to a given recipient. We capture this idea with the exclusivity score.
6.     Geography is the last metric: The more contributors donate to recipients associated with specific races, the more invested in the outcome of those specific races they are, which denotes a stronger relationship with the recipients associated with these races. This is the intuition behind race focus scores.

One could argue than an obvious metric is missing from this list: a simple count of the number of donations associated with each contributor-recipient pair. This is done on purpose, as we're more concerned about the patterns surrounding donations than the number of donations per se. The count is embedded in the computation of several of these scores, notably periodicity scores and length scores, which are assigned a value of 0 in the event of one-time donations.

The choice of scores is of course an editorial decision, one of the several judgment calls that factor into the design of an algorithm of this kind. We hope to make this analysis accountable by disclosing the editorial decisions embedded in the algorithm design.

### Exclusivity Scores

The idea behind exclusivity scores is to capture the share of the overall amount donated by a contributor that is assigned to each recipient. In other words, the score measures how "exclusive" donations are. If all the money donated by a contributor goes to a single recipient, contributor and recipient are likely to have a stronger relationship than that of pairs in which contributor splits its donations among several recipients.
To compute exclusivity scores, we first find total amount donated by a given contributor across all recipients. Then, for each donation made by a contributor, we compute an exclusivity subscore as the quotient of the donation's amount by the total amount donated across all recipients. We finally compute exclusivity scores by summing over all exclusivity subscores associated with a given pair.

#### Step By Step

The first step in calculating exclusivity scores is computing table `total_donated_by_contributor`, which stores total amounts donated by contributors. To compute these amounts, we simply group donations in the `fec_contributions` table by contributor and then sum up values in the `amount` column.

We then populate the `exclusivity_scores` table by first computing the percentage share associated with each donation and then summing over all donations associated with a given contributor-recipient pair. The percentage share associated with each donation is labeled exclusivity subscore and computed as the quotient between the donation's amount and the total amount donated by contributor from table total_donated_by_contributor. Finally, the exclusivity score is the result of the sum over all exclusivity subscores associated with a given pair.

#### In Depth

One would expect all donation amounts stored in the `fec_contributions` table to be positive values, since it doesn't make sense to speak of donations of negative amounts. If this were the case, all exclusivity scores would necessarily fall on a [0,1] range, and the sum of all exclusivity scores associated with a given contributor across all recipients would necessarily amount to exactly 1. (If you don't see this instantly, think about how exclusivity scores are defined: they represent the percentage share of donations allocated to each recipient, so it only makes sense that the sum of all percentage shares equals 100% of amount donated.)

However, the current instance of table fec_contributions contains over 3400 donations whose values in the `amount` column are negative. These negative values refer to refunds made by recipients. That negative donation amounts occur in the database slightly complicate the score computation, as they lead to the occurrence of a few instances in which exclusivity scores is larger than 1. We can't simply ignore these refunds in the process, as they are telling of the contributor-recipient relationship.

We find that there are 10 pairs for which exclusivity score originally evaluates to an amount higher than 1. As there are only 10 such pairs, we have addressed this issue by simply capping the score at 1. It makes sense for these 10 pairs to be assigned a score of 1 because their score would be 1 if the negative amounts were removed from consideration.

### Report Type Scores

Report type scores are built on top of the following premise: The earlier in the election cycle donations are made, the stronger a relationship between contributor and recipient there is. The assumption here is that early donations indicate that either the recipient asks the contributor before others or the contributor wants to establish a tie through proactively donating early.

We seek to translate this premise into a qualitative measure that rewards contributions that occur further from Election Day with high report type scores. We do so by looking at frequencies of different report types associated with donations, hence the name 'report type score'.

Each donation in the `fec_contributions` table is associated with a report type, which (as one would expect) indicates the type of report used to register donations with the FEC. Each report type is by definition associated with the period of the year when donation was reported. This, in combination with the year, makes report types convenient for determining how early in the election cycle donation was made.

Report types are used to compute report type scores as follows: Each report type is assigned a weight which indicates how early in the election cycle they refer to, higher weights being awarded to earlier periods. We find the frequencies of each report type associated with a contributor-recipient pair and then compute unnormalized report type scores as the product of the frequency of each report type by the corresponding weight. Finally, we normalize the scores by dividing all unnormalized scores by the maximum score found so as to ensure they fall in a [0,1] range.

#### Step By Step

The computation of report type scores requires several steps, the first of which is to read into the database a csv file detailing the report type weights to be used in computing report type scores. These weights are stored in the `report_type_weights` table.

Next, we count how many times each report type occurs in the collection of donations associated with each pair. These counts are split by year parity. In other words, we count how often each report type occurs for each contribution-recipient pair in odd years as well as how often they occur for each pair in even years. The goal here is to count the number of donations made at different points of the election cycle. The split by year parity is necessary because federal elections typically take place in even years only, meaning the correspondence between report types and periods of the election cycle differs according to year parity. These counts are stored in the `report_type_count_by_pair` table.

We then compute how many times each contributor-recipient pair occur in `fec_contributions`, that is, we get a count of donations made by each contributor to each recipient. These counts, which are stored in table pairs_count table, are nothing more than the number of occurrences of each pair in `fec_contributions`.

Once equipped with these two counts, we compute report type frequencies: how often each report type occurs in donations associated with each pair. These frequencies are simply the quotient between each report type count (from  `report_type_count_by_pair`) and pair count (from table pairs_count.) We store the results in the `report_type_frequency` table.

The next step is to compute report type subscores for each combination of contribution-recipient pair and report type present in `fec_contributions`. These subscores are simply the product of the frequency of each report type (from `report_type_frequency`) and the corresponding weights stored in the `report_type_weight` table.

Then, we find unnormalized report type scores by summing over all subscores associated with a given pair. In other words, the report type scores is the sum of all subscores corresponding to all combinations of report type and year parity that occur for each pair. We say these are unnormalized scores because it is possible (and indeed inevitable) that some pairs will have a score higher than 1. The `unnormalized_report_type_scores` table store these results.

The `max_report_type_score` table simply finds the maximum score in table `unnormalized_report_type_scores`. I decided to store the maximum unnormalized score in a separate table in order to optimize the normalization query. With maximum score stored in a single-column table, we avoid querying the unnormalized table for the maximum value at each row in the unnormalized scores table. [Check if this is actually true though, since it seems that mysql has max values readily available, in which case this is pointless as an optimization strategy.]

At last, we arrive at the final report type scores by normalizing scores in `unnormalized_report_type_scores`. Normalization is achieved by dividing all scores by maximum score stored in `max_report_type_score`. This way we ensure that scores fall in a scale from 0 to 1.

#### In Depth
One could argue a more objective measure of time such as date is to be preferred over report type for the purposes of pinpointing how early in the election cycle a donation is made, especially seeing as date is an attribute readily available in table fec_contributions. We choose to go with report types because they provide for a convenient grouping of donations made around the same time but not quite on the same date. Had we used date for this analysis, we would have had to come up with a method for clustering dates. If we look at report types as FEC-sanctioned clusters, it follows that we favored using a method already in place over devising a new one.

In fact, even report types are too granular a measure for our purposes, so much so that we grouped similar ones together in the process of assigning them weights. For instance, a weight of 1 is assigned to report types 12C, 12G, 12P, 12R, 12S, 30G, 30R, 30S in both even and odd years, and MY, M7, M8, M9, M10, M11 and M12 in even years. These report types represent donations made within a few months of Election Day. The goal is to differentiate between donations made very early in the election cycle, which get a weight of 4, and donations made towards the end of the campaign, which get a score of 1. Scores of 2 and 3 are assigned for donations in between. File report_types.csv lists the weight assigned to each report type score. Weight assignments are of course an editorial decision and as such are subject to criticism.

### Periodicity Scores

If a contributor donates periodically to the same contributor, their relationship is arguably strong, since said contributor is a source of funding the recipient can expect to count on. Periodicity scores seek to reward pairs for which donations are made around the same time of the year across the years. The goal is to quantify the temporal closeness of donations associated with a pair during the election cycle. Noting that closeness can be interpreted as the inverse of dispersion, we use the inverse of standard deviation – a measure of dispersion – as a means to compute periodicity score. The inverse of standard deviation correlates directly with periodicity, since data points with smaller standard deviation indicate that donations were made around the same time of the year. We favor standard deviation over variance due to its flatter curve, which leads to a less steep curve for periodicity scores.
To compute periodicity scores, we first map dates of donations associated with a pair into a 'day of the year' data point (i.e. number of days passed since Jan 1st) and then compute the inverse of the standard deviation of the resulting data points. If standard deviation is found to be zero, we say periodicity score is 0 if the data is made up of a single distinct point and 1 if the data is made up of multiple distinct points. Otherwise, periodicity score is simply the value found for the inverse of standard deviation.

We now expand on the case when standard deviation is zero. We note that if the standard deviation of a collection of data points is zero, then either the data is made up of a single point or all points in the data are congruent. In the context of donations, the former case means that contributor makes a one-time donation to recipient, while the latter means that contributor makes several donations to recipient on the same day of the year, (though not necessarily in the same year, since donations dates are mapped into a 'day of the year' measure.) It is reasonable to say one-time donations are not periodic and therefore merit a periodicity score of zero. The case when several donations are made on the very same day across the years reflects a highly periodic pattern of donations, to which we assign a periodicity score of 1.

#### Step By Step

Unlike the previous scores, there is no need to compute several tables before actually computing the unnormalized scores. A single query on `fec_contributions` does the trick.

We compute unnormalized periodicity scores as follows: first, we group donations by contributor-recipient pairs; then, we map donation dates to a 'day of year' measure through MySQL's DAYOFYEAR function; finally, we evaluate the standard deviation of the resulting data points. If standard deviation is zero, we look at the number of distinct data points that was used to compute the variance: if data is made up of a single data point, assign a periodicity score of 0, otherwise assign a score of 1. If standard deviation isn't zero, then periodicity score is the value of the inverse of standard deviation. Results are stored in `unnormalized_periodicity_scores`.

The same normalization strategy used before is applied here: we compute values in `periodicity_scores` as the quotient between unnormalized scores in `unnormalized_periodicity_scores` and the maximum periodicity score value stored in `max_periodicity_score`.

#### In Depth

Leap years introduce a slight imprecision in our periodicity score calculation. For dates from March to December, the value returned by MySQL's DAYOFYEAR function for dates in leap years exceeds by one unit the value returned for the same date in a non-leap year. As a result, a pair of donations made on the same day of the year in different years such that one but not the other is a leap year is treated as donations made a day apart from each other. They will be treated as distinct data points even though they refer to the same date. Because variance in this case is very low and so periodicity score is very close to 1 anyway, we let this slide.

On a separate note, we acknowledge that our method for computing periodicity scores may fail to adequately capture the periodical pattern of multimodal data points. If a contributor donates to a given recipient, say, every 4 months, the dataset that results from mapping donation dates into the day-of-year measure will be multimodal; as a result, standard deviation is difficult to interpret. [Makes sense in theory, but I wonder if this multimodal pattern occurs in the data at all. Check!]

### Maxed Out Scores

The rationale behind maxed out scores is very intuitive: Contributors have a stronger relationship with recipients to which they donate the maximum amount allowed under FEC regulations than with recipients that receive less than the contribution limit. Maxed out scores reward maxed out donations associated with a contributor-recipient pair.
To compute maxed out scores, we first identify contributor and recipient types and then assign a contribution limit to each contributor-recipient pair according to FEC rules. We then compute the value of each donation as a percentage share of the contribution limit associated with each pair. Finally, we add up percentage shares from all donations associated with a pair to arrive at an unnormalized score. The usual normalization procedure then ensues.

#### Step By Step

We start by computing `contributor_types`, which assigns a 'contributor_type' value to each contributor in table fec_contributions. Contributor types are one of 'national_party', 'other_party', 'multi_pac' and 'non_multi_pac'. Likewise, we compute table recipient_types to assign a 'recipient_type' value to each recipient in table fec_contributions. Recipient types are one of 'national_party', 'other_party', 'pac', 'candidate'. See the 'In Depth' section below for a detailed explanation of assignment rules.

We then create table contribution_limits by reading file limits.csv into the database. This file contains FEC-regulated contribution limits for each possible combination of contributor and recipient types. Next, we join  `contributor_types`, `recipient_types` and `fec_contributions` into `joined_contr_recpt_types`. This join associates each donation with a contributor type and a recipient type.

We're now ready to associate each donation with a contribution limit based on contributor and recipient types from table joined_contr_recpt_types and contribution limits from `contribution_limits`. We do that in  `maxed_out_subscores`, which computes the percentage share of contribution limit represented by each donation. Maxed out subscore is, in other words, the quotient between donation amount and contribution limit. Finally, we compute unnormalized maxed out scores for each contributor-recipient pair by summing over all subscores associated with a pair. The table `unnormalized_maxed_out_scores` stores these results.

As per our standard normalization method, we store highest score found in table `max_maxed_out_score` and then compute maxed out scores as the quotient between unnormalized scores and highest score found. Results are stored in  `maxed_out_scores`.

#### In Depth

Classification of contributor types is based on the following rules: If committee type is 'X' or 'Y', then contributor is either national party or other party committee. Other party here means state- or local-level committee. We use national parties' FEC IDs to make the distinction as follows: 'C00003418' and 'C00163022' (REPUBLICAN NATIONAL COMMITTEE), 'C00027466' (NATIONAL REPUBLICAN SENATORIAL COMMITTEE), 'C00075820' (NATIONAL REPUBLICAN CONGRESSIONAL COMMITTEE), 'C00000935' (DEMOCRATIC CONGRESSIONAL CAMPAIGN COMMITTEE), 'C00042366' (DEMOCRATIC SENATORIAL CAMPAIGN COMMITTEE), and 'C00010603' (DNC SERVICES CORPORATION/DEMOCRATIC NATIONAL COMMITTEE) are known to be national parties; all others are classified as 'other_party'. If committee type is one of 'N', 'Q', 'F', then contributor is either multicandidate PAC or non-multicandidate PAC. We use attribute 'multiqualify_date' from table fec_committees to distinguish between multicandidate and non-multicandidate PACs. We ignore all contributors associated with other committee types.

Classification of recipient types is based on the following rules: If committee type is one of 'H', 'S', 'P', 'A', 'B' then recipient is a candidate committee. If committee type is 'X' or  'Y' then contributors' rules also apply for recipients. If committee type is one of 'N', 'Q', 'F', 'G', then recipient is a PAC. We ignore all recipients associated with other committee types.

### Length Scores

The length score too has an intuitive premise: The longer the relationship between contributor and recipient lasts, the stronger their relationship is. We want to reward pairs that exhibit a long-lasting relationship between contributors and recipients. Length scores are simply the number of days passed between the first and the last donation associated with a contributor-recipient pair. We normalize the scores by assigning a score of 1 to the highest-scoring pair and scaling others accordingly.

#### Step By Step

We first compute unnormalized length scores as the difference between the first and the last date of donations in record, measured in days. This is readily accomplished with MySQL's DATEDIFF function. The unnormalized scores are stored in `unnormalized_length_scores`. We then normalize scores by first storing the highest value found in table max_length_score and then dividing all unnormalized scores by the highest value. Normalized scores are stored in `length_scores`.

#### In Depth

The scoring model we developed doesn't explicitly reward pairs in proportion to the absolute number of corresponding donations. Rather, the model seeks to flesh out patterns surrounding these contributions, namely periodicity, exclusivity and length of relationship as well as the timing of donations in the context of election cycles, the relative donation value with respect to the limit allowed by FEC and contributor's focus on specific races.
While there isn't a score explicitly devoted to rewarding multiple donations over one-time ones, we do acknowledge that both periodicity and length scores indirectly produce this side effect, as one-time donations necessarily get a periodicity score and a length score of zero. This is meant to counter-balance the relative easiness with which one-time donations can get high values for the other scores. One-time donations will get a high report type score as long as the donation is made early in the election cycle; they will get a high maxed out score as long as donation is close to contribution limit. Moreover, if contributor doesn't donate to other recipients, exclusivity and race focus score will necessarily be 1.

### Race Focus Scores

The motivation for this score is that contributors that give to recipients within a single race should see a bump in their relationship scores. Contributors that donate to races all over the country are not as invested in each particular race as contributors that focus on specific races. We compute race focus scores as the inverse of the count of the number of races a contributor donates to. Normalization is not necessary in this case.
Unlike the other five scores, race focus scores is assigned to each contributor only, (as opposed to contributor-recipient pairs.)

#### Step By Step

The first step is to compile a list of all races associated with donations in table fec_contributions. We define race as a unique combination of the following attributes: district, office state, branch and cycle. (Think about it: No two races will map into the same combination of these four attributes.) We store the results in `races_list`.
Now that we have a list of races associated with donations in the data, we count how many races each contributor is affiliated with, where affiliation means contributor donates to a recipient partaking in a race. We let race focus scores be the inverse of this count. The table `race_focus_scores` stores these results. This methodology necessarily constrains values within the [0,1] range, which removes the need to normalize values at the end as before.

#### In Depth

It is worth noting that the query used to compile a list of races relies on a regular expression ('REGEXP ^[HPS]'). This regular expression restricts the race list to candidates for the House, Senate or presidency.

### Final Scores

The final step is to combine the six scores computed – exclusivity scores, report type scores, periodicity scores, maxed out scores, length scores, and race focus scores – into a unique, final score. We accomplish this by joining the various scores tables and computing a weighted average of the scores, where weights are arbitrarily pre-determined.

#### Step By Step
We start by reading score weights to be attributed to each of the six scores from the CSV file score_weights.csv and storing them in `score_weights`. We then join the first five scores (all except race focus scores) on attributes `fec_committee_id` and `other_id`. Recall that `fec_committee_id` uniquely identifies contributors and `other_id` uniquely identifies recipients. These five scores are attributed to contributor-recipient pairs. The weighted average of these five scores is stored in `five_scores`.

Race focus scores, on the other hand, are attributed to contributors only, and so we separately join partial scores from `five_scores` and `race_focus_scores` on attribute `fec_committee_id`. This means all contributor-recipient pairs associated with the same recipient are assigned the same race focus scores in the computation of the final score. The final result in stored in the `final_scores` table.
