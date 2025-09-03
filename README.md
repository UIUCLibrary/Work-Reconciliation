# Setup
## Cache
This script requires Redis to run. If you do not already have it running, please follow [these instructions](https://redis.io/docs/latest/operate/oss_and_stack/install/archive/install-redis/) to install and setup.

To connect to the cache, you need to create an `application.conf` file with the following format:
```
[redis]
host = <host>
port = <port #>
loc_db = <database #>
wiki_db = <different database #>
```

## BIBFRAME XML
This script expects a BIBFRAME XML file as input, generated from LOC's [marc2bibframe2](https://github.com/lcnetdev/marc2bibframe2) tool. Specifically, this converted MARCXML into BIBFRAME XML, but the conversion won't work unless you have `xmlns="http://www.loc.gov/MARC21/slim"` in the `collection` tag of the MARCXML file.

# Running
The command to run the script should look like
```
python reconcileWorks.py <input.xml> <output directory> <loc|wikidata>
```

# Reconciliation Process
The reconciliation process differs based on what source we are using, so explinations are broken down by source.

## Library of Congress
All main and variant titles are collected for a given work to be used as search terms in LOC's [Linked Data Service](https://id.loc.gov). If present, mappings of the MARC Uniform Title title field are also collected to be used as search terms for the Hub. The search queries combine the title string with any relevant types for the record (such as Monograph or NotatedMusic) to narrow the search, plus a statement that directs the search at a specific source (in this case BIBFRAME Works or BIBFRAME Hubs).

The search returns an HTML page with a table of results. The process then combs through each result, assigns a score to it based on how well the result matches with the local record, and if the best score found is above a minumum threshold, that record and score are selected for the specific title search. This process is repeated for each title variant within a given record, and the highest score among all the titles is selected as the match that record.

The following table lists all fields that are searched for and the potential range of scores they could be assigned. If a field is not present in the local record we're trying to find a match for, that field will not be included in the score calculation. The score calculation is simply the sum of all included fields. A score is considered a match if it is greater than half the number of fields present. So if there are three fields, the score must be greater than 1.5.

| Field        | Score Range |
| ------------ | ----------- |
| title        | 0-0.5       |
| contributors | 0-3         |
| languages    | 0-1         |
| notes        | 0-1         |
| hub          | 1           |

Here is an overview of the logic for how each field is scored:

### title
The score for the title is simply the Levenshtein Distance between the local and the candidate title, subtracted by the length of the local title, all divided by the length of the local title. The title with the highest score is selected.

### contributors
Contributors are split into two groups: Primary and Secondary. Each group gets its own score based on the Levenshtein Distance between the local and the candidate contributor, and just like with title, the distance is subracted from the length of the local candidate string, and divided by that same length. This calculated value must be greater than 0.5 to be selected. All the calculated values for a group are added together and divided by the number of entries in that group. These values are added together. If more than one contributor is found, that is seen as a strong indicator of a match, so the Primary score is doubled before being added to the Secondary score. If there are no matches on the Primary contributors, then the Secondary score is doubled instead.

### languages
Languages are stored as URIs in BIBFRAME, so we simply search a candidate record for each language URI in the local record, tally the number of matches, and divide the total by the number of languages listed in the local record.

### notes
Notes tend to be specific and longer than titles, so only close matches is significant. Therefore, we are more strict in what is considered a match while using a similar formula. We calculate the Levenshtein Distance, but only accept it if it is less than 10% the length of the note. For those cases we subtract the distance from the length of the note and divide that number by the length of the note. This is done for every note, then the scores for each note are added up and divided by the total number of notes in the local record.

### hub
If a selected work lists any associated hubs, those are passed on to the hub search process. If a search result is in that list, a value of 1 is set. Alternately, if a search result hub lists associated works and the work that was selected is in that list, a value of 1 is set. If no match is found, the hub field is not included, so it can only ever improve a match score.

## Wikidata
Wikidata work records don't have an equivalent to BIBFRAME's generic "contributor" – instead every contributor is related by their specific role in the creation of a work, making it difficult to follow the work-centric approach that is used for LOC. Instead, we take the contributors from the local work and search Wikidata for them, either by using URIs in the $1 subfield, or if there is no URI, by using the $a subfield in Wikidata's search service. We then query Wikidata's SPARQL endpoint to get the top occupation of the contributor (for example composer), then run another query to retrieve all works that are connected to the contributor by their occupation. We then calculate the Levenshtein Distance between the local record and the candidate works, selecting results that are less than 10% the length of the local title, and if they are subtracting that value from the length of the local title and dividng the result by the local title length. The candidate work with the best score is considered the match.