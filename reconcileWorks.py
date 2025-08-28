import argparse, sys, os, logging, requests, csv, urllib.parse, copy, json, configparser, time, redis
from lxml import etree
from enum import Enum
from redis.commands.json.path import Path
from limits import RateLimitItemPerMinute
from limits.storage import MemoryStorage
from limits.strategies import FixedWindowRateLimiter

logging.basicConfig(level=logging.WARNING,format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s',datefmt='%H:%M:%S')

class BrokenResponse:
	status_code = '400'

class Sources(Enum):
	loc = "loc"
	wikidata = "wikidata"

	def __str__(self):
		return self.value

class Namespaces(str, Enum):
	BF = "http://id.loc.gov/ontologies/bibframe/"
	RDF = "http://www.w3.org/1999/02/22-rdf-syntax-ns#"
	BFLC = "http://id.loc.gov/ontologies/bflc/"
	RDFS = "http://www.w3.org/2000/01/rdf-schema#"
	MADSRDF = "http://www.loc.gov/mads/rdf/v1#"

	def __str__(self):
		return self.value

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

storage = MemoryStorage()
limiter = FixedWindowRateLimiter(storage)
loc_limit = RateLimitItemPerMinute(200)

def calculateLevenshteinDistance(string1,string2):
	matrix = []
	for counter1 in range(0,len(string1)+1):
		empty_row = []
		for counter2 in range(0,len(string2)+1):
			empty_row.append(0)
		matrix.append(empty_row)
	
	for i in range(1,len(string1)+1):
		matrix[i][0] = i

	for j in range(1,len(string2)+1):
		matrix[0][j] = j

	for j in range(1,len(string2)+1):
		for i in range(1,len(string1)+1):
			if string1[i-1] == string2[j-1]:
				matrix[i][j] = matrix[i-1][j-1]
			else:
				matrix[i][j] = min(matrix[i-1][j]+1, matrix[i][j-1]+1,matrix[i-1][j-1]+1)

	return matrix[len(string1)][len(string2)]

# Stripping whitespace varies based on character encoding, and LOC results aren't always consistent
#	with their character encodings, so to create a list of strings to check against the search term,
#	different functions need to be called.
def normalizeVariant(variant):
	if isinstance(variant,str):
		return variant.strip()
	elif isinstance(variant,unicode):
		return variant.encode('utf-8').strip()

def getRequest(url):
	if 'id.loc.gov' in url:
		while not limiter.hit(loc_limit, "loc"):
			logging.debug("Hit limit")
			time.sleep(0.5)
	try:
		result = requests.get(url, headers={ 'User-Agent': 'reconcileWorks / 0.1 University Library, University of Illinois' }, timeout=60)
		if result.status_code == 429:
			logging.debug(result.headers.get("Retry-After"))
			time.sleep(60)
			result = requests.get(url,timeout=60)
	except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, ValueError) as e:
		logging.error(e)
		try:
			if result:
				logging.debug(result.status_code)
		except:
			result = BrokenResponse()

	return result

# Utility for structuring notes as needed for processing
def getNotes(notes):
	note_list = []
	for n in notes:
		n_children = n.xpath('./child::*')

		new_note = {}
		for n_child in n_children:
			if n_child.text:
				new_note[n_child.tag] = " ".join(n_child.text.split())
			elif n_child.attrib:
				new_note[n_child.tag] = n_child.attrib[n_child.attrib.keys()[0]]

		note_list.append(new_note)

	return note_list

# Search for each local note in LOC record. Note matches are made by calcualting the 
# Levenshtein Distance, but only selecting cases where the Levenshtein Distance is 
# less than 10% of the length of the note. This filter exists because most notes are
# pretty long and unique and should be pretty similar to look like a match.
#
# Among that group the closest match is found and given a score between 0 and 1. Once
# all notes have been searched for, the final score is based on the sum of all matches
# divided by the number of notes found. This is more generous than dividing by the 
# number of notes in the local record because a match is seen as pretty significant
# since they can be so unique.
def compareNotes(local_notes,loc_notes):
	logging.debug(f"\t\tCalculating score based on note similarities")
	logging.debug(f"\t\tLocal notes: {local_notes}")
	logging.debug(f"\t\tLOC notes: {loc_notes}")
	if len(local_notes) > 0:
		found_note_count = 0
		found_note_value = 0
		for note in local_notes:
			for loc_note in loc_notes:
				logging.debug(f"\t\tLocal note: {note}")
				logging.debug(f"\t\tLOC note: {loc_note}")
				score_card = 0
				score_value = 0
				for element in note:
					if element in loc_note:
						if element == '{http://www.w3.org/2000/01/rdf-schema#}label':
							l_dist = calculateLevenshteinDistance(note[element],loc_note[element])

							if l_dist < len(note[element]) * 0.1:
								logging.debug(f"\t\tMax allowed distance: {len(note[element]) * 0.1}")
								score_card += 1
								score_value += (len(note[element]) - l_dist)/(len(note[element]))
						else:
							if note[element] == loc_note[element]:
								score_card += 1
								score_value += 1

				if score_card == len(note):
					found_note_count += 1
					found_note_value += (score_value / score_card) if score_card > 0 else 0
					break

		return (found_note_value / found_note_count) if found_note_count > 0 else 0
	else:
		return 0

# Find the best fit of all possible title matches based on Levenshtein Distance. Use the
# distance to generate a value between 0 and 1. Highest score is reutrned, but divided 
# in half to lessen the weight of title matches.
def compareTitles(target_title,candidate_titles):
	best_fit = 0
	logging.debug(f"\t\tCalculating score based on title similarities")
	for candidate in candidate_titles:
		logging.debug(f"\t\t{target_title}")
		logging.debug(f"\t\t{candidate}")
		l_dist = calculateLevenshteinDistance(target_title,candidate)
		logging.debug(f"\t\tDistance: {l_dist}")
		normalized_value = (len(target_title) - l_dist)/len(target_title)
		if normalized_value > best_fit:
			best_fit = normalized_value
		logging.debug(f"\t\tAdjusted score: {normalized_value}")
	return (best_fit * 0.5)

# Grab contributor names from LOC record, either taking the plain text, or following links and
# taking the labels from those. The fact that most contributors are represented as links could
# greatly expand runtime, so the results are stored in a Redis cache to prevent duplicate calls.
#
# Once the LOC candidates are collected, search for each contributor from the local record in 
# that list. Matches are found by checking if the contributor types match, and calculating the 
# Levenshtein Distance between the two strings. If the types match, that is given a score of 1.
# The Levenshtein Distance is used to create a score between 0 and 1 where 1 is an exact match.
# When the best score is found it will be normalized to the 0-1 range based on how big the final
# score can be.
#
# When a match is found, the best score is added to a running tally and the number of matches 
# found is incremented. The final output is based on this total. No matches will return 0. One 
# match will return the score divided by the total number of contributors listed in the local
# record, which will result in some value 0-1. If more than one contributor is found, the score
# is two times the score over the number of potential contributors, which will result in some
# value 0-2.
def compareContributors(local_contributors,loc_contributors,cache_connection):
	if len(local_contributors) > 0 and len(loc_contributors) > 0:
		found_contributor_count = 0
		found_contributor_value = 0

		logging.debug("\t\tCalculating score based on contributor similarities")
		loc_values = []
		for loc_contributor in loc_contributors:
			loc_contributor_values = {}
			loc_type = loc_contributor.xpath("./rdf:type/@rdf:resource",namespaces={"bf": Namespaces.BF,"rdf": Namespaces.RDF})
			if len(loc_type) > 0:
				loc_contributor_values['type'] = loc_type[0]

			loc_agent_links = loc_contributor.xpath("./bf:agent/@rdf:resource",namespaces={"bf": Namespaces.BF,"rdf": Namespaces.RDF})
			logging.debug(f"\t\tLOC contributor links: {loc_agent_links}")
			if len(loc_agent_links) > 0:
				res = cache_connection.get(loc_agent_links[0])
				if res:
					loc_contributor_values['agent'] = res
				else:
					if 'id.loc.gov' in loc_agent_links[0]:
						request_uri = loc_agent_links[0]
						if request_uri.find('https') != 0:
							request_uri.replace('http','https')

						agent_tree = etree.XML(getRequest(f"{request_uri}.rdf").content)
						agent_label = agent_tree.xpath("/rdf:RDF/madsrdf:RWO/rdfs:label/text()",namespaces={"rdf": Namespaces.RDF,"rdfs": Namespaces.RDFS,"madsrdf": Namespaces.MADSRDF})

						if len(agent_label) > 0:
							loc_contributor_values['agent'] = agent_label[0]
							cache_connection.set(loc_agent_links[0],agent_label[0])
					else:
						logging.debug(f"\t\tAgent link is not from loc: {loc_agent_links[0]}")
			else:
				loc_agent_list = loc_contributor.xpath("./bf:agent/bf:Agent/rdfs:label/text()",namespaces={"bf": Namespaces.BF,"rdfs": Namespaces.RDFS})
				if len(loc_agent_list) > 0:
					loc_contributor_values['agent'] = loc_agent_list[0]

			if len(loc_contributor_values) > 0:
				loc_values.append(loc_contributor_values)
		logging.debug(f"\t\tLOC contributor names: {loc_values}")

		for local_contributor in local_contributors:
			best_score_count = 0
			best_score_value = 0

			local_type = local_contributor.xpath("./rdf:type/@rdf:resource",namespaces={"bf": Namespaces.BF,"rdf": Namespaces.RDF})
			local_agent = local_contributor.xpath("./bf:agent/bf:Agent/rdfs:label/text()",namespaces={"bf": Namespaces.BF,"rdfs": Namespaces.RDFS})
			logging.debug(f"\t\tLooking for a match on local contributor {local_agent[0]}")
			for val in loc_values:
				logging.debug(f"\t\tChecking LOC contributor {val}")
				score_count = 0
				score_value = 0

				if len(local_type) > 0:
					if 'type' in val and local_type[0] == val['type']:
						score_count += 1
						score_value += 1

				if len(local_agent) > 0:
					score_count += 1

					if 'agent' in val:
						logging.debug(f"\t\tLOC contriubytor name: {val['agent']}")

						l_dist = calculateLevenshteinDistance(local_agent[0],val['agent'])

						score_value += (len(local_agent[0]) - l_dist) / len(local_agent[0])

				if (score_count > best_score_count or score_value > best_score_value):
					best_score_count = score_count
					best_score_value = score_value

					logging.debug(f"\t\tupdated score count: {best_score_count}")
					logging.debug(f"\t\tupdated score value: {best_score_value}")

					if len(local_agent) > 0 and 'agent' in val and l_dist == 0:
						break

			if best_score_count != 0:
				found_contributor_count += 1
				found_contributor_value += (best_score_value / best_score_count)

		logging.debug(f"\t\tfound contributor count: {found_contributor_count}")
		logging.debug(f"\t\tfound contributor value: {found_contributor_value}")
		if found_contributor_count > 1:
			return (2 * (found_contributor_value / found_contributor_count))
		elif found_contributor_count > 0:
			return (found_contributor_value / found_contributor_count)
		else:
			return 0
	else:
		return 0

# Feed in dict that contains results for each title variation, which includes 'matches' with
# scores for each element present in the local record that we are searching for. These scores
# generally range from 0-1, except for title which only goes up to 0.5 to lower the weight of
# those matches, and contributor which can go up to 2 if there are multiple contributor matches.
# The URI with the best score is returned along with the associated name and scores. The scores
# must add up to be greater than half of number of scored parameters to count as a match. If 
# no result meets that threshold, we return null values to indicate that no match has been found.
# The threshold can be adjusted in the future to calibrate how strict the matches should be.
def findBestMatch(scores_by_title):
	best_score = 0
	best_score_breakdown = {}
	best_url = None
	best_name = None
	for title in scores_by_title:
		logging.debug(f"\t\tSearching for best match on results from {title}")
		for match in scores_by_title[title]['matches']:
			url = match
			logging.debug(f"\t\tProcessing: {scores_by_title[title]}")
			score = sum(scores_by_title[title]['matches'][match].values())
			if score > best_score and score > (len(scores_by_title[title]['matches'][match])/2.0):
				best_score = score
				best_url = url
				best_score_breakdown = copy.deepcopy(scores_by_title[title]['matches'][match])
				best_name = title

			logging.debug(f"\t\tScoring {match}")
			logging.debug(f"\t\tIndividual scores: {scores_by_title[title]['matches'][match].values()}")
			logging.debug(f"\t\tFinal score: {score}")

	logging.debug(f"\t\tBest score from search results: {best_score}")
	logging.debug(f"\t\tBest name from search results: {best_name}")
	return (best_url, best_name, best_score_breakdown, False if best_url else True)

# Search LOC based on title text, types and specify if searching for a Work or Hub.
# If there are results, try to find matches for title, language, contributor, and
# notes fields. Create a score for each of these fields based on how well they match.
# If a field isn't present in record we're searcing for, don't create a score for that.
# Scores are kept for each search result, and the best score is selected at the end as
# long as it is higher than a minimum threshold.
#
# When Works are being processed, we keep track of any associated Hubs, so if a Work is 
# selected, thos Hubs are also returned as candidates to be checked alongside the search
# results.
def searchForRecordLOC(placeholder_work_id,match_fields,resource,types,output_writer,cache_connection,work_uri=None,candidate_hubs=None):
	results_by_title = {}
	for text_string in match_fields['titles']:
		BASE_LC_URL = 'https://id.loc.gov/search/?q='
		ENCODED_TEXT_STRING = urllib.parse.quote_plus(text_string)
		RDFTYPES = "".join([f"+rdftype:{x.rsplit('/',1)[1]}" for x in types])
		ENCODED_RESOURCE_URL = urllib.parse.quote_plus(resource)
		query_url = f"{BASE_LC_URL}{ENCODED_TEXT_STRING}{RDFTYPES}&q=cs:{ENCODED_RESOURCE_URL}"
		logging.debug(f"\tConducting LOC search: {query_url}")

		match_not_found = True
		i = 0
		results_by_title[text_string] = {}
		matches = {}
		hubs = {}
		try:
			results_tree = etree.HTML(getRequest(query_url).content)
			result_table = results_tree.xpath("//table[@class='id-std']/tbody/tr")

			while i < len(result_table):
				authorized_heading = result_table[i].xpath("./td/a/text()")
				logging.debug(f"\tAUTHORIZED HEADING: {authorized_heading}")
				variant_headings = result_table[i+1].xpath("./td[@colspan='5']/text()")
				logging.debug(f"\tVARIANT HEADINGS: {variant_headings}")
				if len(variant_headings) > 0:
					variant_headings = map(normalizeVariant,variant_headings[0].split(';'))

				if len(authorized_heading) > 0 or len(variant_headings) > 0:
					logging.debug(f"\tFound {text_string}")
					found_uri = 'http://id.loc.gov' + result_table[i].xpath("./td/a/@href")[0]
					logging.debug(f"\t{found_uri}")
					details_tree = etree.XML(getRequest(f"{found_uri.replace('http','https')}.bibframe.rdf").content)
					details_title = details_tree.xpath("/rdf:RDF/bf:Work/bf:title/bf:Title/bf:mainTitle/text()",namespaces={"bf": Namespaces.BF,"rdf": Namespaces.RDF})
					logging.debug(f"\tTitle from record: {details_title}")
					details_variant_title = details_tree.xpath("/rdf:RDF/bf:Work/bf:title/bf:VariantTitle/bf:mainTitle/text()",namespaces={"bf": Namespaces.BF,"rdf": Namespaces.RDF})
					logging.debug(f"\tVariant titles from record: {details_variant_title}")
					found_titles = set(authorized_heading + variant_headings + details_title + details_variant_title)

					logging.debug(f"\tALL SEARCH TITLES: {found_titles}")
					matches[found_uri] = { 'title': compareTitles(text_string,found_titles) }

					logging.debug(f"Searching for fields: {match_fields}")
					if len(match_fields['languages']) > 0:
						record_languages = details_tree.xpath("/rdf:RDF/bf:Work/bf:language/@rdf:resource",namespaces={"bf": Namespaces.BF,"rdf": Namespaces.RDF})
						language_match_count = 0
						for lang in match_fields['languages']:
							if lang in record_languages:
								language_match_count += 1
						
						matches[found_uri]['languages'] = language_match_count / len(match_fields['languages'])

					if len(match_fields['contributors']) > 0:
						record_contributors = details_tree.xpath("/rdf:RDF/bf:Work/bf:contribution/bf:Contribution", namespaces={"bf": Namespaces.BF,"rdf": Namespaces.RDF})
						matches[found_uri]['contributors'] = compareContributors(match_fields['contributors'],record_contributors,cache_connection)
						logging.debug(f"\tMatches updated with contributor: {matches[found_uri]}")

					if len(match_fields['notes']) > 0:
						record_notes = details_tree.xpath("/rdf:RDF/bf:Work/bf:note/bf:Note",namespaces={"bf": Namespaces.BF,"rdf": Namespaces.RDF})
						notes_a = getNotes(match_fields['notes'])
						notes_b = getNotes(record_notes)

						matches[found_uri]['notes'] = compareNotes(notes_a,notes_b)
						logging.debug(f"\tMatches updated with notes: {matches[found_uri]}")

					if 'hubs' in resource:
						# Check list of pre-identified hubs for the current search result, if that isn't 
						# present, check to see if the current search result links back to the selected
						# Work
						if candidate_hubs and found_uri in candidate_hubs:
							matches[found_uri]['hub'] = 1
						else:
							record_works = details_tree.xpath("/rdf:RDF/bf:Work/bf:hasExpression/@rdf:resource",namespaces={"bf": Namespaces.BF,"rdf": Namespaces.RDF})
							logging.debug(f"\tHub hasExpression list: {record_works}")
							if work_uri in record_works:
								matches[found_uri]['hub'] = 1
					else:
						record_hubs = details_tree.xpath("/rdf:RDF/bf:Work/bf:expressionOf/@rdf:resource",namespaces={"bf": Namespaces.BF,"rdf": Namespaces.RDF})
						hubs[found_uri] = record_hubs

				i = i + 2

		except Exception as e:
			logging.error(e)
			if len(match_fields['titles'] == 1):
				output_writer.writerow([placeholder_work_id,text_string,query_url,"QUERY ERROR",e])
				return None, None
			else:
				# Need some way to log errors that don't break the process so we can come back and fix them later
				pass

		results_by_title[text_string]['matches'] = matches
		results_by_title[text_string]['hubs'] = hubs

	logging.debug(f"\tScores for all search results: {results_by_title}")
	selected_url, selected_name, selected_breakdown, match_not_found = findBestMatch(results_by_title)
	logging.debug(f"\tBest match from search results: {selected_url}")
	logging.debug(f"\tMatch not found: {match_not_found}")
	if selected_url:
		logging.debug(f"\tWriting results to spreadsheet: {placeholder_work_id}, {match_fields['titles'][0]}, {query_url}, {json.dumps(selected_breakdown)}, {selected_url}")
		output_writer.writerow([placeholder_work_id,match_fields['titles'][0],query_url,json.dumps(selected_breakdown),selected_url])
		if 'hubs' in resource:
			return selected_url, None
		else:
			return selected_url, results_by_title[selected_name]['hubs'][selected_url] if len(results_by_title[selected_name]['hubs'][selected_url]) > 0 else None

	if match_not_found:
		logging.debug(f"{placeholder_work_id}, {match_fields['titles'][0]}, {query_url},")
		output_writer.writerow([placeholder_work_id,match_fields['titles'][0],query_url])
		return None, None

def searchForRecordWiki(placeholder_work_id,match_fields,cache_connection,output_writer):
	BASE_WIKIDATA_URL = "https://www.wikidata.org/w/api.php"
	BASE_WIKIDATA_SPARQL_URL = "https://query.wikidata.org/bigdata/namespace/wdq/sparql"

	best_work_score = 0
	best_work = None
	best_work_uri = None
	for contributor in match_fields['contributors']:
		split_contributor = contributor.split('$')
		split_contributor.pop(0)
		logging.debug(split_contributor)
		marc_contributor = { x[0]: x[1:] for x in split_contributor }
		logging.debug(marc_contributor)
		res = cache_connection.hget(marc_contributor['a'],'empty')
		logging.debug(res)

		if res == 'True':
			continue
		else:
			contributor_works = {}
			contributor_found = False
			if '1' in marc_contributor:
				if 'https://id.oclc.org/worldcat/entity' in marc_contributor['1']:
					ID_STRING = marc_contributor['1'][marc_contributor['1'].rfind('/')+1:]
					SPARQL_QUERY = f"""SELECT ?contrib
WHERE
{{
  ?contrib wdt:P10832 "{ID_STRING}"
}}"""
					sparql_query_url = f"{BASE_WIKIDATA_SPARQL_URL}?format=json&query={urllib.parse.quote_plus(SPARQL_QUERY)}"
					query_results = json.loads(getRequest(sparql_query_url).content)
					logging.debug(query_results)
					if len(query_results['results']['bindings']) > 0:
						contributor_code = query_results['results']['bindings'][0]['contrib']['value']
						contributor_code = contributor_code[contributor_code.rfind('/')+1:]
						contributor_found = True

			if not contributor_found:
				ENCODED_CONTRIBUTOR = urllib.parse.quote_plus(marc_contributor['a'])
				wikidata_query = f"{BASE_WIKIDATA_URL}?action=query&list=search&srsearch={ENCODED_CONTRIBUTOR}&format=json"
				wikidata_search = json.loads(getRequest(wikidata_query).content)
				logging.debug(wikidata_search)
				if len(wikidata_search['query']['search']) > 0:
					contributor_code = wikidata_search['query']['search'][0]['title']
				else:
					contributor_code = None
					logging.debug(f"Non-WorldCat Entity URL: {marc_contributor['a']}")
			
			if contributor_code:
				OCCUPATION_QUERY = f"""SELECT ?occupation_properties
WHERE
{{
  wd:{contributor_code} wdt:P106 ?occupations .
  ?occupations wdt:P1687 ?occupation_properties.
}}"""
				logging.debug(contributor_code)
				occupation_query_url = f"{BASE_WIKIDATA_SPARQL_URL}?format=json&query={urllib.parse.quote_plus(OCCUPATION_QUERY)}"
				occupation_query_results = json.loads(getRequest(occupation_query_url).content)
				logging.debug(occupation_query_results)
				occupation_property = occupation_query_results['results']['bindings']
				if len(occupation_property) > 0:
					occupation_property = occupation_property[0]['occupation_properties']['value']
					occupation_property = occupation_property[occupation_property.rfind('/')+1:]
					logging.debug(occupation_property)
					WORKS_QUERY = f"""SELECT ?works ?worksLabel
WHERE
{{
  ?works wdt:{occupation_property} wd:{contributor_code} .
  SERVICE wikibase:label {{ bd:serviceParam wikibase:language "[AUTO_LANGUAGE],mul,en". }}
}}"""
					works_query_url = f"{BASE_WIKIDATA_SPARQL_URL}?format=json&query={urllib.parse.quote_plus(WORKS_QUERY)}"
					works_query_results = json.loads(getRequest(works_query_url).content)
					logging.debug(works_query_results)
					if len(works_query_results['results']['bindings']) > 0:
						for w in works_query_results['results']['bindings']:
							logging.debug(w)
							contributor_works[w['works']['value']] = w['worksLabel']['value']

			logging.debug(contributor_works)
			if len(contributor_works) > 0:
				cache_connection.hset(marc_contributor['a'], mapping=contributor_works)
			else:
				cache_connection.hset(marc_contributor['a'], mapping={ 'empty': 'True' })

		logging.debug(marc_contributor['a'])
		for found_work in cache_connection.hscan_iter(marc_contributor['a']):
			for t in match_fields['titles']:
				l_dist = calculateLevenshteinDistance(t,found_work[1])
				if l_dist < len(t) * 0.1:
					logging.debug(len(t) * 0.1)
					score_value = (len(t) - l_dist)/(len(t))
					if score_value > best_work_score:
						best_work_score = score_value
						best_work = found_work[1]
						best_work_uri = found_work[0]


	if best_work_score > 0 and best_work and best_work_uri:
		logging.debug(f"{placeholder_work_id}, {json.dumps(match_fields)}, {best_work}, {best_work_score}, {best_work_uri}")
		output_writer.writerow([placeholder_work_id,json.dumps(match_fields),best_work,best_work_score,best_work_uri])
		return best_work_uri
	else:
		logging.debug(f"{placeholder_work_id}, {json.dumps(match_fields)}")
		output_writer.writerow([placeholder_work_id,json.dumps(match_fields)])
		return None

def clearBlankText(text_array):
	return " ".join([x for x in text_array if x.strip() != ''])

def init(args):
	if not args.input.endswith('.xml'):
		raise Exception("Input file must be an XML file")

	if args.verbose:
		logging.getLogger().setLevel(logging.DEBUG)

	config = configparser.ConfigParser()
	config.read('application.config')

	redis_connected = False
	while not redis_connected:
		try:
			loc_cache_connection = redis.Redis(host=config.get('redis','host'), port=config.get('redis','port'), db=0, decode_responses=True)
			wiki_cache_connection = redis.Redis(host=config.get('redis','host'), port=config.get('redis','port'), db=1, decode_responses=True)
			if loc_cache_connection.ping() and wiki_cache_connection.ping():
				redis_connected = True
		except Exception as e:
			logging.error(f'Error initializing cache {e}')
			time.sleep(5)
			logging.info('Retrying cache initialization')

	os.makedirs(args.output,exist_ok=True)

	return loc_cache_connection, wiki_cache_connection

def reconcileWorks(args):
	loc_cache_connection, wiki_cache_connection = init(args)
	
	parser = etree.XMLParser(remove_blank_text=True)
	tree = etree.parse(args.input, parser)
	root = tree.getroot()
	works = root.xpath('/rdf:RDF/bf:Work', namespaces={ "rdf": Namespaces.RDF, "bf": Namespaces.BF })

	with open(f"{args.output}{SLASH}{args.input.rsplit('/',1)[1][:-4]}_{args.source}.tsv",'w') as outfile:
		writer = csv.writer(outfile,delimiter='\t')
		for work in works:
			# Select identifying characteristics of Work, and search based on those values
			placeholder_work_id = work.xpath("./@rdf:about", namespaces={ "rdf": Namespaces.RDF })[0]
			logging.debug(f"Processing new Work with placeholder id: {placeholder_work_id}")
			work_title = work.xpath("./bf:title/bf:Title//text()", namespaces={ "bf": Namespaces.BF })
			work_title_text = clearBlankText(work_title)
			work_types = work.xpath("./rdf:type/@rdf:resource", namespaces={ "rdf": Namespaces.RDF })
			logging.debug(f"Found work types: {work_types}")

			variant_titles = work.xpath("./bf:title/bf:VariantTitle", namespaces={ "bf":Namespaces.BF })
			variant_titles_text = [clearBlankText(variant_title.xpath(".//text()")) for variant_title in variant_titles]

			contributors = work.xpath("./bf:contribution/bf:Contribution", namespaces={ "bf": Namespaces.BF })

			search_titles = [work_title_text]
			if variant_titles_text:
				search_titles += variant_titles_text

			if args.source == Sources.loc:
				notes = work.xpath("./bf:note/bf:Note", namespaces={ "bf": Namespaces.BF })

				languages = work.xpath("./bf:language/@rdf:resource", namespaces={ "bf": Namespaces.BF, "rdf": Namespaces.RDF })

				match_fields = { 'titles': search_titles, 'notes': notes, 'languages': languages, 'contributors': contributors }
				
				# Find best match for Work, and if that Work has any linked Hubs, add that to our list of Hubs to check
				found_work_uri, found_work_associated_hubs = searchForRecordLOC(placeholder_work_id,match_fields,'http://id.loc.gov/resources/works',work_types,writer,loc_cache_connection)
				found_hub_uri, trash = searchForRecordLOC(placeholder_work_id,match_fields,'http://id.loc.gov/resources/hubs',['http://id.loc.gov/ontologies/bibframe/Work','http://id.loc.gov/ontologies/bibframe/Hub'],writer,loc_cache_connection,found_work_uri,found_work_associated_hubs)

			elif args.source == Sources.wikidata:
				match_fields = { 'titles': search_titles, 'contributors': contributors }
				marc_keys = []
				found_primary = False
				for contributor in match_fields['contributors']:
					contributor_labels = contributor.xpath("./bf:agent/bf:Agent/bflc:marcKey/text()",namespaces={"bf": Namespaces.BF,"bflc": Namespaces.BFLC})

					contributor_types = contributor.xpath("./rdf:type/@rdf:resource",namespaces={"bf": Namespaces.BF,"rdf": Namespaces.RDF})
					if 'http://id.loc.gov/ontologies/bibframe/PrimaryContribution' in contributor_types:
						match_fields['contributors'] = contributor_labels
						found_primary = True
						break
					else:
						marc_keys += contributor_labels

				if not found_primary:
					match_fields['contributors'] = marc_keys

				found_work_uri = searchForRecordWiki(placeholder_work_id,match_fields,wiki_cache_connection,writer)

			# Make Instances point to new URI
			if found_work_uri:
				work.set('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about',found_work_uri)
				instances = root.xpath(f"/rdf:RDF/bf:Instance[bf:instanceOf/@rdf:resource=\"{placeholder_work_id}\"]", namespaces={ "rdf": Namespaces.RDF, "bf": Namespaces.BF })
				for instance in instances:
					instance.xpath(f"./bf:instanceOf[@rdf:resource=\"{placeholder_work_id}\"]",namespaces={ "rdf": Namespaces.RDF, "bf": Namespaces.BF })[0].set(f"{{{Namespaces.RDF}}}resource",found_work_uri)

			# Add a new Work for a found Hub that points back at the Work it was derived from
			if found_hub_uri:
				expression_of = etree.SubElement(work,f"{{{Namespaces.BF}}}expressionOf")
				expression_of.set(f"{{{Namespaces.RDF}}}resource",found_hub_uri)

				new_hub = etree.SubElement(root,f"{{{Namespaces.BF}}}Work")
				new_hub.set(f"{{{Namespaces.RDF}}}about",found_hub_uri)
				hub_type = etree.SubElement(new_hub,f"{{{Namespaces.RDF}}}type")
				hub_type.set(f"{{{Namespaces.RDF}}}resource","http://id.loc.gov/ontologies/bibframe/Hub")
				has_expression = etree.SubElement(new_hub,f"{{{Namespaces.BF}}}hasExpression")
				has_expression.set(f"{{{Namespaces.RDF}}}resource", found_work_uri if found_work_uri else placeholder_work_id)

	with open(f"{args.output}{SLASH}{args.input.rsplit('/',1)[1][:-4]}_{args.source}.xml",'wb') as out_xml_file:
		out_xml_file.write(etree.tostring(tree,pretty_print=True))

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("input", help="BIBFRAME XML file to process")
	parser.add_argument("output", help="Directory to write the output to")
	parser.add_argument("source", type=Sources, choices=list(Sources), help="Run queries on LOC or Wikidata")
	parser.add_argument("-v", "--verbose", action="store_true")
	args = parser.parse_args()

	reconcileWorks(args)