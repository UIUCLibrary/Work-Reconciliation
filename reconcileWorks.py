import argparse, sys, os, logging, requests, csv, urllib.parse, copy, json, configparser, time, redis
from lxml import etree
from enum import Enum
from redis.commands.json.path import Path

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s',datefmt='%H:%M:%S')

class BrokenResponse:
	status_code = '400'

class Sources(Enum):
	loc = "loc"
	wikidata = "wikidata"

	def __str__(self):
		return self.value

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

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

	#for i in range(1,len(string1)+1):
	#	print matrix[i]

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
	try:
		result = requests.get(url,timeout=60)
		if result.status_code == 429:
			time.sleep(60)
			result = requests.get(url,timeout=60)
	except (requests.exceptions.ConnectionError, requests.exceptions.Timeout, ValueError) as e:
		logging.error(e)
		try:
			if result:
				logging.debug(result.status_code)
		except:
			result = BrokenResponse()
#	logging.debug(result.status_code)
#	logging.debug(result.content)
	return result

def getNotes(notes):
	note_list = []
	for n in notes:
		n_children = n.xpath('./child::*')
#		logging.debug(n_children)
		new_note = {}
		for n_child in n_children:
			if n_child.text:
				new_note[n_child.tag] = " ".join(n_child.text.split())
			elif n_child.attrib:
				new_note[n_child.tag] = n_child.attrib[n_child.attrib.keys()[0]]
#			logging.debug(n_child)
#			logging.debug(n_child.tag)
#			logging.debug(n_child.text)
#			logging.debug(n_child.attrib)

		note_list.append(new_note)
#	logging.debug("NOTE LIST")
#	logging.debug(note_list)
	return note_list

def compareNotes(local_notes,loc_notes):
	logging.debug(local_notes)
	logging.debug(loc_notes)
	if len(local_notes) > 0:
		found_note_count = 0
		found_note_value = 0
		for note in local_notes:
			for loc_note in loc_notes:
				logging.debug("CHECK VVVVVVVV")
				logging.debug(note)
				logging.debug(loc_note)
				score_card = 0
				score_value = 0
				for element in note:
					if element in loc_note:
						if element == '{http://www.w3.org/2000/01/rdf-schema#}label':
#							logging.debug(f"\t\t\t{note[element]}")
#							logging.debug(f"\t\t\t{loc_note[element]}")
							l_dist = calculateLevenshteinDistance(note[element],loc_note[element])
#							logging.debug(l_dist)
							if l_dist < len(note[element]) * 0.1:
								logging.debug(len(note[element]) * 0.1)
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
#		return (found_note_count / len(local_notes))
	else:
		return 0

def compareTitles(target_title,candidate_titles):
	best_fit = 0
	for candidate in candidate_titles:
		logging.debug(f"\t\t\t{target_title}")
		logging.debug(f"\t\t\t{candidate}")
		l_dist = calculateLevenshteinDistance(target_title,candidate)
		logging.debug("::::::::::::::::::::::::::::::::::::::::::::::::::::::::::")
		logging.debug(l_dist)
		normalized_value = (len(target_title) - l_dist)/len(target_title)
		if normalized_value > best_fit:
			best_fit = normalized_value
		logging.debug(normalized_value)
	return best_fit

def compareContributors(local_contributors,loc_contributors,cache_connection):
	if len(local_contributors) > 0 and len(loc_contributors) > 0:
		found_contributor_count = 0
		found_contributor_value = 0

		logging.debug("\t\tCHECK\t\t****************************")
		loc_values = []
		for loc_contributor in loc_contributors:
			loc_contributor_values = {}
			loc_type = loc_contributor.xpath("./rdf:type/@rdf:resource",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
			if len(loc_type) > 0:
				loc_contributor_values['type'] = loc_type[0]

			loc_agent_links = loc_contributor.xpath("./bf:agent/@rdf:resource",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
			logging.debug(loc_agent_links)
			if len(loc_agent_links) > 0:
#				logging.debug("\t\t\t\t\t\tQUERYING REDIS")
				res = cache_connection.get(loc_agent_links[0])
				if res:
#					logging.debug("\t\tIN CACHE:")
#					logging.debug(res)
					loc_contributor_values['agent'] = res
				else:
					agent_tree = etree.XML(getRequest(f"{loc_agent_links[0].replace('http','https')}.rdf").content)
					agent_label = agent_tree.xpath("/rdf:RDF/madsrdf:RWO/rdfs:label/text()",namespaces={"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#","rdfs": "http://www.w3.org/2000/01/rdf-schema#","madsrdf": "http://www.loc.gov/mads/rdf/v1#"})
#					logging.debug("\t\t\tSETTING CACHE:")
#					logging.debug(agent_label)
					if len(agent_label) > 0:
						loc_contributor_values['agent'] = agent_label[0]
						cache_connection.set(loc_agent_links[0],agent_label[0])
			else:
				loc_agent_list = loc_contributor.xpath("./bf:agent/bf:Agent/rdfs:label/text()",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdfs": "http://www.w3.org/2000/01/rdf-schema#"})
				if len(loc_agent_list) > 0:
					loc_contributor_values['agent'] = loc_agent_list[0]

			if len(loc_contributor_values) > 0:
				loc_values.append(loc_contributor_values)
		logging.debug(loc_values)

		for local_contributor in local_contributors:
			best_score_count = 0
			best_score_value = 0
			for val in loc_values:
				logging.debug(val)
				score_count = 0
				score_value = 0

				local_type = local_contributor.xpath("./rdf:type/@rdf:resource",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
				if len(local_type) > 0:
					if 'type' in val and local_type[0] == val['type']:
						score_count += 1
						score_value += 1

				local_agent = local_contributor.xpath("./bf:agent/bf:Agent/rdfs:label/text()",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdfs": "http://www.w3.org/2000/01/rdf-schema#"})
				if len(local_agent) > 0:
					logging.debug(local_agent[0])
					score_count += 1

					if 'agent' in val:
						logging.debug(val['agent'])

						l_dist = calculateLevenshteinDistance(local_agent[0],val['agent'])

						score_value += (len(local_agent[0]) - l_dist) / len(local_agent[0])

				if (score_count > best_score_count or score_value > best_score_value):
					best_score_count = score_count
					best_score_value = score_value

					logging.debug("updated score count")
					logging.debug(best_score_count)
					logging.debug("updated score value")
					logging.debug(best_score_value)

					if len(local_agent) > 0 and l_dist == 0:
						break

			if best_score_count != 0:
				found_contributor_count += 1
				found_contributor_value += (best_score_value / best_score_count)

		logging.debug("found contributor count")
		logging.debug(found_contributor_count)
		logging.debug("found contributor value")
		logging.debug(found_contributor_value)
		return (found_contributor_value / found_contributor_count) if found_contributor_count > 0 else 0
	else:
		return 0

def findBestMatch(matches):
	best_score = 0
	best_score_breakdown = {}
	best_url = None
	for match in matches:
		url = match
		score = sum(matches[match].values())
		if score > best_score and score > (len(matches[match])/2.0):
			best_score = score
			best_url = url
			best_score_breakdown = copy.deepcopy(matches[match])

		logging.debug(match)
		logging.debug(matches[match].values())
		logging.debug(score)

	logging.debug(best_url)
	logging.debug(best_score)
	return (best_url, best_score_breakdown, False if best_url else True)

def searchForRecordLOC(placeholder_work_id,match_fields,resource,types,output_writer,cache_connection,work_uri=None,candidate_hubs=None):
	for text_string in match_fields['titles']:
		BASE_LC_URL = 'https://id.loc.gov/search/?q='
		ENCODED_TEXT_STRING = urllib.parse.quote_plus(text_string)
		RDFTYPES = "".join([f"+rdftype:{x.rsplit('/',1)[1]}" for x in types])
		ENCODED_RESOURCE_URL = urllib.parse.quote_plus(resource)
		query_url = f"{BASE_LC_URL}{ENCODED_TEXT_STRING}{RDFTYPES}&q=cs:{ENCODED_RESOURCE_URL}"
		logging.debug(query_url)

		match_not_found = True

		try:
			results_tree = etree.HTML(getRequest(query_url).content)
			result_table = results_tree.xpath("//table[@class='id-std']/tbody/tr")
			i = 0
			matches = {}
			hubs = {}
			while i < len(result_table):
				authorized_heading = result_table[i].xpath("./td/a/text()")
				logging.debug("AUTHORIZED HEADING:")
				logging.debug(authorized_heading)
				variant_headings = result_table[i+1].xpath("./td[@colspan='5']/text()")
				logging.debug(variant_headings)
				if len(variant_headings) > 0:
					variant_headings = map(normalizeVariant,variant_headings[0].split(';'))
	#			logging.debug(variant_headings)
	#			logging.debug("MATCH KEY: %s" %(text_string,))
	#			logging.debug("AUTHORIZED HEADING: %s" %(authorized_heading[0],))
	#			logging.debug(text_string == authorized_heading[0])

#				if text_string in authorized_heading or text_string in variant_headings:
				if len(authorized_heading) > 0 or len(variant_headings) > 0:
					logging.debug("Found " + text_string)
					found_uri = 'http://id.loc.gov' + result_table[i].xpath("./td/a/@href")[0]
					logging.debug(found_uri)
					details_tree = etree.XML(getRequest(f"{found_uri.replace('http','https')}.bibframe.rdf").content)
					details_title = details_tree.xpath("/rdf:RDF/bf:Work/bf:title/bf:Title/bf:mainTitle/text()",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
					details_variant_title = details_tree.xpath("/rdf:RDF/bf:Work/bf:title/bf:VariantTitle/bf:mainTitle/text()",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})

					if len(authorized_heading) > 0 or len(variant_headings) > 0 or len(details_title) > 0 or len(details_variant_title):
						logging.debug("TITLES>>>>>>")
						logging.debug(authorized_heading + variant_headings)
						matches[found_uri] = { 'title': compareTitles(text_string,authorized_heading + variant_headings + details_title + details_variant_title) }

					logging.debug("\t\t\t\t????????????????????????????????????????????????????????")
					logging.debug(match_fields)
					if len(match_fields['languages']) > 0:
						record_languages = details_tree.xpath("/rdf:RDF/bf:Work/bf:language/@rdf:resource",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
						logging.debug('LANGUAGELANGUAGELANGUAGELANGUAGELANGUAGELANGUAGELANGUAGELANGUAGELANGUAGELANGUAGE')
						language_match_count = 0
						for lang in match_fields['languages']:
							if lang in record_languages:
								language_match_count += 1
						
						matches[found_uri]['languages'] = language_match_count / len(match_fields['languages'])

					if len(match_fields['contributors']) > 0:
						logging.debug("\t\tCONTRIBUTORSCONTRIBUTORSCONTRIBUTORSCONTRIBUTORSCONTRIBUTORSCONTRIBUTORSCONTRIBUTORSCONTRIBUTORS")
#						logging.debug(match_fields['contributors'])
						record_contributors = details_tree.xpath("/rdf:RDF/bf:Work/bf:contribution/bf:Contribution", namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
#						logging.debug(record_contributors)
						matches[found_uri]['contributors'] = compareContributors(match_fields['contributors'],record_contributors,cache_connection)
						logging.debug(matches)

					if len(match_fields['notes']) > 0:
	#					logging.debug(details_tree)
						record_notes = details_tree.xpath("/rdf:RDF/bf:Work/bf:note/bf:Note",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
	#					logging.debug("NOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTES")
	#					logging.debug(notes)
						notes_a = getNotes(match_fields['notes'])
	#					logging.debug("RECORD NOTES")
	#					logging.debug(record_notes)
						notes_b = getNotes(record_notes)
	#					logging.debug("UNPACKAGEDUNPACKAGEDUNPACKAGEDUNPACKAGEDUNPACKAGED")
						logging.debug("HEREHEREHEREHEREHEREHEREHEREHEREHEREHEREHEREHEREHEREHEREHEREHERE")
						logging.debug(notes_a)
						logging.debug(notes_b)
						logging.debug(compareNotes(notes_a,notes_b))
						matches[found_uri]['notes'] = compareNotes(notes_a,notes_b)
						logging.debug(matches)
						logging.debug("THERETHERETHERETHERETHERETHERETHERETHERETHERETHERETHERETHERETHERE")

					if 'hubs' in resource:
						if candidate_hubs and found_uri in candidate_hubs:
							matches[found_uri]['hub'] = 1
						else:
							record_works = details_tree.xpath("/rdf:RDF/bf:Work/bf:hasExpression/@rdf:resource",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
							logging.debug("hasEXPRESSION--hasEXPRESSION--hasEXPRESSION--hasEXPRESSION--hasEXPRESSION--hasEXPRESSION--hasEXPRESSION--hasEXPRESSION--")
							logging.debug(record_works)
							if work_uri in record_works:
								matches[found_uri]['hub'] = 1
					else:
						record_hubs = details_tree.xpath("/rdf:RDF/bf:Work/bf:expressionOf/@rdf:resource",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
						hubs[found_uri] = record_hubs
#					logging.debug(f"{placeholder_work_id}, {text_string}, {query_url}, {found_uri}")
#					output_writer.writerow([placeholder_work_id,text_string,query_url,found_uri])
#					match_not_found = False

				i = i + 2

			logging.debug(matches)
			selected_url, selected_breakdown, match_not_found = findBestMatch(matches)
			logging.debug(selected_url)
			logging.debug(match_not_found)
			if selected_url:
				logging.debug(f"{placeholder_work_id}, {text_string}, {query_url}, {json.dumps(selected_breakdown)}, {selected_url}")
				output_writer.writerow([placeholder_work_id,text_string,query_url,json.dumps(selected_breakdown),selected_url])
				if 'hubs' in resource:
					return selected_url, None
				else:
					return selected_url, hubs[selected_url] if len(hubs[selected_url]) > 0 else None
		except Exception as e:
			logging.debug(e)
			output_writer.writerow([placeholder_work_id,text_string,query_url,"QUERY ERROR","QUERY ERROR"])
			return None, None

		if match_not_found:
			logging.debug(f"{placeholder_work_id}, {text_string}, {query_url},")
			output_writer.writerow([placeholder_work_id,text_string,query_url])
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
#		if marc_contributor['a'] not in contributor_cache:
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
#							cache_connection.set(f"{marc_contributor['a']}:{w['works']['value']}",w['worksLabel']['value'])

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
	else:
		logging.debug(f"{placeholder_work_id}, {json.dumps(match_fields)}")
		output_writer.writerow([placeholder_work_id,json.dumps(match_fields)])

def clearBlankText(text_array):
	return " ".join([x for x in text_array if x.strip() != ''])

def reconcileWorks(args):
	if not args.input.endswith('.xml'):
		raise Exception("Input file must be an XML file")

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
	
	tree = etree.parse(args.input)
	root = tree.getroot()

	works = root.xpath('/rdf:RDF/bf:Work', namespaces={ "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "bf": "http://id.loc.gov/ontologies/bibframe/" })
	logging.debug(len(works))
	logging.debug(works)

	with open(f"{args.output}{SLASH}{args.input.rsplit('/',1)[1][:-4]}_{args.source}.tsv",'w') as outfile:
		writer = csv.writer(outfile,delimiter='\t')
		for work in works:
			placeholder_work_id = work.xpath("./@rdf:about", namespaces={ "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#" })[0]
			logging.debug(placeholder_work_id)
			work_title = work.xpath("./bf:title/bf:Title//text()", namespaces={ "bf": "http://id.loc.gov/ontologies/bibframe/" })
			work_title_text = clearBlankText(work_title)
			work_types = work.xpath("./rdf:type/@rdf:resource", namespaces={ "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#" })
			logging.debug(work_types)

			variant_titles = work.xpath("./bf:title/bf:VariantTitle//text()", namespaces={ "bf": "http://id.loc.gov/ontologies/bibframe/" })
			variant_titles_text = clearBlankText(variant_titles)

			contributors = work.xpath("./bf:contribution/bf:Contribution", namespaces={ "bf": "http://id.loc.gov/ontologies/bibframe/" })

			search_titles = [work_title_text]
			if variant_titles_text:
				search_titles.append(variant_titles_text)

			if args.source == Sources.loc:
				notes = work.xpath("./bf:note/bf:Note", namespaces={ "bf": "http://id.loc.gov/ontologies/bibframe/" })

				languages = work.xpath("./bf:language/@rdf:resource", namespaces={ "bf": "http://id.loc.gov/ontologies/bibframe/", "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#" })

				match_fields = { 'titles': search_titles, 'notes': notes, 'languages': languages, 'contributors': contributors }
				
				found_work_uri, found_work_associated_hubs = searchForRecordLOC(placeholder_work_id,match_fields,'http://id.loc.gov/resources/works',work_types,writer,loc_cache_connection)
				searchForRecordLOC(placeholder_work_id,match_fields,'http://id.loc.gov/resources/hubs',['http://id.loc.gov/ontologies/bibframe/Work','http://id.loc.gov/ontologies/bibframe/Hub'],writer,loc_cache_connection,found_work_uri,found_work_associated_hubs)
#					instances = root.xpath("/rdf:RDF/bf:Instance[bf:instanceOf/@rdf:resource='" + placeholder_work_id + "']", namespaces={ "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "bf": "http://id.loc.gov/ontologies/bibframe/" })
#					logging.debug(instances)
			elif args.source == Sources.wikidata:
				match_fields = { 'titles': search_titles, 'contributors': contributors }
				marc_keys = []
				found_primary = False
				for contributor in match_fields['contributors']:
					contributor_labels = contributor.xpath("./bf:agent/bf:Agent/bflc:marcKey/text()",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","bflc": "http://id.loc.gov/ontologies/bflc/"})
					logging.debug(contributor_labels)
					contributor_types = contributor.xpath("./rdf:type/@rdf:resource",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
					if 'http://id.loc.gov/ontologies/bibframe/PrimaryContribution' in contributor_types:
						match_fields['contributors'] = contributor_labels
						found_primary = True
						break
					else:
						marc_keys += contributor_labels

				if not found_primary:
					match_fields['contributors'] = marc_keys

				searchForRecordWiki(placeholder_work_id,match_fields,wiki_cache_connection,writer)

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("input", help="BIBFRAME XML file to process")
	parser.add_argument("output", help="Directory to write the output to")
	parser.add_argument("source", type=Sources, choices=list(Sources), help="Run queries on LOC or Wikidata")
	args = parser.parse_args()

	reconcileWorks(args)