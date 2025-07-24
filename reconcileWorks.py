import argparse, sys, os, logging, requests, csv, urllib.parse
from lxml import etree

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s',datefmt='%H:%M:%S')

class BrokenResponse:
	status_code = '400'

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

def findBestMatch(matches):
	best_score = 0
	best_url = None
	for match in matches:
		url = match
		score = sum(matches[match].values())
		if score > best_score:
			best_score = score
			best_url = url

		logging.debug(match)
		logging.debug(matches[match].values())
		logging.debug(score)

	logging.debug(best_url)
	logging.debug(best_score)
	return (best_url, False if best_url else True)

def searchForRecord(placeholder_work_id,titles,resource,notes,types,output_writer):
	for text_string in titles:
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
					if len(authorized_heading) > 0 or len(variant_headings) > 0:
						logging.debug("TITLES>>>>>>")
						logging.debug(authorized_heading + variant_headings)
						matches[found_uri] = { 'title': compareTitles(text_string,authorized_heading + variant_headings) }
#					if text_string in authorized_heading or text_string in variant_headings:
#						matches[found_uri] = { 'title': 1 }
#					else:
#						matches[found_uri] = { 'title': 0 }

					if notes:
						details_tree = etree.XML(getRequest(f"{found_uri}.bibframe.rdf").content)
	#					logging.debug(details_tree)
						record_notes = details_tree.xpath("/rdf:RDF/bf:Work/bf:note/bf:Note",namespaces={"bf": "http://id.loc.gov/ontologies/bibframe/","rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#"})
	#					logging.debug("NOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTESNOTES")
	#					logging.debug(notes)
						notes_a = getNotes(notes)
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

#					logging.debug(f"{placeholder_work_id}, {text_string}, {query_url}, {found_uri}")
#					output_writer.writerow([placeholder_work_id,text_string,query_url,found_uri])
#					match_not_found = False

				i = i + 2

			logging.debug(matches)
			selected_url, match_not_found = findBestMatch(matches)
			logging.debug(selected_url)
			logging.debug(match_not_found)
			if selected_url:
				logging.debug(f"{placeholder_work_id}, {text_string}, {query_url}, {selected_url}")
				output_writer.writerow([placeholder_work_id,text_string,query_url,selected_url])
		except Exception as e:
			logging.debug(e)

		if match_not_found:
			logging.debug(f"{placeholder_work_id}, {text_string}, {query_url},")
			output_writer.writerow([placeholder_work_id,text_string,query_url])

def clearBlankText(text_array):
	return " ".join([x for x in text_array if x.strip() != ''])

def reconcileWorks(args):
	if not args.input.endswith('.xml'):
		raise Exception("Input file must be an XML file")

	os.makedirs(args.output,exist_ok=True)

	tree = etree.parse(args.input)
	root = tree.getroot()

	works = root.xpath('/rdf:RDF/bf:Work', namespaces={ "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "bf": "http://id.loc.gov/ontologies/bibframe/" })
	logging.debug(len(works))
	logging.debug(works)

	with open(f"{args.output}{SLASH}{args.input.rsplit('/',1)[1][:-4]}.tsv",'w') as outfile:
		writer = csv.writer(outfile,delimiter='\t')
		for work in works:
			placeholder_work_id = work.xpath("./@rdf:about", namespaces={ "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#" })[0]
			logging.debug(placeholder_work_id)
			work_title = work.xpath("./bf:title/bf:Title//text()", namespaces={ "bf": "http://id.loc.gov/ontologies/bibframe/" })
			work_title_text = clearBlankText(work_title)
			work_types = work.xpath("./rdf:type/@rdf:resource", namespaces={ "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#" })

			variant_titles = work.xpath("./bf:title/bf:VariantTitle//text()", namespaces={ "bf": "http://id.loc.gov/ontologies/bibframe/" })
			variant_titles_text = clearBlankText(variant_titles)

			notes = work.xpath("./bf:note/bf:Note", namespaces={ "bf": "http://id.loc.gov/ontologies/bibframe/" })

			search_titles = [work_title_text]
			if variant_titles_text:
				search_titles.append(variant_titles_text)
			
			searchForRecord(placeholder_work_id,search_titles,'http://id.loc.gov/resources/works',notes,work_types,writer)
			searchForRecord(placeholder_work_id,search_titles,'http://id.loc.gov/resources/hubs',notes,work_types,writer)
			instances = root.xpath("/rdf:RDF/bf:Instance[bf:instanceOf/@rdf:resource='" + placeholder_work_id + "']", namespaces={ "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "bf": "http://id.loc.gov/ontologies/bibframe/" })
			logging.debug(instances)

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("input", help="BIBFRAME XML file to process")
	parser.add_argument("output", help="Directory to write the output to")
	args = parser.parse_args()

	reconcileWorks(args)