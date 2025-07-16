import argparse, sys, os, logging, requests, csv, urllib.parse
from lxml import etree

logging.basicConfig(level=logging.DEBUG,format='%(asctime)s [%(levelname)s] (%(threadName)-10s) %(message)s',datefmt='%H:%M:%S')

class BrokenResponse:
	status_code = '400'

if os.name == 'nt':
	SLASH = '\\'
else:
	SLASH = '/'

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

def searchForRecord(placeholder_work_id,titles,resource,types,output_writer):
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
			while i < len(result_table) and match_not_found:
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
					logging.debug(f"{placeholder_work_id}, {text_string}, {query_url}, {found_uri}")
					output_writer.writerow([placeholder_work_id,text_string,query_url,found_uri])
					match_not_found = False

				i = i + 2
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

			search_titles = [work_title_text]
			if variant_titles_text:
				search_titles.append(variant_titles_text)
			
			searchForRecord(placeholder_work_id,search_titles,'http://id.loc.gov/resources/works',work_types,writer)
			searchForRecord(placeholder_work_id,search_titles,'http://id.loc.gov/resources/hubs',work_types,writer)
			instances = root.xpath("/rdf:RDF/bf:Instance[bf:instanceOf/@rdf:resource='" + placeholder_work_id + "']", namespaces={ "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#", "bf": "http://id.loc.gov/ontologies/bibframe/" })
			logging.debug(instances)

if __name__ == '__main__':
	parser = argparse.ArgumentParser()
	parser.add_argument("input", help="BIBFRAME XML file to process")
	parser.add_argument("output", help="Directory to write the output to")
	args = parser.parse_args()

	reconcileWorks(args)