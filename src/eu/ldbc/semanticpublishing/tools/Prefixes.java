package eu.ldbc.semanticpublishing.tools;

import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.Map;

public class Prefixes {

	private static Map<String, String> prefixes = new HashMap<>();

	static {
		prefixes.put("http://www.bbc.co.uk/ontologies/event/", "bbcevent");
		prefixes.put("http://www.w3.org/2003/01/geo/wgs84_pos#", "geo-pos");
		prefixes.put("http://www.bbc.co.uk/ontologies/bbc/", "bbc");
		prefixes.put("http://www.w3.org/2006/time#", "time");
		prefixes.put("http://purl.org/NET/c4dm/event.owl#", "event");
		prefixes.put("http://purl.org/ontology/mo/", "music-ont");
		prefixes.put("http://www.w3.org/1999/02/22-rdf-syntax-ns#", "rdf");
		prefixes.put("http://xmlns.com/foaf/0.1/", "foaf");
		prefixes.put("http://www.bbc.co.uk/ontologies/provenance/", "provenance");
		prefixes.put("http://www.w3.org/2002/07/owl#", "owl");
		prefixes.put("http://www.bbc.co.uk/ontologies/cms/", "cms");
		prefixes.put("http://www.bbc.co.uk/ontologies/news/", "news");
		prefixes.put("http://www.bbc.co.uk/ontologies/news/cnews/", "cnews");
		prefixes.put("http://www.bbc.co.uk/ontologies/coreconcepts/", "cconcepts");
		prefixes.put("http://dbpedia.org/property/", "dbp-prop");
		prefixes.put("http://sws.geonames.org/", "geonames");
		prefixes.put("http://www.w3.org/2000/01/rdf-schema#", "rdfs");
		prefixes.put("http://www.bbc.co.uk/ontologies/domain/", "domain");
		prefixes.put("http://dbpedia.org/resource/", "dbpedia");
		prefixes.put("http://www.geonames.org/ontology#", "geo-ont");
		prefixes.put("http://purl.org/ontology/po/", "bbc-pont");
		prefixes.put("http://www.bbc.co.uk/ontologies/tagging/", "tagging");
		prefixes.put("http://www.bbc.co.uk/ontologies/sport/", "sport");
		prefixes.put("http://www.w3.org/2004/02/skos/core#", "skosCore");
		prefixes.put("http://dbpedia.org/ontology/", "dbp-ont");
		prefixes.put("http://www.w3.org/2001/XMLSchema#", "xsd");
		prefixes.put("http://www.bbc.co.uk/ontologies/curriculum/", "curric");
		prefixes.put("http://www.bbc.co.uk/ontologies/creativework/", "cwork");
		prefixes.put("http://rdf.freebase.com/ns/", "fb");
		prefixes.put("http://www.ontotext.com/", "ot");
		prefixes.put("http://www.ldbcouncil.org/spb#", "ldbcspb");
	}

	public static String compact(String uri) {
		String trimmed = uri.replace("<", "").replace(">", "");
		IRI iri = SimpleValueFactory.getInstance().createIRI(trimmed);

		String namespace = iri.getNamespace();
		if (prefixes.containsKey(namespace)) {
			return prefixes.get(namespace) + ":" + URLEncoder.encode(iri.getLocalName());
		}

		return uri;
	}

	public static void main(String... args) {
		System.out.println(compact("<http://dbpedia.org/resource/Mexican–American_War>"));
	}
}
