package eu.ldbc.semanticpublishing.mongo;

import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import eu.ldbc.semanticpublishing.properties.Configuration;
import org.bson.Document;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.rio.RDFWriter;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.WriterConfig;
import org.eclipse.rdf4j.rio.helpers.JSONLDMode;
import org.eclipse.rdf4j.rio.helpers.JSONLDSettings;
import org.eclipse.rdf4j.rio.jsonld.JSONLDWriter;

import java.io.StringWriter;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class Utils {

	private static WriterConfig writerConfig;
	private static DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'");

	static {
		writerConfig = new WriterConfig();
		writerConfig.set(JSONLDSettings.HIERARCHICAL_VIEW, true);
		writerConfig.set(JSONLDSettings.JSONLD_MODE, JSONLDMode.COMPACT);
	}

	public static MongoCollection<Document> getCollection(Configuration configuration) {
		MongoClient mongoClient = new MongoClient(configuration.getString(Configuration.MONGODB_HOST)
				, configuration.getInt(Configuration.MONGODB_PORT));
		MongoDatabase database = mongoClient.getDatabase(configuration.getString(Configuration.MONGODB_DATABASE));
		return database.getCollection(configuration.getString(Configuration.MONGODB_COLLECTION)).withWriteConcern(WriteConcern.JOURNALED);
	}

	public static Document modelToJsonLd(Model model) {
		StringWriter out = new StringWriter();

		RDFWriter writer = new JSONLDWriter(out);
		writer.setWriterConfig(writerConfig);

		writer.handleNamespace("bbcevent", "http://www.bbc.co.uk/ontologies/event/");
		writer.handleNamespace("geo-pos", "http://www.w3.org/2003/01/geo/wgs84_pos#");
		writer.handleNamespace("bbc", "http://www.bbc.co.uk/ontologies/bbc/");
		writer.handleNamespace("time", "http://www.w3.org/2006/time#");
		writer.handleNamespace("event", "http://purl.org/NET/c4dm/event.owl#");
		writer.handleNamespace("music-ont", "http://purl.org/ontology/mo/");
		writer.handleNamespace("rdf", "http://www.w3.org/1999/02/22-rdf-syntax-ns#");
		writer.handleNamespace("foaf", "http://xmlns.com/foaf/0.1/");
		writer.handleNamespace("provenance", "http://www.bbc.co.uk/ontologies/provenance/");
		writer.handleNamespace("owl", "http://www.w3.org/2002/07/owl#");
		writer.handleNamespace("cms", "http://www.bbc.co.uk/ontologies/cms/");
		writer.handleNamespace("news", "http://www.bbc.co.uk/ontologies/news/");
		writer.handleNamespace("cnews", "http://www.bbc.co.uk/ontologies/news/cnews/");
		writer.handleNamespace("cconcepts", "http://www.bbc.co.uk/ontologies/coreconcepts/");
		writer.handleNamespace("dbp-prop", "http://dbpedia.org/property/");
		writer.handleNamespace("geonames", "http://sws.geonames.org/");
		writer.handleNamespace("rdfs", "http://www.w3.org/2000/01/rdf-schema#");
		writer.handleNamespace("domain", "http://www.bbc.co.uk/ontologies/domain/");
		writer.handleNamespace("dbpedia", "http://dbpedia.org/resource/");
		writer.handleNamespace("geo-ont", "http://www.geonames.org/ontology#");
		writer.handleNamespace("bbc-pont", "http://purl.org/ontology/po/");
		writer.handleNamespace("tagging", "http://www.bbc.co.uk/ontologies/tagging/");
		writer.handleNamespace("sport", "http://www.bbc.co.uk/ontologies/sport/");
		writer.handleNamespace("skosCore", "http://www.w3.org/2004/02/skos/core#");
		writer.handleNamespace("dbp-ont", "http://dbpedia.org/ontology/");
		writer.handleNamespace("xsd", "http://www.w3.org/2001/XMLSchema#");
		writer.handleNamespace("core", "http://www.bbc.co.uk/ontologies/coreconcepts/");
		writer.handleNamespace("curric", "http://www.bbc.co.uk/ontologies/curriculum/");
		writer.handleNamespace("skos", "http://www.w3.org/2004/02/skos/core#");
		writer.handleNamespace("cwork", "http://www.bbc.co.uk/ontologies/creativework/");
		writer.handleNamespace("fb", "http://rdf.freebase.com/ns/");
		writer.handleNamespace("ot", "http://www.ontotext.com/");
		writer.handleNamespace("ldbcspb", "http://www.ldbcouncil.org/spb#");
		writer.handleNamespace("ldbcspb", "http://www.ldbcouncil.org/spb#");
		writer.handleNamespace("bbcd", "http://www.bbc.co.uk/document/");
		writer.handleNamespace("bbcc", "http://www.bbc.co.uk/context/");
		writer.handleNamespace("bbct", "http://www.bbc.co.uk/thumbnail/");

		Rio.write(model, writer);

		out.flush();
		Document doc = Document.parse(out.toString());

		try {
			Document dc = ((Document) doc.get("@graph", List.class).get(0)).get("cwork:dateCreated", Document.class);
			Document dm = ((Document) doc.get("@graph", List.class).get(0)).get("cwork:dateModified", Document.class);

			instrumentDate(dc);
			instrumentDate(dm);
		} catch (Exception e) {
			e.printStackTrace();
		}

		return doc;
	}

	private static synchronized void instrumentDate(Document doc) throws ParseException {
		Date date = dateFormat.parse(doc.getString("@value"));
		doc.append("@date", date);
	}

}
