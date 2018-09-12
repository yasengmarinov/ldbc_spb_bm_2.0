package eu.ldbc.semanticpublishing.refdataset;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import eu.ldbc.semanticpublishing.properties.Configuration;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.JSONLDMode;
import org.eclipse.rdf4j.rio.helpers.JSONLDSettings;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;

public class InsertCreativeWorksInMongoDB {

	private long timeDocsConversion;
	private long timeInsertsInMongoDB;

	private static MongoCollection<Document> collection;

	private static WriterConfig writerConfig;
	private static List<String> convertedDocs;

	private static Logger LOGGER = LoggerFactory.getLogger(InsertCreativeWorksInMongoDB.class.getName());

	public static void main (String[] args) {
		new InsertCreativeWorksInMongoDB(new Configuration());
	}

	public InsertCreativeWorksInMongoDB(Configuration configuration) {

		File file = new File(configuration.getString(Configuration.CREATIVE_WORKS_PATH));
		MongoClient mongoClient = null;

		try {
			convertedDocs = new ArrayList<String>(100000);
			writerConfig = new WriterConfig();
			writerConfig.set(JSONLDSettings.HIERARCHICAL_VIEW, true);
			writerConfig.set(JSONLDSettings.JSONLD_MODE, JSONLDMode.COMPACT);

			long startProcess = System.currentTimeMillis();
			mongoClient = new MongoClient(configuration.getString(Configuration.MONGODB_HOST)
					, configuration.getInt(Configuration.MONGODB_PORT));
			MongoDatabase database = mongoClient.getDatabase(configuration.getString(Configuration.MONGODB_DATABASE));
			collection = database.getCollection(configuration.getString(Configuration.MONGODB_COLLECTION));
			collection.drop();

			generateJSONLDStrings(file);
			loadDataInMongo();

			LOGGER.info("Files converted to JSON-LD in " + timeDocsConversion + " ms!");
			LOGGER.info("Documents inserted in MongoDB for " + timeInsertsInMongoDB + " ms!");
			LOGGER.info("Whole process took " + (System.currentTimeMillis() - startProcess) + " ms!");

			// Dates in documents should taken and converted from strings to Date objects
			// which MongoDB converts internal in ISODate objects
			fixMongoDates();
		} catch (Exception e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			mongoClient.close();
		}
	}

	private void fixMongoDates() {
		FindIterable<Document> documents = collection.find();
		LOGGER.info("Fixing dates in MongoDB...");

		int count = 0;

		for (Document doc : documents) {
			ObjectId id = doc.getObjectId("_id");
			List<Map<String,Object>> graph = (List<Map<String, Object>>) doc.get("@graph");
			Document created = (Document) graph.get(0).get("cwork:dateCreated");
			Document modified = (Document) graph.get(0).get("cwork:dateModified");

			Bson newCreatedValue = new Document("@graph.0.cwork:dateCreated.@date", convertDate(created.getString("@value")));
			Bson updateCreatedDocument = new Document("$set", newCreatedValue);
			collection.updateOne(new Document("_id", id), updateCreatedDocument);

			Bson newModifiedValue = new Document("@graph.0.cwork:dateModified.@date", convertDate(modified.getString("@value")));
			Bson updateModifiedDocument = new Document("$set", newModifiedValue);
			collection.updateOne(new Document("_id", id), updateModifiedDocument);

			if (++count % 10000 == 0) {
				LOGGER.info("Processed docs: {}", count);
			}
		}

		LOGGER.info("Total processed docs: {}", count);
	}

	private Date convertDate(String dateString) {
		OffsetDateTime odt = OffsetDateTime.parse( dateString );
		Instant instant = odt.toInstant();
		return Date.from(instant);
	}

	private void generateJSONLDStrings(File file) throws Exception {
		if (file.isDirectory()) {
			final File[] files = file.listFiles();
			assert files != null;
			Arrays.sort(files);
			for (File fileInDirectory : files) {
				generateJSONLDStrings(fileInDirectory);
			}

			return;
		}

		InputStream in = null;
		try {
			in = markSupported(new FileInputStream(file));
			RDFFormat dataFormat;
			Optional<RDFFormat> detectedFormat = Rio.getParserFormatForFileName(file.getName());

			if (detectedFormat.isPresent()) {
				dataFormat = detectedFormat.get();
			} else {
				LOGGER.error("Could not find RDF format for file: {}", file.getName());
				return;
			}

			getPrettyJsonLdString(in, dataFormat);
		} finally {
			if (in != null) {
				in.close();
			}
		}
	}

	private InputStream markSupported(InputStream in) {
		if (!in.markSupported()) {
			in = new BufferedInputStream(in, 1024);
		}
		return in;
	}

	private void loadDataInMongo() {
		LOGGER.info("Total docs in MongoDB before load: {}", collection.countDocuments());
		long start = System.currentTimeMillis();
		List<Document> batch = new LinkedList<Document>();

		for (String currDoc : convertedDocs) {
			batch.add(Document.parse(currDoc));
			if (batch.size() == 100000) {
				collection.insertMany(batch);
				batch.clear();
				LOGGER.info("Total docs in MongoDB after load: {}", collection.countDocuments());
			}
		}

		collection.insertMany(batch);
		timeInsertsInMongoDB += System.currentTimeMillis() - start;
		LOGGER.info("Total docs in MongoDB after load: {}", collection.countDocuments());
	}

	public void getPrettyJsonLdString(InputStream in, RDFFormat format) {
		long start = System.currentTimeMillis();
		List<String> generated = readRdfToString(in, format, RDFFormat.JSONLD, "");
		for (String currJSONLD : generated) {
			convertedDocs.add(currJSONLD);
		}
		timeDocsConversion += System.currentTimeMillis() - start;
		if (convertedDocs.size() >= 98000) {
			LOGGER.info("Starting current load in MongoDB");
			loadDataInMongo();
			convertedDocs.clear();
		}
	}

	private Collection<Statement> collectRDFStatements(
			final InputStream inputStream, final RDFFormat inf,
			final String baseUrl) {
		try {
			final RDFParser rdfParser = Rio.createParser(inf);
			final StatementCollector collector = new StatementCollector();
			rdfParser.setRDFHandler(collector);
			rdfParser.parse(inputStream, baseUrl);
			collector.getNamespaces();
			return collector.getStatements();
		} catch (final Exception e) {
			LOGGER.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	private List<String> readRdfToString(InputStream in, RDFFormat inf,
										 RDFFormat outf, String baseUrl) {
		Collection<Statement> myGraph = null;
		myGraph = collectRDFStatements(in, inf, baseUrl);
		return graphToString(myGraph, outf);
	}

	private List<String> graphToString(Collection<Statement> myGraph,
									   RDFFormat outf) {
		StringWriter out = new StringWriter();
		RDFWriter writer = null;
		Set<String> uniqueSubjectsList = new HashSet<String>();
		List<String> currList = new ArrayList<String>(3000);
		Statement prevSt = null;

		try {
			for (Statement st : myGraph) {
				String subjectStringValue = st.getSubject().stringValue();
				if (!uniqueSubjectsList.add(subjectStringValue)
						|| (prevSt != null && prevSt.toString().indexOf(subjectStringValue) != -1)) {
					writer.handleStatement(st);
					prevSt = st;
				} else {

					if (prevSt != null && !prevSt.getSubject().stringValue().equals(subjectStringValue)) {
						writer.endRDF();
						currList.add(out.getBuffer().toString());
					}

					out = new StringWriter();
					writer = Rio.createWriter(outf, out);
					writer.setWriterConfig(writerConfig);
					writer.handleNamespace("bbcevent", "http://www.bbc.co.uk/ontologies/event/");
					writer.handleNamespace("bbcwebDocumentType", "http://www.bbc.co.uk/ontologies/bbc/webDocumentType");
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


					writer.startRDF();
					writer.handleStatement(st);
					prevSt = st;
				}
			}

			if (writer != null) {
				writer.endRDF();
				currList.add(out.getBuffer().toString());
			}
		} catch (RDFHandlerException e) {
			LOGGER.error(e.getMessage());
			throw new RuntimeException(e);
		}

		return currList;
	}
}
