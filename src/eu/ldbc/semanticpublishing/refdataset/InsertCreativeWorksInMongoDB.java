package eu.ldbc.semanticpublishing.refdataset;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import eu.ldbc.semanticpublishing.properties.Configuration;
import org.apache.commons.io.input.BOMInputStream;
import org.bson.Document;
import org.bson.conversions.Bson;
import org.bson.types.ObjectId;
import org.eclipse.rdf4j.common.io.GZipUtil;
import org.eclipse.rdf4j.common.io.UncloseableInputStream;
import org.eclipse.rdf4j.common.io.ZipUtil;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.*;
import org.eclipse.rdf4j.rio.helpers.JSONLDMode;
import org.eclipse.rdf4j.rio.helpers.JSONLDSettings;
import org.eclipse.rdf4j.rio.helpers.StatementCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;

public class InsertCreativeWorksInMongoDB {

	private long timeDocsConversion;
	private long timeInsertsInMongoDB;
	private int BATCH_SIZE;

	private MongoCollection<Document> collection;

	private WriterConfig writerConfig;
	private List<String> convertedDocs;
	protected DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSX");

	private static Logger LOGGER = LoggerFactory.getLogger(InsertCreativeWorksInMongoDB.class.getName());

	public static void main(String[] args) {
		new InsertCreativeWorksInMongoDB(new Configuration());
	}

	public InsertCreativeWorksInMongoDB(Configuration configuration) {

		BATCH_SIZE = configuration.getInt(Configuration.MONGODB_BATCH_SIZE);
		File file = new File(configuration.getString(Configuration.CREATIVE_WORKS_PATH));
		MongoClient mongoClient = null;

		try {
			convertedDocs = new ArrayList<>(BATCH_SIZE);
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

	private void fixMongoDates() throws ParseException {
		FindIterable<Document> documents = collection.find();
		LOGGER.info("Fixing dates in MongoDB...");

		int count = 0;

		for (Document doc : documents) {
			ObjectId id = doc.getObjectId("_id");
			List<Map<String, Object>> graph = (List<Map<String, Object>>) doc.get("@graph");
			Document created = (Document) graph.get(0).get("cwork:dateCreated");
			Document modified = (Document) graph.get(0).get("cwork:dateModified");

			Bson newCreatedValue = new Document("@graph.0.cwork:dateCreated.@date", dateFormat.parse(created.getString("@value")));
			Bson updateCreatedDocument = new Document("$set", newCreatedValue);
			collection.updateOne(new Document("_id", id), updateCreatedDocument);

			Bson newModifiedValue = new Document("@graph.0.cwork:dateModified.@date", dateFormat.parse(modified.getString("@value")));
			Bson updateModifiedDocument = new Document("$set", newModifiedValue);
			collection.updateOne(new Document("_id", id), updateModifiedDocument);

			if (++count % 10000 == 0) {
				LOGGER.info("Processed docs: {}", count);
			}
		}

		LOGGER.info("Total processed docs: {}", count);
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

		try (InputStream in = markSupported(new FileInputStream(file))) {
			RDFFormat dataFormat;
			Optional<RDFFormat> detectedFormat = Rio.getParserFormatForFileName(file.getName());

			if (detectedFormat.isPresent()) {
				dataFormat = detectedFormat.get();
			} else {
				return;
			}

			// If this isn't a compressed stream
			if (!ZipUtil.isZipStream(in) && !GZipUtil.isGZipStream(in)) {
				convertAndLoadInMongoIfNeeded(in, dataFormat);
			} else {
				loadZipOrGZip(in, dataFormat);
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
		long start = System.currentTimeMillis();
		List<Document> batch = new LinkedList<Document>();

		for (String currDoc : convertedDocs) {
			batch.add(Document.parse(currDoc));
			if (batch.size() == BATCH_SIZE) {
				collection.insertMany(batch);
				batch.clear();
				LOGGER.info("Total docs in MongoDB {}", collection.countDocuments());
			}
		}

		if (batch.size() != 0) {
			collection.insertMany(batch);
			LOGGER.info("Total docs in MongoDB: {}", collection.countDocuments());
		}

		timeInsertsInMongoDB += System.currentTimeMillis() - start;
	}

	public void convertAndLoadInMongoIfNeeded(Object in, RDFFormat format) {
		long start = System.currentTimeMillis();
		List<String> generated = readRdfToString(in, format, RDFFormat.JSONLD, "");
		for (String currJSONLD : generated) {
			convertedDocs.add(currJSONLD);
			// In case we ready documents reach initial capacity of the
			// ArrayList we insert them into MongoDB to avoid resizing
			if (convertedDocs.size() == BATCH_SIZE) {
				loadDataInMongo();
				convertedDocs.clear();
			}
		}
		timeDocsConversion += System.currentTimeMillis() - start;
	}

	private Collection<Statement> collectRDFStatements(
			final Object inputStreamOrReader, final RDFFormat inf,
			final String baseUrl) {
		try {
			final RDFParser rdfParser = Rio.createParser(inf);
			final StatementCollector collector = new StatementCollector();
			rdfParser.setRDFHandler(collector);
			if (inputStreamOrReader instanceof InputStream) {
				rdfParser.parse((InputStream) inputStreamOrReader, baseUrl);
			} else {
				if (!(inputStreamOrReader instanceof Reader)) {
					throw new IllegalArgumentException("Must be an InputStream or a Reader, is a: " + inputStreamOrReader.getClass());
				}

				rdfParser.parse((Reader) inputStreamOrReader, baseUrl);
			}
			collector.getNamespaces();
			return collector.getStatements();
		} catch (final Exception e) {
			LOGGER.error(e.getMessage());
			throw new RuntimeException(e);
		}
	}

	private List<String> readRdfToString(Object in, RDFFormat inf,
										 RDFFormat outf, String baseUrl) {
		Collection<Statement> myGraph;
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

	private void loadZipOrGZip(InputStream in, RDFFormat dataFormat) throws IOException {
		if (!in.markSupported()) {
			in = new BufferedInputStream(in, 1024);
		}
		if (ZipUtil.isZipStream(in)) {
			loadZip(in, dataFormat);
		} else if (GZipUtil.isGZipStream(in)) {
			createReaderAndConvertToJsonLD((new GZIPInputStream(in)), dataFormat);
		} else {
			convertAndLoadInMongoIfNeeded(in, dataFormat);
		}
	}

	private void loadZip(InputStream in, RDFFormat dataFormat) {
		ZipInputStream zipIn = new ZipInputStream(in);

		try {
			for (ZipEntry entry = zipIn.getNextEntry(); entry != null; entry = zipIn.getNextEntry()) {
				if (!entry.isDirectory()) {
					try {
						RDFFormat format = Rio.getParserFormatForFileName(entry.getName()).orElse(dataFormat);
						UncloseableInputStream wrapper = new UncloseableInputStream(zipIn);
						loadZipOrGZip(wrapper, format);
					} finally {
						zipIn.closeEntry();
					}
				}
			}
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		} finally {
			if (zipIn != null) {
				try {
					zipIn.close();
				} catch (IOException e) {
					LOGGER.error(e.getMessage(), e);
				}
			}
		}
	}

	private void createReaderAndConvertToJsonLD(InputStream in, RDFFormat dataFormat) {
		in = markSupported(in);

		InputStreamReader isr;
		try {
			isr = new InputStreamReader(new BOMInputStream(in, false), "UTF-8");
			try (BufferedReader reader = new BufferedReader(isr, 1024)) {
				convertAndLoadInMongoIfNeeded(reader, dataFormat);
			}
		} catch (IOException e) {
			LOGGER.error(e.getMessage(), e);
		}
	}
}
