package eu.ldbc.semanticpublishing.agents;

import com.mongodb.client.MongoCollection;
import eu.ldbc.semanticpublishing.TestDriver;
import eu.ldbc.semanticpublishing.endpoint.SparqlQueryConnection;
import eu.ldbc.semanticpublishing.endpoint.SparqlQueryConnection.QueryType;
import eu.ldbc.semanticpublishing.endpoint.SparqlQueryExecuteManager;
import eu.ldbc.semanticpublishing.mongo.Utils;
import eu.ldbc.semanticpublishing.properties.Configuration;
import eu.ldbc.semanticpublishing.properties.Definitions;
import eu.ldbc.semanticpublishing.refdataset.DataManager;
import eu.ldbc.semanticpublishing.statistics.Statistics;
import eu.ldbc.semanticpublishing.substitutionparameters.SubstitutionParametersGenerator;
import eu.ldbc.semanticpublishing.templates.editorial.DeleteTemplate;
import eu.ldbc.semanticpublishing.templates.editorial.InsertTemplate;
import eu.ldbc.semanticpublishing.templates.editorial.UpdateTemplate;
import eu.ldbc.semanticpublishing.util.RandomUtil;
import eu.ldbc.semanticpublishing.util.RdfUtils;
import eu.ldbc.semanticpublishing.validation.EditorialOperationsValidator;
import eu.ldbc.semanticpublishing.validation.EditorialOperationsValidator.EditorialOperation;
import org.bson.Document;
import org.eclipse.rdf4j.model.IRI;
import org.eclipse.rdf4j.model.Model;
import org.eclipse.rdf4j.model.Resource;
import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.model.impl.LinkedHashModel;
import org.eclipse.rdf4j.model.impl.SimpleValueFactory;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.Rio;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.StringReader;
import java.util.HashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A class that represents an editorial agent. It executes INSERT, UPDATE, DELETE queries
 * with a defined distribution, updates query execution statistics.
 */
public class EditorialAgent extends AbstractAsynchronousAgent {
	private final SparqlQueryExecuteManager queryExecuteManager;
	private final RandomUtil ru;
	private final AtomicBoolean benchmarkingState;
	protected final HashMap<String, String> queryTemplates;
	private SparqlQueryConnection connection;
	private Definitions definitions;
	private final boolean enableValidation;
	private final int editorialOpsValidationInterval;
	private final AtomicBoolean maxUpdateOperationsReached;
	private EditorialOperationsValidator editorialOperationsValidator;
	private boolean mongoRun;
	private MongoCollection<Document> coll;

	private final static Logger DETAILED_LOGGER = LoggerFactory.getLogger(EditorialAgent.class.getName());
	private final static Logger BRIEF_LOGGER = LoggerFactory.getLogger(TestDriver.class.getName());

	private final static long SLEEP_TIME_MS = 1000;

	public EditorialAgent(AtomicBoolean benchmarkingState, SparqlQueryExecuteManager queryExecuteManager, RandomUtil ru, AtomicBoolean runFlag, HashMap<String, String> queryTemplates, HashMap<String, String> validationQueryTemplates, Configuration configuration, Definitions definitions, AtomicBoolean maxUpdateOperationsReached) {
		super(runFlag);
		this.queryExecuteManager = queryExecuteManager;
		this.ru = ru;
		this.benchmarkingState = benchmarkingState;
		this.queryTemplates = queryTemplates;
		this.connection = new SparqlQueryConnection(queryExecuteManager.getEndpointUrl(), queryExecuteManager.getEndpointUpdateUrl(), RdfUtils.CONTENT_TYPE_RDFXML, queryExecuteManager.getTimeoutMilliseconds(), true);
		this.definitions = definitions;
		this.maxUpdateOperationsReached = maxUpdateOperationsReached;
		this.enableValidation = configuration.getBoolean(Configuration.ENABLE_EDITORIAL_OPS_VALIDATION);
		this.editorialOpsValidationInterval = configuration.getInt(Configuration.EDITORIAL_OPS_VALIDATION_INTEVAL);
		this.editorialOperationsValidator = new EditorialOperationsValidator(queryExecuteManager, ru, queryTemplates, validationQueryTemplates, configuration, definitions);
		this.mongoRun = configuration.getBoolean(Configuration.MONGO_RUN);
		if (mongoRun) {
			coll = Utils.getCollection(configuration);
		}
	}

	@Override
	public boolean executeLoop() {
		int queryDistribution = Definitions.editorialOperationsAllocation.getAllocation();

		long queryId = 0;
		String queryName = "";
		String queryString = "";
		String queryResult = "";
		QueryType queryType = QueryType.INSERT;
		int validationErrors = 0;
		String[] validationParameters = null;

		try {

			if (maxUpdateOperationsReached.get()) {
				DETAILED_LOGGER.info(Thread.currentThread().getName() + " : Max update operations per seconds has been reached, skipping current update until update rate drops below configured maximum.");
				Thread.sleep(SLEEP_TIME_MS);
				return true;
			}

			switch (queryDistribution) {
				case 0:
					InsertTemplate insertQuery = new InsertTemplate("", ru, queryTemplates, definitions);

					queryType = insertQuery.getTemplateQueryType();
					queryName = insertQuery.getTemplateFileName();
					queryString = insertQuery.compileMustacheTemplate();

					queryId = Statistics.insertCreativeWorksQueryStatistics.getNewQueryId();

					if ((queryId > 0) && (queryId % editorialOpsValidationInterval == 0) && enableValidation && !mongoRun) {
						validationParameters = insertQuery.generateSubstitutionParameters(null, 1).split(SubstitutionParametersGenerator.PARAMS_DELIMITER);
						validationErrors = editorialOperationsValidator.validateAction(EditorialOperation.INSERT, 0, validationParameters, false);
						if (validationErrors > 0) {
							updateQueryStatistics(false, queryType, queryName, "validate insert " + queryId, "", 0, System.currentTimeMillis());
						}
					}

					break;
				case 1:
					long cwNextId = ru.nextInt((int) DataManager.creativeWorksNextId.get());
					String uri = ru.numberURI("context", cwNextId, true, true);

					UpdateTemplate updateQuery = new UpdateTemplate(uri, ru, queryTemplates, definitions);

					queryType = updateQuery.getTemplateQueryType();
					queryName = updateQuery.getTemplateFileName();
					queryString = updateQuery.compileMustacheTemplate();

					queryId = Statistics.updateCreativeWorksQueryStatistics.getNewQueryId();

					break;
				case 2:
					DeleteTemplate deleteQuery = new DeleteTemplate(ru, queryTemplates);

					queryType = deleteQuery.getTemplateQueryType();
					queryName = deleteQuery.getTemplateFileName();
					queryString = deleteQuery.compileMustacheTemplate();

					queryId = Statistics.deleteCreativeWorksQueryStatistics.getNewQueryId();

					if ((queryId > 0) && (queryId % editorialOpsValidationInterval == 0) && enableValidation && !mongoRun) {
						validationParameters = deleteQuery.generateSubstitutionParameters(null, 1).split(SubstitutionParametersGenerator.PARAMS_DELIMITER);
						validationErrors = editorialOperationsValidator.validateAction(EditorialOperation.DELETE, 0, validationParameters, false);
						if (validationErrors > 0) {
							updateQueryStatistics(false, queryType, queryName, "validate delete " + queryId, "", 0, System.currentTimeMillis());
						}
					}

					break;
			}

			long executionTimeMs = System.currentTimeMillis();

			if (!mongoRun) {
				queryResult = queryExecuteManager.executeQueryWithStringResult(connection, queryName, queryString, queryType, true, false);
			} else {
				Model model = Rio.parse(new StringReader(queryString), "", RDFFormat.TURTLE);
				IRI id = null;

				for (Resource subject : model.subjects()) {
					if (subject.stringValue().startsWith("http://www.bbc.co.uk/context/")) {
						id = SimpleValueFactory.getInstance().createIRI(subject.stringValue().replace("/context/", "/things/"));
						break;
					}
					if (subject.stringValue().startsWith("http://www.bbc.co.uk/things/")) {
						id = SimpleValueFactory.getInstance().createIRI(subject.stringValue());
						break;
					}
				}

				if (id == null) {
					throw new RuntimeException("Could not get context from mongo doc :(");
				}

				Document matchClause = Document.parse("{\"@graph.0.@id\" : \"" + id.stringValue() + "\"}");
				if (queryType.equals(QueryType.DELETE)) {
					executionTimeMs = System.currentTimeMillis();
					coll.deleteOne(matchClause);
				}
				if (queryType.equals(QueryType.UPDATE) || queryType.equals(QueryType.INSERT)) {
					Model withCtx = new LinkedHashModel();
					for (Statement statement : model) {
						withCtx.add(statement.getSubject(), statement.getPredicate(), statement.getObject(), id);
					}

					Document doc = Utils.modelToDocument(withCtx);

					executionTimeMs = System.currentTimeMillis();
					if (queryType.equals(QueryType.INSERT)) {
						coll.insertOne(doc);
					}
					else {
						coll.replaceOne(matchClause, doc);
					}
				}
			}

			updateQueryStatistics(true, queryType, queryName, queryString, queryResult, queryId, System.currentTimeMillis() - executionTimeMs);
		} catch (InterruptedException ie) {
			DETAILED_LOGGER.warn("InterruptedException : " + ie.getMessage());
		} catch (Throwable t) {
			String msg = "Warning : EditorialAgent [" + Thread.currentThread().getName() + "] reports: " + t.getMessage() + ", attempting a new connection" + "\n" + "\tfor query : \n" + connection.getQueryString();

			System.out.println(msg);
			System.out.println(t);

			DETAILED_LOGGER.warn(msg);

			updateQueryStatistics(false, queryType, queryName, queryString, queryResult, queryId, 0);

			connection.disconnect();
			connection = new SparqlQueryConnection(queryExecuteManager.getEndpointUrl(), queryExecuteManager.getEndpointUpdateUrl(), RdfUtils.CONTENT_TYPE_RDFXML, queryExecuteManager.getTimeoutMilliseconds(), true);
		}

		return true;
	}

	@Override
	public void executeFinalize() {
		connection.disconnect();
	}

	private void updateQueryStatistics(boolean reportSuccess, QueryType queryType, String queryName, String queryString, String queryResult, long id, long queryExecutionTimeMs) {

		String queryNameId = constructQueryNameId(queryName, queryType, id);

		//report success
		if (reportSuccess) {
			if (queryType == QueryType.INSERT) {
				if (queryResult.length() >= 0 && benchmarkingState.get()) {
					Statistics.insertCreativeWorksQueryStatistics.reportSuccess(queryExecutionTimeMs);
				}
			} else if (queryType == QueryType.UPDATE) {
				if (queryResult.length() >= 0 && benchmarkingState.get()) {
					Statistics.updateCreativeWorksQueryStatistics.reportSuccess(queryExecutionTimeMs);
				}
			} else if (queryType == QueryType.DELETE) {
				if (queryResult.length() >= 0 && benchmarkingState.get()) {
					Statistics.deleteCreativeWorksQueryStatistics.reportSuccess(queryExecutionTimeMs);
				}
			}

			logBrief(queryNameId, queryType, "", queryExecutionTimeMs);

			//report failure
		} else {
			if (queryType == QueryType.INSERT) {
				Statistics.insertCreativeWorksQueryStatistics.reportFailure();
			} else if (queryType == QueryType.UPDATE) {
				Statistics.updateCreativeWorksQueryStatistics.reportFailure();
			} else if (queryType == QueryType.DELETE) {
				Statistics.deleteCreativeWorksQueryStatistics.reportFailure();
			}
			logBrief(queryNameId, queryType, ", query error!", queryExecutionTimeMs);
		}
		DETAILED_LOGGER.info("\n*** Query [" + queryNameId + "], execution time : " + queryExecutionTimeMs + " ms\n" + queryString + "\n---------------------------------------------\n*** Result for query [" + queryNameId + "]" + " : \n" + "Length : " + queryResult.length() + "\n" + queryResult + "\n\n");
	}

	private void logBrief(String queryNameId, QueryType queryType, String appendString, long queryExecutionTimeMs) {
		StringBuilder reportSb = new StringBuilder();
		reportSb.append(String.format("\t[%s, %s] Query executed, execution time : %d ms %s", queryNameId, Thread.currentThread().getName(), queryExecutionTimeMs, appendString));

		BRIEF_LOGGER.info(reportSb.toString());
	}

	private String constructQueryNameId(String queryName, QueryType queryType, long id) {
		StringBuilder queryId = new StringBuilder();
		queryId.append(queryName);
		queryId.append(", id:");
		queryId.append("" + id);
		return queryId.toString();
	}
}
