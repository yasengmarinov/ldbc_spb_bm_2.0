package eu.ldbc.semanticpublishing.templates.aggregation;

import eu.ldbc.semanticpublishing.endpoint.SparqlQueryConnection.QueryType;
import eu.ldbc.semanticpublishing.mongo.MongoAwareTemplate;
import eu.ldbc.semanticpublishing.properties.Definitions;
import eu.ldbc.semanticpublishing.refdataset.DataManager;
import eu.ldbc.semanticpublishing.refdataset.model.Entity;
import eu.ldbc.semanticpublishing.substitutionparameters.SubstitutionParametersGenerator;
import eu.ldbc.semanticpublishing.util.RandomUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;

/**
 * A class extending the MustacheTemplate, used to generate a query string
 * corresponding to file Configuration.QUERIES_PATH/aggregation/query11.txt
 */
public class Query11Template extends MongoAwareTemplate implements SubstitutionParametersGenerator {
	//must match with corresponding file name of the mustache template file
	private static final String templateFileName = "query11.txt";

	private final RandomUtil ru;

	public Query11Template(RandomUtil ru, HashMap<String, String> queryTemplates, Definitions definitions, String[] substitutionParameters) {
		super(queryTemplates, substitutionParameters, templateFileName);
		this.ru = ru;
	}

	/**
	 * A method for replacing mustache template : {{{entitiyOfInterest}}}
	 */
	public String entitiyOfInterest() {
		if (substitutionParameters != null) {
			return substitutionParameters[parameterIndex++];
		}

		Entity e;
		e = DataManager.popularEntitiesList.get(ru.nextInt(DataManager.popularEntitiesList.size()));

		return e.getURI();
	}

	@Override
	public String generateSubstitutionParameters(BufferedWriter bw, int amount) throws IOException {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < amount; i++) {
			String entityOfInterest = entitiyOfInterest();
			sb.setLength(0);
			sb.append(entityOfInterest);
			sb.append(SubstitutionParametersGenerator.PARAMS_DELIMITER);
			sb.append(entityOfInterest);
			sb.append("\n");
			bw.write(sb.toString());
		}
		return null;
	}

	@Override
	public QueryType getTemplateQueryType() {
		return QueryType.SELECT;
	}
}
