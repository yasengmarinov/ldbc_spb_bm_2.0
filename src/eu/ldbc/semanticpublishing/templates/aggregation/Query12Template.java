package eu.ldbc.semanticpublishing.templates.aggregation;

import eu.ldbc.semanticpublishing.endpoint.SparqlQueryConnection.QueryType;
import eu.ldbc.semanticpublishing.mongo.MongoAwareTemplate;
import eu.ldbc.semanticpublishing.properties.Definitions;
import eu.ldbc.semanticpublishing.refdataset.DataManager;
import eu.ldbc.semanticpublishing.substitutionparameters.SubstitutionParametersGenerator;
import eu.ldbc.semanticpublishing.util.RandomUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;

/**
 * A class extending the MustacheTemplate, used to generate a query string
 * corresponding to file Configuration.QUERIES_PATH/aggregation/query12.txt
 */
public class Query12Template  extends MongoAwareTemplate implements SubstitutionParametersGenerator {
	//must match with corresponding file name of the mustache template file
	private static final String templateFileName = "query12.txt";

	private final RandomUtil ru;
		
	public Query12Template(RandomUtil ru, HashMap<String, String> queryTemplates, Definitions definitions, String[] substitutionParameters) {
		super(queryTemplates, substitutionParameters, templateFileName);
		this.ru = ru;
	}
	
	/**
	 * A method for replacing mustache template : {{{creativeWorkTag}}}
	 * Will return DBpedia locations only
	 */
	public String creativeWorkTag() {
		if (substitutionParameters != null) {
			return substitutionParameters[parameterIndex++];
		}

		return DataManager.locationsIdsList.get(ru.nextInt(DataManager.locationsIdsList.size()));
	}	
	
	@Override
	public String generateSubstitutionParameters(BufferedWriter bw, int amount) throws IOException {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < amount; i++) {
			sb.setLength(0);
			sb.append(creativeWorkTag().replace("<", "").replace(">", ""));
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
