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
 * corresponding to file Configuration.QUERIES_PATH/aggregation/query2.txt
 */
public class Query2Template extends MongoAwareTemplate implements SubstitutionParametersGenerator {

	//must match with corresponding file name of the mustache template file
	private static final String templateFileName = "query2.txt"; 
	
	private final RandomUtil ru;	
	
	public Query2Template(RandomUtil ru, HashMap<String, String> queryTemplates, Definitions definitions, String[] substitutionParameters) {
		super(queryTemplates, substitutionParameters, templateFileName);
		this.ru = ru;
	}
	
	/**
	 * A method for replacing mustache template : {{{cwUri}}}
	 */	
	public String cwUri() {
		if (substitutionParameters != null) {
			return substitutionParameters[parameterIndex++];
		}

		long cwNextId = ru.nextInt((int)DataManager.creativeWorksNextId.get());
		return ru.numberURI("things", cwNextId, true, true);		
	}

	@Override
	public String generateSubstitutionParameters(BufferedWriter bw, int amount) throws IOException {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < amount; i++) {
			sb.setLength(0);
			sb.append(cwUri().replace("<", "").replace(">", ""));
			sb.append("\n");
			bw.write(sb.toString());
		}
		return null;
	}
	
	@Override
	public QueryType getTemplateQueryType() {
		return QueryType.CONSTRUCT;
	}
}
