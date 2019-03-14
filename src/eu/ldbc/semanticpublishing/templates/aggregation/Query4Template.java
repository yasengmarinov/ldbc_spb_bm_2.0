package eu.ldbc.semanticpublishing.templates.aggregation;

import eu.ldbc.semanticpublishing.endpoint.SparqlQueryConnection.QueryType;
import eu.ldbc.semanticpublishing.mongo.MongoAwareTemplate;
import eu.ldbc.semanticpublishing.properties.Definitions;
import eu.ldbc.semanticpublishing.refdataset.DataManager;
import eu.ldbc.semanticpublishing.refdataset.model.Entity;
import eu.ldbc.semanticpublishing.substitutionparameters.SubstitutionParametersGenerator;
import eu.ldbc.semanticpublishing.tools.Prefixes;
import eu.ldbc.semanticpublishing.util.RandomUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.HashMap;

/**
 * A class extending the MustacheTemplate, used to generate a query string
 * corresponding to file Configuration.QUERIES_PATH/aggregation/query4.txt
 */
public class Query4Template extends MongoAwareTemplate implements SubstitutionParametersGenerator {
	//must match with corresponding file name of the mustache template file
	private static final String templateFileName = "query4.txt";
	private final RandomUtil ru;	
	private int creativeWorkType;
	
	public Query4Template(RandomUtil ru, HashMap<String, String> queryTemplates, Definitions definitions, String[] substitutionParameters) {
		super(queryTemplates, substitutionParameters, templateFileName);
		this.ru = ru;
		preInitialize();
	}

	private void preInitialize() {
		this.creativeWorkType = Definitions.creativeWorkTypesAllocation.getAllocation();
	}
	
	/**
	 * A method for replacing mustache template : {{{cwAboutUri}}}
	 */		
	public String cwAboutUri() {
		if (substitutionParameters != null) {
			return substitutionParameters[parameterIndex++];
		}
		
		//use a popular or regular entity for about/mentions uri 
		boolean usePopularEntity = Definitions.usePopularEntities.getAllocation() == 0;
		
		Entity e;
		
		if (usePopularEntity) {
			e = DataManager.popularEntitiesList.get(ru.nextInt(DataManager.popularEntitiesList.size()));
		} else {
			e = DataManager.regularEntitiesList.get(ru.nextInt(DataManager.regularEntitiesList.size()));
		}
		
		return e.getURI();
	}
	
	/**
	 * A method for replacing mustache template : {{{cwFormat}}}
	 */	
	public String cwFormat() {
		if (substitutionParameters != null) {
			return substitutionParameters[parameterIndex++];
		}		
		
		switch (creativeWorkType) {
		//BlogPost
		case 0 :
			return "cwork:TextualFormat";
		//NewsItem
		case 1 :
			if (ru.nextBoolean()) {
				return "cwork:TextualFormat";
			} else {
				return "cwork:InteractiveFormat";
			}
		//Programme
		case 2 :
			if (ru.nextBoolean()) {
				return "cwork:VideoFormat";
			} else {
				return "cwork:AudioFormat";
			}
		}
		return "cwork:TextualFormat";
	}
	
	/**
	 * A method for replacing mustache template : {{{cwType}}}
	 */	
	public String cwType() {
		if (substitutionParameters != null) {
			return substitutionParameters[parameterIndex++];
		}		
		
		switch (creativeWorkType) {
		case 0 :
			return "cwork:BlogPost";
		case 1 :
			return "cwork:NewsItem";
		case 2 :
			return "cwork:Programme";
		}
		return "cwork:BlogPost";
	}	
	
	/**
	 * A method for replacing mustache template : {{{randomLimit}}}
	 */			
	public String randomLimit() {
		if (substitutionParameters != null) {
			return substitutionParameters[parameterIndex++];
		}		
		
		return "" + ru.nextInt(5, 20 + 1);
	}	
	
	@Override
	public String generateSubstitutionParameters(BufferedWriter bw, int amount) throws IOException {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < amount; i++) {
			preInitialize();
			sb.setLength(0);
			if (!isMongoTemplate()) {
				sb.append(cwAboutUri().replace("<", "").replace(">", ""));
				sb.append(SubstitutionParametersGenerator.PARAMS_DELIMITER);
				sb.append(cwFormat());
				sb.append(SubstitutionParametersGenerator.PARAMS_DELIMITER);
				sb.append(cwType());
				sb.append(SubstitutionParametersGenerator.PARAMS_DELIMITER);
				sb.append(randomLimit());
			} else {
				sb.append(Prefixes.compact(cwType()));
				sb.append(SubstitutionParametersGenerator.PARAMS_DELIMITER);
				sb.append(Prefixes.compact(cwFormat()));
				sb.append(SubstitutionParametersGenerator.PARAMS_DELIMITER);
				sb.append(randomLimit());
				sb.append(SubstitutionParametersGenerator.PARAMS_DELIMITER);
				sb.append(cwAboutUri().replace("<", "").replace(">", "").replace("http://dbpedia.org/resource/", "dbpedia:").replace("http://sws.geonames.org/", "geonames:"));
			}
			sb.append("\n");
			bw.write(sb.toString());
		}
		return null;
	}
	
	@Override
	public QueryType getTemplateQueryType() {
		return QueryType.DESCRIBE;
	}
}
