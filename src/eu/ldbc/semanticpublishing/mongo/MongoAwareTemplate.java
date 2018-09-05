package eu.ldbc.semanticpublishing.mongo;

import eu.ldbc.semanticpublishing.properties.Configuration;
import eu.ldbc.semanticpublishing.templates.MustacheTemplate;

import java.util.HashMap;

public abstract class MongoAwareTemplate extends MustacheTemplate {

	private static final String MONGO_PREFIX = "mongo_";

	private final String templateDefaultName;

	public MongoAwareTemplate(HashMap<String, String> queryTemplates, String[] substitutionParameters, String templateDefaultName) {
		super(queryTemplates, substitutionParameters);
		this.templateDefaultName = templateDefaultName;
	}

	@Override
	public String getTemplateFileName() {
		return (Configuration.INSTANCE.getBoolean(Configuration.MONGO_ENABLED) ? MONGO_PREFIX : "") + templateDefaultName;
	}
}
