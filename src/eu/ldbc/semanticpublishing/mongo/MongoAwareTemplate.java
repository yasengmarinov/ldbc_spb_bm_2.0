package eu.ldbc.semanticpublishing.mongo;

import eu.ldbc.semanticpublishing.properties.Configuration;
import eu.ldbc.semanticpublishing.templates.MustacheTemplate;

import java.util.HashMap;

public abstract class MongoAwareTemplate extends MustacheTemplate {

	protected static final String MONGO_PREFIX = "mongo_";

	protected final String templateDefaultName;

	public MongoAwareTemplate(HashMap<String, String> queryTemplates, String[] substitutionParameters, String templateDefaultName) {
		super(queryTemplates, substitutionParameters);
		this.templateDefaultName = templateDefaultName;
	}

	@Override
	public String getTemplateFileName() {
		return (isMongoTemplate() ? MONGO_PREFIX : "") + templateDefaultName;
	}

	protected boolean isMongoTemplate() {
		return Configuration.INSTANCE.getBoolean(Configuration.MONGO_ENABLED);
	}
}
