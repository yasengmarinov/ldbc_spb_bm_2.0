package eu.ldbc.semanticpublishing.templates.aggregation;

import eu.ldbc.semanticpublishing.endpoint.SparqlQueryConnection.QueryType;
import eu.ldbc.semanticpublishing.mongo.MongoAwareTemplate;
import eu.ldbc.semanticpublishing.properties.Definitions;
import eu.ldbc.semanticpublishing.substitutionparameters.SubstitutionParametersGenerator;
import eu.ldbc.semanticpublishing.util.RandomUtil;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Calendar;
import java.util.HashMap;

/**
 * A class extending the MustacheTemplate, used to generate a query string
 * corresponding to file Configuration.QUERIES_PATH/aggregation/query7.txt
 */
public class Query7Template extends MongoAwareTemplate implements SubstitutionParametersGenerator {
	//must match with corresponding file name of the mustache template file
	private static final String templateFileName = "query7.txt";

	private final RandomUtil ru;
	private int creativeWorkType;
	private int year;
	private int month;
	private int day;
	private int hour;
	private int minute;
	private Calendar calendar;
	
	public Query7Template(RandomUtil ru, HashMap<String, String> queryTemplates, Definitions definitions, String[] substitutionParameters) {
		super(queryTemplates, substitutionParameters, templateFileName);
		this.ru = ru;
		this.calendar = Calendar.getInstance();
		preInitialize();
	}
	
	private void preInitialize() {
		creativeWorkType = Definitions.creativeWorkTypesAllocation.getAllocation();
		
		//Initializing year with a value that is certain to be used. see RandomUtil.YEARS_OFFSET
		calendar.setTime(ru.randomDateTime());
		year = calendar.get(Calendar.YEAR);
		month = ru.nextInt(1, 12 + 1);
		calendar.set(year, month - 1, 1);		
		day = ru.nextInt(1, calendar.getActualMaximum(Calendar.DAY_OF_MONTH) + 1);
		hour = ru.nextInt(0, 23 + 1);
		minute = ru.nextInt(0, 59 + 1);
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
	
	public String cwStartDateTime() {
		if (substitutionParameters != null) {
			return substitutionParameters[parameterIndex++];
		}

		return generateFilterStartDateString(year, month, day, hour, minute, Calendar.HOUR_OF_DAY, 1);
	}

	public String cwEndDateTime() {
		if (substitutionParameters != null) {
			return substitutionParameters[parameterIndex++];
		}

		return generateFilterEndDateString();
	}

	private String generateFilterStartDateString(int startYear, int startMonth, int startDay, int startHour, int startMinute, int calendarOffsetType, int offset) {
		StringBuilder sbStartRange = new StringBuilder();

		calendar.set(startYear, startMonth - 1, startDay, startHour, startMinute, 0);
		calendar.add(calendarOffsetType, offset);

		sbStartRange.append(startYear);
		sbStartRange.append("-");
		sbStartRange.append(String.format("%02d", startMonth));
		sbStartRange.append("-");
		sbStartRange.append(String.format("%02d", startDay));
		sbStartRange.append("T");
		sbStartRange.append(String.format("%02d", startHour));
		sbStartRange.append(":");
		sbStartRange.append(String.format("%02d", startMinute));
		sbStartRange.append(":");
		sbStartRange.append("00.000Z");

		return sbStartRange.toString();
	}

	private String generateFilterEndDateString() {
		StringBuilder sbEndRange = new StringBuilder();

		sbEndRange.append(calendar.get(Calendar.YEAR));
		sbEndRange.append("-");
		sbEndRange.append(String.format("%02d", calendar.get(Calendar.MONTH) + 1));
		sbEndRange.append("-");
		sbEndRange.append(String.format("%02d", calendar.get(Calendar.DAY_OF_MONTH)));
		sbEndRange.append("T");
		sbEndRange.append(String.format("%02d", calendar.get(Calendar.HOUR_OF_DAY)));
		sbEndRange.append(":");
		sbEndRange.append(String.format("%02d", calendar.get(Calendar.MINUTE)));
		sbEndRange.append(":");
		sbEndRange.append("00.000Z");

		return sbEndRange.toString();
	}
	
	@Override
	public String generateSubstitutionParameters(BufferedWriter bw, int amount) throws IOException {
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < amount; i++) {
			preInitialize();
			sb.setLength(0);
			sb.append(cwType());
			sb.append(SubstitutionParametersGenerator.PARAMS_DELIMITER);
			sb.append(cwStartDateTime());
			sb.append(SubstitutionParametersGenerator.PARAMS_DELIMITER);
			sb.append(cwEndDateTime());
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
