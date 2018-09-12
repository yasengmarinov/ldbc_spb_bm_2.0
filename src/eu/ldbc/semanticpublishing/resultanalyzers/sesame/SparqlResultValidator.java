package eu.ldbc.semanticpublishing.resultanalyzers.sesame;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;

import org.eclipse.rdf4j.query.BindingSet;
import org.eclipse.rdf4j.query.QueryEvaluationException;
import org.eclipse.rdf4j.query.TupleQueryResult;
import org.eclipse.rdf4j.query.TupleQueryResultHandlerException;
import org.eclipse.rdf4j.query.impl.TupleQueryResultBuilder;
import org.eclipse.rdf4j.query.resultio.QueryResultParseException;
import org.eclipse.rdf4j.query.resultio.sparqlxml.SPARQLResultsXMLParser;

public class SparqlResultValidator {
	
	public int validate(String validationString, String resultString) throws TupleQueryResultHandlerException, QueryResultParseException, IOException, QueryEvaluationException {
		int errors = 0;
		
		TupleQueryResult validationSet = parse(validationString);
		TupleQueryResult resultSet = parse(resultString);
		
		ArrayList<BindingSet> resultList = new ArrayList<BindingSet>();
		
		while (resultSet.hasNext()) {
			resultList.add(resultSet.next());
		}

		while (validationSet.hasNext()) {
			boolean exists = false;
			BindingSet validationBindingSet = validationSet.next();
			
			for (int i = 0; i < resultList.size(); i++) {
				BindingSet bs = resultList.get(i);				
				if (bs.equals(validationBindingSet)) {
					exists = true;
					break;					
				}
			}
	
			if (!exists) {
				System.out.println("\t\tvalidation failed, missing : " + validationBindingSet.toString());
				errors++;				
			}
		}
		
		return errors;
	}
	
	private TupleQueryResult parse(String s) throws IOException, TupleQueryResultHandlerException, QueryResultParseException {
		SPARQLResultsXMLParser sparqlParser = new SPARQLResultsXMLParser();
		TupleQueryResultBuilder tupleQueryResultBuilder = new TupleQueryResultBuilder();
		InputStream inputStream = new ByteArrayInputStream(s.getBytes("UTF8"));	
		sparqlParser.setTupleQueryResultHandler(tupleQueryResultBuilder);
		sparqlParser.parse(inputStream);
		
		return tupleQueryResultBuilder.getQueryResult();
	}
}
