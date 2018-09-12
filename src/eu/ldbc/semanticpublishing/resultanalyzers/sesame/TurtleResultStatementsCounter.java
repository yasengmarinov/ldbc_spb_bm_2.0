package eu.ldbc.semanticpublishing.resultanalyzers.sesame;

import java.io.IOException;
import java.io.InputStream;

import org.eclipse.rdf4j.model.Statement;
import org.eclipse.rdf4j.rio.RDFFormat;
import org.eclipse.rdf4j.rio.RDFHandlerException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.eclipse.rdf4j.rio.RDFParser;
import org.eclipse.rdf4j.rio.Rio;
import org.eclipse.rdf4j.rio.helpers.RDFHandlerBase;

import eu.ldbc.semanticpublishing.util.StringUtil;

public class TurtleResultStatementsCounter {
	private final RDFParser rdfParser;
	private final StatementsCounter statementsCounter;
	
	//stores last parse time for correcting query execution times
	private long parseTime = 0;
	
	private static final String BASE_URI_STRING = "http://www.ldbc.eu";
	
	public TurtleResultStatementsCounter() {
		statementsCounter = new StatementsCounter();
		rdfParser = Rio.createParser(RDFFormat.TURTLE);
		rdfParser.setRDFHandler(statementsCounter);
	}
	
	public long getStatementsCount(InputStream is) {
		try {
			statementsCounter.resetStatementsCount();
			
			//suppress warnings about not properly configured Log4J system caused by Sesame
//			PrintStream oldPrintStream = System.err;
//			PrintStream newPrintStream = new PrintStream(new ByteArrayOutputStream());
//			System.setErr(newPrintStream);

			long currentTime = System.currentTimeMillis();
			rdfParser.parse(is, BASE_URI_STRING);
			parseTime = System.currentTimeMillis() - currentTime;
			
			//restore back initial std output
//			System.setErr(oldPrintStream);			
		} catch (RDFParseException rpe) {
			rpe.printStackTrace();
			System.out.println("-Result causing exception:--------------------------------------------------");
			try {
				System.out.println(StringUtil.iostreamToString(is));
			} catch (IOException e) {
				e.printStackTrace();
			}
			System.out.println("----------------------------------------------------------------------------");
		} catch (RDFHandlerException rhe) {
			rhe.printStackTrace();
		} catch (IOException ioe) {
			ioe.printStackTrace();
		}
		return statementsCounter.getStatementsCount();
	}
	
	/**
	 * Method will be used to correct query execution times in statistics
	 * 
	 * @return time needed to count(parse) the result, ms
	 */
	public long getParseTime() {
		return parseTime;
	}
	
	static class StatementsCounter extends RDFHandlerBase {
		private int countedStatements = 0;
		  		  
		@Override
		public void handleStatement(Statement st) {
			countedStatements++;
		}
		
		public void resetStatementsCount() {
			countedStatements = 0;
		}
		
		public int getStatementsCount() {
			return countedStatements;
		}		 		 
	}
}
