import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import org.simmetrics.StringMetric;
import org.simmetrics.metrics.StringMetrics;
import org.simmetrics.metrics.CosineSimilarity;
import javafx.util.Pair;

public class IntentCreatorMultiThread extends Thread{
	ArrayList<String> sessQueries;
	Pair<Integer,Integer> lowerUpperIndexBounds;
	String outputFile;
	SchemaParser schParse;
	int threadID;
	String pruneKeepModifyRepeatedQueries;
	String prevQueryBitVector = null;
	
	public IntentCreatorMultiThread(int threadID, ArrayList<String> sessQueries, Pair<Integer,Integer> lowerUpperIndexBounds, String outputFile, SchemaParser schParse, String pruneKeepModifyRepeatedQueries) {
		this.sessQueries = sessQueries;
		this.lowerUpperIndexBounds = lowerUpperIndexBounds;
		this.outputFile = outputFile;
		this.schParse = schParse;
		this.threadID = threadID;
		this.pruneKeepModifyRepeatedQueries = pruneKeepModifyRepeatedQueries;
	}
	
	public void processQueriesKeepOrModifyReps() throws Exception{
		assert (pruneKeepModifyRepeatedQueries == "KEEP" || pruneKeepModifyRepeatedQueries == "MODIFY");
		MINCFragmentIntent.deleteIfExists(this.outputFile);
		BufferedWriter bw = new BufferedWriter(new FileWriter(this.outputFile, true));
		double absQueryID = 0;
		int queryID = 0;
		String prevSessionID = "";
		String concLine = "";
		int lowerIndex = this.lowerUpperIndexBounds.getKey();
		int upperIndex = this.lowerUpperIndexBounds.getValue();
		System.out.println("Initialized Thread ID: "+this.threadID+" with outputFile "+this.outputFile);
		for(int index = lowerIndex; index <= upperIndex; index++) {
			String line = this.sessQueries.get(index);
			if(line.contains("Query")) {
				String[] tokens = line.trim().split(" ");
				String query = "";
				for(int i=2; i<tokens.length; i++) {
					if(i==2)
						query = tokens[i];
					else
						query += " "+tokens[i];
				}
		//		System.out.println("Query: "+query);
				query = query.trim();
				boolean validQuery = false;
				if(query.toLowerCase().startsWith("select") || query.toLowerCase().startsWith("insert") || query.toLowerCase().startsWith("update") || query.toLowerCase().startsWith("delete")) {
					validQuery = true;
				}					
				try {						
					MINCFragmentIntent fragmentObj = null;
					if(validQuery) {
						fragmentObj = new MINCFragmentIntent(query, this.schParse);
				//		System.out.println("Inside Thread ID: "+this.threadID+" valid Query");
					}
					if(fragmentObj!=null) {
						validQuery = fragmentObj.parseQueryAndCreateFragmentVectors();
				//		System.out.println("Inside Thread ID: "+this.threadID+" valid Query obtained FragmentObj");
					}
					else
						validQuery = false;
					/*if(validQuery)
						fragmentObj.printIntentVector();*/
					if(validQuery) {
			//			System.out.println("Inside Thread ID: "+this.threadID+" Created fragment vector writing it to file");
						String sessionID = tokens[0];
						if(!sessionID.equals(prevSessionID)) {
							queryID = 1;  // queryID starts with 1 not 0
							prevSessionID = sessionID;
						} 
						else if (pruneKeepModifyRepeatedQueries == "KEEP")
							queryID++;
						else if(pruneKeepModifyRepeatedQueries == "MODIFY") {
							String curQueryBitVector = fragmentObj.getIntentBitVector();
							if (curQueryBitVector.equals(prevQueryBitVector)) {
								continue;
							} else
								queryID++;
						}
						absQueryID++;
						String to_append = "Session "+sessionID+", Query "+queryID+"; OrigQuery: "+query+";"+fragmentObj.getIntentBitVector()+"\n";
						concLine += to_append;
						
						bw.append(concLine);
						bw.flush();
						concLine = "";
						if(absQueryID % 10 == 0) {									
//							System.out.println("Query: "+query);
							System.out.println("ThreadID: "+this.threadID+", Covered SessionID: "+sessionID+", queryID: "+queryID+", absQueryID: "+absQueryID);
						}
					}
				} catch(Exception e) {
					continue;
				}
			}
		}
		if(!concLine.equals("")) {
			bw.append(concLine);
			bw.flush();
		}
	}
	
	public boolean isValidSession(ArrayList<String> sessQueries) {
		if(sessQueries.size()==0)
			return false;
		boolean isValid = true;
		String prevSessQuery = null;
		for(String curSessQuery : sessQueries) {
			if(prevSessQuery != null) {
				StringMetric metric = StringMetrics.cosineSimilarity();
				float result = metric.compare(curSessQuery, prevSessQuery);
				if(result > 0.9)
					isValid = false; // curQuery is similar to prevQuery -- so repetition likely
			}
			prevSessQuery = curSessQuery;
		}
		System.out.println("isValid: "+isValid);
		return isValid;
	}
	
	public double createSessQueryBitVectors(String sessionID, double absQueryID, BufferedWriter bw, ArrayList<String> curSessQueries) throws Exception{
		int queryID = 0;
		String concLine = "";
		for(String query : curSessQueries) {
			try {						
				MINCFragmentIntent fragmentObj = null;
				fragmentObj = new MINCFragmentIntent(query, this.schParse);
				boolean validQuery = false;
			//		System.out.println("Inside Thread ID: "+this.threadID+" valid Query");
				if(fragmentObj!=null) {
					validQuery = fragmentObj.parseQueryAndCreateFragmentVectors();
			//		System.out.println("Inside Thread ID: "+this.threadID+" valid Query obtained FragmentObj");
				}
				/*if(validQuery)
					fragmentObj.printIntentVector();*/
				if(validQuery) {
		//			System.out.println("Inside Thread ID: "+this.threadID+" Created fragment vector writing it to file");			
					queryID++;  // queryID starts with 1 not 0
					absQueryID++;
					String to_append = "Session "+sessionID+", Query "+queryID+"; OrigQuery: "+query+";"+fragmentObj.getIntentBitVector()+"\n";
					concLine += to_append;
					
					bw.append(concLine);
					bw.flush();
					concLine = "";
					if(absQueryID % 10 == 0) {									
//						System.out.println("Query: "+query);
						System.out.println("ThreadID: "+this.threadID+", Covered SessionID: "+sessionID+", queryID: "+queryID+", absQueryID: "+absQueryID);
					}
				}
			} catch(Exception e) {
				continue;
			}
		}
		if(!concLine.equals("")) {
			bw.append(concLine);
			bw.flush();
		}
		return absQueryID;
	}
	
	public void processQueriesPruneReps() throws Exception{
		assert (pruneKeepModifyRepeatedQueries == "PRUNE");
		System.out.println("Entered Fn");
		MINCFragmentIntent.deleteIfExists(this.outputFile);
		BufferedWriter bw = new BufferedWriter(new FileWriter(this.outputFile, true));
		double absQueryID = 0;
		String prevSessionID = "";
		String sessionID = "";
		String query = "";
		int lowerQueryIndex = this.lowerUpperIndexBounds.getKey();
		int upperQueryIndex = this.lowerUpperIndexBounds.getValue();
		System.out.println("Initialized Thread ID: "+this.threadID+" with outputFile "+this.outputFile);
		int curQueryIndex = lowerQueryIndex;
		ArrayList<String> curSessQueries = new ArrayList<String>();
		while(curQueryIndex <= upperQueryIndex) {
			while(sessionID.equals(prevSessionID)) { // iterates over all queries in a session, terminates on new session
				if(query.toLowerCase().startsWith("select") || query.toLowerCase().startsWith("insert") || query.toLowerCase().startsWith("update") || query.toLowerCase().startsWith("delete")) {
					curSessQueries.add(query);
				}
				//read queries session-wise
				String line = this.sessQueries.get(curQueryIndex);
				if(line.contains("Query")) {
					String[] tokens = line.trim().split(" ");
					query = "";
					for(int i=2; i<tokens.length; i++) {
						if(i==2)
							query = tokens[i];
						else
							query += " "+tokens[i];
					}
					sessionID = tokens[0];
					if(!sessionID.equals(prevSessionID)) {
						prevSessionID = sessionID;
					} 
					System.out.println("Query: "+query);
					query = query.trim();
				}
				curQueryIndex++;
			}
			//curQueryIndex = populateCurSessionQueries(curQueryIndex, sessionID, prevSessionID, curSessQueries); // index incremented within this method
			// if sessQueries is not empty, check for repetitions
			boolean validSess = isValidSession(curSessQueries);
			if(validSess) {
				absQueryID = createSessQueryBitVectors(sessionID, absQueryID, bw, curSessQueries);
			}
		}
	}
	
	public void run(){
		try {
			System.out.println("Entered Run");
			assert (pruneKeepModifyRepeatedQueries == "PRUNE" || pruneKeepModifyRepeatedQueries == "KEEP" || pruneKeepModifyRepeatedQueries == "MODIFY");
			if (pruneKeepModifyRepeatedQueries == "KEEP" || pruneKeepModifyRepeatedQueries == "MODIFY") {	
				System.out.println(pruneKeepModifyRepeatedQueries);
				processQueriesKeepOrModifyReps();
			}
			else if(pruneKeepModifyRepeatedQueries == "PRUNE") {
				System.out.println(pruneKeepModifyRepeatedQueries);
				processQueriesPruneReps();
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
