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
	String refinedSessFile;
	
	public IntentCreatorMultiThread(int threadID, ArrayList<String> sessQueries, Pair<Integer,Integer> lowerUpperIndexBounds, 
			String outputFile, SchemaParser schParse, String pruneKeepModifyRepeatedQueries, String refinedSessFile) {
		this.sessQueries = sessQueries;
		this.lowerUpperIndexBounds = lowerUpperIndexBounds;
		this.outputFile = outputFile;
		this.schParse = schParse;
		this.threadID = threadID;
		this.pruneKeepModifyRepeatedQueries = pruneKeepModifyRepeatedQueries;
		this.refinedSessFile = refinedSessFile;
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
		if(sessQueries.size()==0) {
	//		System.out.println("Session Empty !");
			return false;
		}
		if(sessQueries.get(0).contains("SELECT * FROM jos_session WHERE session_id =") &&
				sessQueries.get(1).contains("DELETE FROM jos_session WHERE ( time <")  &&
				sessQueries.get(2).contains("SELECT * FROM jos_session WHERE session_id =")
				)
			return false;
		if(sessQueries.size()==1)
			return false;
		if(sessQueries.get(0).contains("SELECT * FROM jos_session WHERE session_id =") && 
				sessQueries.get(1).contains("UPDATE `jos_session` SET `time`=") &&
				sessQueries.size()==2)
			return false;
		if(sessQueries.get(0).contains("SELECT usefulness FROM jos_community_usefulness WHERE userid =") &&
				sessQueries.get(1).contains("SELECT usefulness FROM resource WHERE gid =") &&
				sessQueries.get(2).contains("SELECT count(*) FROM jos_community_usefulness WHERE resourceid ="))
			return false;
		boolean isValid = true;
		String prevSessQuery = null;
		for(String curSessQuery : sessQueries) {
			if(!isValid)
				return isValid;
			if(prevSessQuery != null) {
				StringMetric metric = StringMetrics.cosineSimilarity();
				float cosineSim = metric.compare(curSessQuery, prevSessQuery);
				if(cosineSim > 0.8)
					isValid = false; // curQuery is similar to prevQuery -- so repetition likely
	//			if(!isValid)
	//				System.out.println(curSessQuery+";"+prevSessQuery+"; cosineSim: "+cosineSim+"; isValid: "+isValid);
			}
			prevSessQuery = curSessQuery;
		}
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
	
	public double appendToValidSessFile(double absSessID, double absQueryID, BufferedWriter bw, ArrayList<String> curSessQueries) throws Exception {
		int queryID = 0;
		String concLine = "";
		for(String query : curSessQueries) {
			try {			
				queryID++;
				absQueryID++;
				String to_append = "Session "+absSessID+", Query "+queryID+"; OrigQuery: "+query+"\n";
				concLine += to_append;
				bw.append(concLine);
				bw.flush();
				concLine = "";
				if(absQueryID % 10000 == 0) {									
//					System.out.println("Query: "+query);
					System.out.println("ThreadID: "+this.threadID+", Appended SessionID: "+absSessID+", queryID: "+queryID+", absQueryID: "+absQueryID);
				}
			} catch(Exception e) {
				e.printStackTrace();
			}
		}
		return absQueryID;
	}
	
	public void processQueriesPruneReps() throws Exception{
		MINCFragmentIntent.deleteIfExists(this.refinedSessFile);
		BufferedWriter bw = new BufferedWriter(new FileWriter(this.refinedSessFile, true));
		double absQueryID = 0;
		double absSessID = 0;
		String prevSessionID = "";
		String sessionID = "";
		String query = "";
		int lowerQueryIndex = this.lowerUpperIndexBounds.getKey();
		int upperQueryIndex = this.lowerUpperIndexBounds.getValue();
		System.out.println("Initialized Thread ID: "+this.threadID+" with outputFile "+this.refinedSessFile);
		int curQueryIndex = lowerQueryIndex;
		ArrayList<String> curSessQueries = new ArrayList<String>();
		int numValidSessions = 0;
		int numValidQueries = 0;
		while(curQueryIndex <= upperQueryIndex) {
			while(sessionID.equals(prevSessionID) && curQueryIndex <=upperQueryIndex) { // iterates over all queries in a session, terminates on new session
				if(query.toLowerCase().startsWith("select") || query.toLowerCase().startsWith("insert") || query.toLowerCase().startsWith("update") || query.toLowerCase().startsWith("delete")) {
					if (!query.equals("SELECT VERSION()"))
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
//					System.out.println("Query: "+query);
					query = query.trim();
				}
				curQueryIndex++;
			}
			if(!sessionID.equals(prevSessionID)) {
				prevSessionID = sessionID;
			} 
			//curQueryIndex = populateCurSessionQueries(curQueryIndex, sessionID, prevSessionID, curSessQueries); // index incremented within this method
			// if sessQueries is not empty, check for repetitions
			boolean validSess = isValidSession(curSessQueries);
			if(validSess) {
			//	System.out.println("Session "+sessionID+"'s validity: "+validSess);
				numValidSessions++;
				numValidQueries+=curSessQueries.size();
				absQueryID = appendToValidSessFile(absSessID, absQueryID, bw, curSessQueries);
				//absQueryID = createSessQueryBitVectors(sessionID, absQueryID, bw, curSessQueries);
				absSessID++;
			}
			curSessQueries.clear();
		}
		System.out.println("Total # Valid Sessions: "+numValidSessions+", # Valid Queries: "+numValidQueries);
	}
	
	
	public void run(){
		try {
			if (pruneKeepModifyRepeatedQueries.equals("KEEP") || pruneKeepModifyRepeatedQueries.equals("MODIFY")) {	
				System.out.println(pruneKeepModifyRepeatedQueries);
				processQueriesKeepOrModifyReps();
			}
			else if(pruneKeepModifyRepeatedQueries.equals("PRUNE")) {
				System.out.println(pruneKeepModifyRepeatedQueries);
				processQueriesPruneReps();
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
