import java.io.BufferedWriter;
import java.io.FileWriter;
import java.util.ArrayList;

import org.simmetrics.StringMetric;
import org.simmetrics.metrics.StringMetrics;
import toolsForMetrics.Pair;

public class IntentCreatorMultiThread extends Thread{
	ArrayList<String> sessQueries;
	Pair<Integer,Integer> lowerUpperIndexBounds;
	String outputFile;
	SchemaParser schParse;
	int threadID;
	String pruneKeepModifyRepeatedQueries;
	String prevQueryBitVector = null;
	boolean includeSelOpConst;
	String dataset;
	
	public IntentCreatorMultiThread(String dataset, int threadID, ArrayList<String> sessQueries, Pair<Integer,Integer> lowerUpperIndexBounds,
			String outputFile, SchemaParser schParse, String pruneKeepModifyRepeatedQueries, boolean includeSelOpConst) {
		this.sessQueries = sessQueries;
		this.lowerUpperIndexBounds = lowerUpperIndexBounds;
		this.outputFile = outputFile;
		this.schParse = schParse;
		this.threadID = threadID;
		this.pruneKeepModifyRepeatedQueries = pruneKeepModifyRepeatedQueries;
		this.includeSelOpConst = includeSelOpConst;
		this.dataset = dataset;
	}
	
	public void processQueriesPreprocess() throws Exception{
		//	assert (pruneKeepModifyRepeatedQueries.equals("KEEP") || pruneKeepModifyRepeatedQueries.equals("MODIFY"));
			MINCFragmentIntent.deleteIfExists(this.outputFile);
			BufferedWriter bw = new BufferedWriter(new FileWriter(this.outputFile, true));
			double absQueryID = 0;
			int queryID = 0;
			String prevSessionID = "";
			String concLine = "";
			int lowerIndex = this.lowerUpperIndexBounds.first;
			int upperIndex = this.lowerUpperIndexBounds.second;
			System.out.println("Initialized Thread ID: "+this.threadID+" with outputFile "+this.outputFile);
			for(int index = lowerIndex; index <= upperIndex; index++) {
				String line = this.sessQueries.get(index);
				String[] tokens = line.trim().split(";(?=(?:[^\']*\'[^\']*\')*[^\']*$)");
				String query = tokens[1].split(": ")[1];
			//	System.out.println("Query: "+query);
				String sessionID = tokens[0].split(", ")[0].split(" ")[1];
				query = query.trim();
				boolean validQuery = false;
				if(query.toLowerCase().startsWith("select") || query.toLowerCase().startsWith("insert") || query.toLowerCase().startsWith("update") || query.toLowerCase().startsWith("delete")) {
					validQuery = true;
				}					
				try {						
					MINCFragmentIntent fragmentObj = null;
					if(validQuery) {
						fragmentObj = new MINCFragmentIntent(query, this.schParse, this.includeSelOpConst, this.dataset);
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
						if(!sessionID.equals(prevSessionID)) {
							queryID = 1;  // queryID starts with 1 not 0
							prevSessionID = sessionID;
						} 
						queryID++;
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
			if(!concLine.equals("")) {
				bw.append(concLine);
				bw.flush();
			}
		}
	
	public String fetchMINCQueryFromLine(String line) throws Exception {
		String[] tokens = line.trim().split(" ");
		String query = "";
		for(int i=2; i<tokens.length; i++) {
			if(i==2)
				query = tokens[i];
			else
				query += " "+tokens[i];
		}
		return query;
	}
	
	public String fetchBusTrackerQueryFromLine(String line) throws Exception {
		String regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"; // to split on comma outside double quotes
		String[] tokens = line.split(regex);
		// format is "startTime","sessID","endTime","execute <unnamed>: Query","parameters: $1 = ..., $2 = ..."
		String query = tokens[3].split(": ")[1];
		query = query.replaceAll("^\"|\"$", ""); // remove starting and trailing double quotes
		return query;
	}
	
	public String fetchQueryFromLine(String line) throws Exception {
		if(this.dataset.equals("MINC")) 
			return fetchMINCQueryFromLine(line);
		else if(this.dataset.equals("BusTracker"))
			return fetchBusTrackerQueryFromLine(line);
		return null;
	}
		
	
	public void processQueriesKeepReps() throws Exception{
	//	assert (pruneKeepModifyRepeatedQueries.equals("KEEP") || pruneKeepModifyRepeatedQueries.equals("MODIFY_INTENT"));
		MINCFragmentIntent.deleteIfExists(this.outputFile);
		BufferedWriter bw = new BufferedWriter(new FileWriter(this.outputFile, true));
		double absQueryID = 0;
		int queryID = 0;
		String prevSessionID = "";
		String concLine = "";
		int lowerIndex = this.lowerUpperIndexBounds.first;
		int upperIndex = this.lowerUpperIndexBounds.second;
		System.out.println("Initialized Thread ID: "+this.threadID+" with outputFile "+this.outputFile);
		for(int index = lowerIndex; index <= upperIndex; index++) {
			String line = this.sessQueries.get(index);
			boolean condToHold = (this.dataset.equals("MINC") && line.contains("Query")) || this.dataset.equals("BusTracker");
			if((pruneKeepModifyRepeatedQueries.equals("KEEP") || pruneKeepModifyRepeatedQueries.equals("MODIFY")) && condToHold) {
				String query = fetchQueryFromLine(line);
		//		System.out.println("Query: "+query);
				query = query.trim();
				boolean validQuery = false;
				if(query.toLowerCase().startsWith("select") || query.toLowerCase().startsWith("insert") || query.toLowerCase().startsWith("update") || query.toLowerCase().startsWith("delete")) {
					validQuery = true;
				}					
				try {						
					MINCFragmentIntent fragmentObj = null;
					if(validQuery) {
						fragmentObj = new MINCFragmentIntent(query, this.schParse, this.includeSelOpConst, this.dataset);
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
						String sessionID = MINCFragmentIntent.fetchSessID(this.dataset, line);
						if(!sessionID.equals(prevSessionID)) {
							queryID = 1;  // queryID starts with 1 not 0
							prevSessionID = sessionID;
						} 
						else if (pruneKeepModifyRepeatedQueries.equals("KEEP"))
							queryID++;
						else if(pruneKeepModifyRepeatedQueries.equals("MODIFY_INTENT")) { // for future use to handle repetition in intent vectors
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
	
	public boolean hasRecurringQueries(ArrayList<String> sessQueries) {
		// Here we manually detected query sub-sequences which are highly repetitive and pruned them 
		if(this.dataset.equals("MINC")) {
			if(sessQueries.size()==2 && sessQueries.get(0).contains("SELECT * FROM jos_session WHERE session_id =") && 
					sessQueries.get(1).contains("UPDATE `jos_session` SET `time`="))
				return true;
			else if(sessQueries.size()>=3 && sessQueries.get(0).contains("SELECT * FROM jos_session WHERE session_id =") &&
					sessQueries.get(1).contains("DELETE FROM jos_session WHERE ( time <")  &&
					sessQueries.get(2).contains("SELECT * FROM jos_session WHERE session_id ="))
				return true;
		}
	/*	else if(this.dataset.equals("BusTracker")) {
			if(sessQueries.size()==2 && sessQueries.get(0).contains("SELECT DISTINCT agency_timezone FROM agency WHERE agency_id = $1") &&
					sessQueries.get(1).contains("select st.trip_id, st.stop_sequence, st.estimate_source, st.fullness, st.departure_time_hour, "
							+ "st.departure_time_minute, s.stop_lat, s.stop_lon, t.direction_id, t.route_id, r.route_short_name from stop AS s RIGHT JOIN stop_time AS st"))
				return true;
			else if(sessQueries.size()==2 && sessQueries.get(1).contains("SELECT DISTINCT agency_timezone FROM agency WHERE agency_id = $1") &&
					sessQueries.get(0).contains("select st.trip_id, st.stop_sequence, st.estimate_source, st.fullness, st.departure_time_hour, "
							+ "st.departure_time_minute, s.stop_lat, s.stop_lon, t.direction_id, t.route_id, r.route_short_name from stop AS s RIGHT JOIN stop_time AS st"))
				return true;
			else if(sessQueries.size()>=3 && sessQueries.get(0).contains("SELECT DISTINCT agency_timezone FROM agency WHERE agency_id = $1") &&
					sessQueries.get(1).contains("select st.trip_id, st.stop_sequence, st.estimate_source, st.fullness, st.departure_time_hour, "
							+ "st.departure_time_minute, s.stop_lat, s.stop_lon, t.direction_id, t.route_id, r.route_short_name from stop AS s RIGHT JOIN stop_time AS st")  &&
					sessQueries.get(2).contains("SELECT DISTINCT agency_timezone FROM agency WHERE agency_id = $1"))
				return true;
			else if(sessQueries.size()>=3 && sessQueries.get(1).contains("SELECT DISTINCT agency_timezone FROM agency WHERE agency_id = $1") &&
					sessQueries.get(0).contains("select st.trip_id, st.stop_sequence, st.estimate_source, st.fullness, st.departure_time_hour, "
							+ "st.departure_time_minute, s.stop_lat, s.stop_lon, t.direction_id, t.route_id, r.route_short_name from stop AS s RIGHT JOIN stop_time AS st")  &&
					sessQueries.get(2).contains("select st.trip_id, st.stop_sequence, st.estimate_source, st.fullness, st.departure_time_hour, "
							+ "st.departure_time_minute, s.stop_lat, s.stop_lon, t.direction_id, t.route_id, r.route_short_name from stop AS s RIGHT JOIN stop_time AS st"))
				return true;
			for(int i=0; i<sessQueries.size(); i++) {
				if(i>=2 && sessQueries.get(i-1).contains("SELECT DISTINCT agency_timezone FROM agency WHERE agency_id = $1") &&
					sessQueries.get(i).contains("select st.trip_id, st.stop_sequence, st.estimate_source, st.fullness, st.departure_time_hour, "
							+ "st.departure_time_minute, s.stop_lat, s.stop_lon, t.direction_id, t.route_id, r.route_short_name from stop AS s RIGHT JOIN stop_time AS st")  &&
					sessQueries.get(i-2).contains("select st.trip_id, st.stop_sequence, st.estimate_source, st.fullness, st.departure_time_hour, "
							+ "st.departure_time_minute, s.stop_lat, s.stop_lon, t.direction_id, t.route_id, r.route_short_name from stop AS s RIGHT JOIN stop_time AS st"))
					return true;
			}
		} */
		return false;
	}
	
	public boolean isValidSession(ArrayList<String> sessQueries) {
		if((sessQueries.size()>=0 && sessQueries.size()<=1) || sessQueries.size()>=50) {
	//		System.out.println("Session Empty !");
			return false;
		} 
		else if(hasRecurringQueries(sessQueries)) {
			return false;
		}
		boolean isValid = true;
		String prevSessQuery = null;
		for(String curSessQuery : sessQueries) {
			if(!isValid)
				return false;
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
				fragmentObj = new MINCFragmentIntent(query, this.schParse, this.includeSelOpConst, this.dataset);
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
	
	public void processQueriesPruneOrModifyReps() throws Exception{
		MINCFragmentIntent.deleteIfExists(this.outputFile);
		BufferedWriter bw = new BufferedWriter(new FileWriter(this.outputFile, true));
		double absQueryID = 0;
		double absSessID = 0;
		String prevSessionID = "";
		String sessionID = "";
		String query = "";
		int lowerQueryIndex = this.lowerUpperIndexBounds.first;
		int upperQueryIndex = this.lowerUpperIndexBounds.second;
		System.out.println("Initialized Thread ID: "+this.threadID+" with outputFile "+this.outputFile);
		int curQueryIndex = lowerQueryIndex;
		ArrayList<String> curSessQueries = new ArrayList<String>();
		int numValidSessions = 0;
		int numValidQueries = 0;
		while(curQueryIndex <= upperQueryIndex) {
			while(sessionID.equals(prevSessionID) && curQueryIndex <=upperQueryIndex) { // iterates over all queries in a session, terminates on new session
				boolean condToHold;
				if(query.toLowerCase().startsWith("select") || query.toLowerCase().startsWith("insert") || query.toLowerCase().startsWith("update") || query.toLowerCase().startsWith("delete")) {
					condToHold = (this.dataset.equals("BusTracker") && !query.toLowerCase().equals("select 1"))|| (this.dataset.equals("MINC") && !query.equals("SELECT VERSION()") && !query.equals("SELECT f.id"));
					if(condToHold)
						curSessQueries.add(query);
				}
				//read queries session-wise
				String line = this.sessQueries.get(curQueryIndex);
				condToHold = (this.dataset.equals("MINC") && line.contains("Query")) || this.dataset.equals("BusTracker");
				if(condToHold) {
					query = fetchQueryFromLine(line);
//					System.out.println("Query: "+query);
					query = query.trim();
					sessionID = MINCFragmentIntent.fetchSessID(this.dataset, line);
				}
				curQueryIndex++;
			}
			if(!sessionID.equals(prevSessionID)) {
				prevSessionID = sessionID;
			} 
			//curQueryIndex = populateCurSessionQueries(curQueryIndex, sessionID, prevSessionID, curSessQueries); // index incremented within this method
			// if modifyFlag set first condense repetitions to a single occurrence and still prune invalid sessions
			if(pruneKeepModifyRepeatedQueries.equals("MODIFY"))
				curSessQueries = modifyQueries(curSessQueries);
			// if sessQueries is not empty, check for repetitions
			boolean validSess = isValidSession(curSessQueries);
			if(validSess) {
			//	System.out.println("Session "+sessionID+"'s validity: "+validSess);
				numValidSessions++;
				numValidQueries+=curSessQueries.size();
				//if(pruneKeepModifyRepeatedQueries.equals("PRUNE") || pruneKeepModifyRepeatedQueries.equals("MODIFY"))
				absQueryID = appendToValidSessFile(absSessID, absQueryID, bw, curSessQueries);
				//absQueryID = createSessQueryBitVectors(sessionID, absQueryID, bw, curSessQueries);
				absSessID++;
			}
			curSessQueries.clear();
		}
		System.out.println("Total # Valid Sessions: "+numValidSessions+", # Valid Queries: "+numValidQueries);
	}
	
	public ArrayList<String> modifyQueries(ArrayList<String> curSessQueries) throws Exception {
		ArrayList<String> modifiedQueries = new ArrayList<>(curSessQueries);
		String prevSessQuery = null;
		for(int i=0; i<curSessQueries.size(); i++) {
			if(prevSessQuery != null) {
				StringMetric metric = StringMetrics.cosineSimilarity();
				String curSessQuery = curSessQueries.get(i);
				float cosineSim = metric.compare(curSessQuery, prevSessQuery);
				if(cosineSim< 0.8) {
					prevSessQuery = curSessQuery;
					modifiedQueries.add(curSessQuery);
				}
			} else {
				prevSessQuery = curSessQueries.get(i);
				modifiedQueries.add(prevSessQuery);
			}
		}
		curSessQueries.clear();
		return modifiedQueries;
	}
	
	
	
	public void run(){
		try {
			if (pruneKeepModifyRepeatedQueries.equals("KEEP") || pruneKeepModifyRepeatedQueries.equals("MODIFY_INTENT")) {	
				System.out.println(pruneKeepModifyRepeatedQueries);
				processQueriesKeepReps();
			}
			else if(pruneKeepModifyRepeatedQueries.equals("MODIFY") || pruneKeepModifyRepeatedQueries.equals("PRUNE")) {
				System.out.println(pruneKeepModifyRepeatedQueries);
				processQueriesPruneOrModifyReps();
			}
			else if(pruneKeepModifyRepeatedQueries.equals("PREPROCESS")) {
				System.out.println(pruneKeepModifyRepeatedQueries);
				processQueriesPreprocess();
			}
			
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
