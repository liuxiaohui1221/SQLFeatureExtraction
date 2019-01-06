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

import javafx.util.Pair;

public class IntentCreatorMultiThread extends Thread{
	ArrayList<String> sessQueries;
	Pair<Integer,Integer> lowerUpperIndexBounds;
	String outputFile;
	SchemaParser schParse;
	int threadID;
	
	public IntentCreatorMultiThread(int threadID, ArrayList<String> sessQueries, Pair<Integer,Integer> lowerUpperIndexBounds, String outputFile, SchemaParser schParse) {
		this.sessQueries = sessQueries;
		this.lowerUpperIndexBounds = lowerUpperIndexBounds;
		this.outputFile = outputFile;
		this.schParse = schParse;
		this.threadID = threadID;
	}
	
	public void run(){
		try {
			MINCFragmentIntent.deleteIfExists(this.outputFile);
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true));
			double absQueryID = 0;
			int queryID = 0;
			String prevSessionID = "";
			String concLine = "";
			int lowerIndex = this.lowerUpperIndexBounds.getKey();
			int upperIndex = this.lowerUpperIndexBounds.getValue();
			System.out.println("Initialized Thread ID: "+this.threadID);
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
								queryID = 0;
								prevSessionID = sessionID;
							} else
								queryID++;
							absQueryID++;
							String to_append = "Session "+sessionID+", Query "+queryID+"; OrigQuery: "+query+";"+fragmentObj.getIntentBitVector()+"\n";
							concLine += to_append;
							if(absQueryID % 100 == 0) {
								bw.append(concLine);
								bw.flush();
								concLine = "";
//								System.out.println("Query: "+query);
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
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
