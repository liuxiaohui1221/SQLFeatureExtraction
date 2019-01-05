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
	ArrayList<String> inputLines;
	String outputFile;
	SchemaParser schParse;
	
	public IntentCreatorMultiThread(ArrayList<String> inputLines, String outputFile, SchemaParser schParse) {
		this.inputLines = inputLines;
		this.outputFile = outputFile;
		this.schParse = schParse;
	}
	
	public void run(){
		try {
			MINCFragmentIntent.deleteIfExists(this.outputFile);
			BufferedWriter bw = new BufferedWriter(new FileWriter(outputFile, true));
			double absQueryID = 0;
			int queryID = 0;
			String prevSessionID = "";
			String concLine = "";
			for(String line:this.inputLines) {
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
					
					MINCFragmentIntent fragmentObj = new MINCFragmentIntent(query, this.schParse);
					try {
						boolean validQuery = fragmentObj.parseQueryAndCreateFragmentVectors();
						/*if(validQuery)
							fragmentObj.printIntentVector();*/
						if(validQuery) {
							String sessionID = tokens[0];
							if(!sessionID.equals(prevSessionID)) {
								queryID = 0;
								prevSessionID = sessionID;
							} else
								queryID++;
							absQueryID++;
							if(absQueryID % 100000 == 0) {
							//	System.out.println("Query: "+query);
								System.out.println("Covered SessionID: "+sessionID+", queryID: "+queryID+", absQueryID: "+absQueryID);
							}
							String to_append = "Session "+sessionID+", Query "+queryID+"; OrigQuery: "+query+";"+fragmentObj.getIntentBitVector()+"\n";
							concLine += to_append;
							if(absQueryID % 10000 == 0) {
								bw.append(concLine);
								concLine = "";
							}
						}
					} catch(Exception e) {
						continue;
					}
				}
			}
			if(!concLine.equals(""))
				bw.append(concLine);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
}
