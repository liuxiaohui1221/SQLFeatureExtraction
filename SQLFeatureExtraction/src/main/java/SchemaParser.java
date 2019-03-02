import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.FileReader;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeSet;
import toolsForMetrics.Util;
import javafx.util.Pair;
import java.net.InetAddress;
import java.net.UnknownHostException;
public class SchemaParser {
	//following are the schema data structures
	HashMap<String,Integer> MINCTables = new HashMap<String,Integer>();
//	HashMap<Integer, String> MINCTableOrder = new HashMap<Integer,String>();
	HashMap<String,String> MINCColumns = new HashMap<String,String>();
	HashMap<String,String> MINCColTypes = new HashMap<String,String>();
	HashMap<String,ArrayList<Pair<String, String>>> MINCJoinPreds = new HashMap<String,ArrayList<Pair<String, String>>>();
	HashMap<String,Pair<Integer, Integer>> MINCJoinPredBitPos = new HashMap<String,Pair<Integer, Integer>>();
	int MINCJoinPredBitCount;
	HashMap<String, String> configDict = new HashMap<String, String>();
	
	public HashMap<String, String> getConfigDict(){
		return this.configDict;
	}
	
	public HashMap<String,Integer> fetchMINCTables(){
		return this.MINCTables;
	}
	
/*	public HashMap<Integer,String> fetchMINCTableOrder(){
		return this.MINCTableOrder;
	}
*/	
	public HashMap<String,String> fetchMINCColumns(){
		return this.MINCColumns;
	}
	
	public HashMap<String,String> fetchMINCColTypes(){
		return this.MINCColTypes;
	}
	
	public HashMap<String,ArrayList<Pair<String,String>>> fetchMINCJoinPreds(){
		return this.MINCJoinPreds;
	}
	
	public HashMap<String,Pair<Integer, Integer>> fetchMINCJoinPredBitPos(){
		return this.MINCJoinPredBitPos;
	}
	
	public int fetchMINCJoinPredBitCount() {
		return this.MINCJoinPredBitCount;
	}
	
	public void readIntoJoinPredDict(String fn, HashMap<String,ArrayList<Pair<String,String>>> MINCJoinPredDict) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fn)); 
			String st; 
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String key = st.split(":")[0];
				String right = st.split(":")[1].replace("[", "").replace("]", "");
				ArrayList<Pair<String,String>> joinPredVal = new ArrayList<Pair<String,String>>();
				String[] joinPreds = right.split(",(?=([^\']*\'[^\']*\')*[^\']*$)");
				for(String joinPred : joinPreds) {
					joinPred = joinPred.replace("'", "");
					Pair<String,String> colPair = new Pair<>(joinPred.split(",")[0], joinPred.split(",")[1]);
					joinPredVal.add(colPair);
				}
				MINCJoinPredDict.put(key, joinPredVal);
			} 
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void readIntoJoinPredBitPos(String fn, HashMap<String,Pair<Integer,Integer>> MINCJoinPredBitPos) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fn)); 
			String st;
			this.MINCJoinPredBitCount = 0;
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String key = st.split(":")[0];
				String right = st.split(":")[1];
				String[] bitPos = right.split(",");
				int startPos = Integer.parseInt(bitPos[0]);
				int endPos = Integer.parseInt(bitPos[1]);
				if(endPos > this.MINCJoinPredBitCount) {
					this.MINCJoinPredBitCount = endPos+1; // because bitPos starts from 0
				}
				Pair<Integer, Integer> startEndBitPos = new Pair<>(startPos, endPos);
				MINCJoinPredBitPos.put(key, startEndBitPos);
			} 
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void readInto(String fn, HashMap<String,String> MINCMap) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fn)); 
			String st; 
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String key = st.split(":")[0];
				String val = st.split(":")[1];
				MINCMap.put(key, val);
			} 
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void readIntoTables(String fn) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fn)); 
			String st; 
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String key = st.split(":")[0];
				int val = Integer.parseInt(st.split(":")[1]);
				this.MINCTables.put(key, val);
			//	this.MINCTableOrder.put(val,  key);
			} 
			this.MINCTables = Util.sortByValue(this.MINCTables);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void fetchSchemaElements() {
		for(String key:configDict.keySet()) {
			if(key.equals("MINC_TABLES"))
				readIntoTables(configDict.get(key));
			else if(key.equals("MINC_COLS"))
				readInto(configDict.get(key),MINCColumns);
			else if(key.equals("MINC_COL_TYPES"))
				readInto(configDict.get(key),MINCColTypes);
			else if(key.equals("MINC_JOIN_PREDS"))
				readIntoJoinPredDict(configDict.get(key),MINCJoinPreds);
			else if(key.equals("MINC_JOIN_PRED_BIT_POS"))
				readIntoJoinPredBitPos(configDict.get(key),MINCJoinPredBitPos);
		}
	}
	
	public void fetchSchema(String configFile) {
		try {
			configDict = new HashMap<String, String>();
			BufferedReader br = new BufferedReader(new FileReader(configFile)); 
			String st; 
			String homeDir = System.getProperty("user.home");
			if(MINCFragmentIntent.getMachineName().contains("4119510"))
				homeDir = "/hdd2/vamsiCodeData"; // comment it when you are not running on EN4119510L.dhcp.cidse.adu.edu
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String key = st.split("=")[0];
				String val = homeDir+"/"+st.split("=")[1];
				if(key.contains("MINC_NUM_THREADS"))
					val = st.split("=")[1];
				configDict.put(key, val);
			} 
			fetchSchemaElements();
		}
		catch(Exception e) {
			e.printStackTrace();
		}	
	}
	
	public static void Main(String[] args) {
		String configFile = "config.txt";
		SchemaParser schParse = new SchemaParser();
		schParse.fetchSchema(configFile);
	}
}
