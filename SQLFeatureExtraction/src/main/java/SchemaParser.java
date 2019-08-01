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
import java.util.Arrays;
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
	HashMap<String,Integer> MINCColBitPos = new HashMap<String,Integer>();
	HashMap<String,ArrayList<String>> MINCJoinPreds = new HashMap<String,ArrayList<String>>();
	HashMap<String,Pair<Integer, Integer>> MINCJoinPredBitPos = new HashMap<String,Pair<Integer, Integer>>();
	HashMap<String,Integer> MINCSelPredCols = new HashMap<String,Integer>();
	HashMap<String,Pair<Integer,Integer>> MINCSelPredOpBitPos = new HashMap<String,Pair<Integer,Integer>>();
	HashMap<String,Pair<Integer,Integer>> MINCSelPredColRangeBitPos = new HashMap<String,Pair<Integer,Integer>>();
	HashMap<String,ArrayList<Pair<String, String>>> MincSelPredColRangeBins = new HashMap<String,ArrayList<Pair<String, String>>>();
	int MINCJoinPredBitCount;
	int MINCSelPredOpBitCount;
	int MINCSelPredColRangeBitCount;
	
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
	
	public HashMap<String,Integer> fetchMINCColBitPos(){
		return this.MINCColBitPos;
	}
	
	public HashMap<String,ArrayList<String>> fetchMINCJoinPreds(){
		return this.MINCJoinPreds;
	}
	
	public HashMap<String,Pair<Integer, Integer>> fetchMINCJoinPredBitPos(){
		return this.MINCJoinPredBitPos;
	}
	
	public HashMap<String,Integer> fetchMINCSelPredCols(){
		return this.MINCSelPredCols;
	}
	
	public HashMap<String,ArrayList<Pair<String, String>>> fetchMINCSelPredColRangeBins(){
		return this.MincSelPredColRangeBins;
	}
	
	public HashMap<String,Pair<Integer,Integer>> fetchMINCSelPredColRangeBitPos(){
		return this.MINCSelPredColRangeBitPos;
	}
	
	public HashMap<String,Pair<Integer,Integer>> fetchMINCSelPredOpBitPos(){
		return this.MINCSelPredOpBitPos;
	}
	
	public int fetchMINCSelPredOpBitCount() {
		return this.MINCSelPredOpBitCount;
	}
	
	public int fetchMINCSelPredColRangeBitCount() {
		return this.MINCSelPredColRangeBitCount;
	}
	
	public int fetchMINCJoinPredBitCount() {
		return this.MINCJoinPredBitCount;
	}
		
	public void readIntoJoinPredDict(String fn, HashMap<String,ArrayList<String>> MINCJoinPredDict) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fn)); 
			String st; 
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String key = st.split(":")[0];
				String right = st.split(":")[1].replace("[", "").replace("]", "");
				ArrayList<String> joinPredVal = new ArrayList<String>();
				String[] joinPreds = right.split("', '");
				//String[] joinPreds = right.split(",(?=([^\']*\'[^\']*\')*[^\']*$)");
				for(String joinPred : joinPreds) {
					joinPred = joinPred.replace("'", "");
					//Pair<String,String> colPair = new Pair<>(joinPred.split(",")[0], joinPred.split(",")[1]);
					joinPredVal.add(joinPred);
				}
				MINCJoinPredDict.put(key, joinPredVal);
			} 
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public int readIntoBitPosDict(String fn, HashMap<String,Pair<Integer,Integer>> MINCBitPosDict) {
		int numBits = 0;
		try {
			BufferedReader br = new BufferedReader(new FileReader(fn)); 
			String st;
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String key = st.split(":")[0];
				String right = st.split(":")[1];
				String[] bitPos = right.split(",");
				int startPos = Integer.parseInt(bitPos[0]);
				int endPos = Integer.parseInt(bitPos[1]);
				if(endPos+1 > numBits) {
					numBits = endPos+1; // because bitPos starts from 0
				}
				Pair<Integer, Integer> startEndBitPos = new Pair<>(startPos, endPos);
				MINCBitPosDict.put(key, startEndBitPos);
			} 
		}
		catch(Exception e) {
			e.printStackTrace();
		}
		return numBits;
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
	
	public void readIntoMincColBitPos(String fn) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fn)); 
			String st; 
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String key = st.split(":")[0];
				int val = Integer.parseInt(st.split(":")[1]);
				this.MINCColBitPos.put(key, val);
			//	this.MINCTableOrder.put(val,  key);
			} 
			this.MINCColBitPos = Util.sortByValue(this.MINCColBitPos);
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void readIntoSelPredColRangeBins(String fn, HashMap<String,ArrayList<Pair<String,String>>> MINCSelPredColRangeBins) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fn)); 
			String st; 
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String key = st.split(":")[0];
				String[] subArr = Arrays.copyOfRange(st.split(":"), 1, st.split(":").length);
				String right = String.join(":", subArr).replace("[", "").replace("]", "");
				ArrayList<Pair<String,String>> dictVal = new ArrayList<Pair<String,String>>();
				String[] selPredColRangeBins = right.split("', '");
				//String[] selPredColRangeBins = right.split(",(?=([^\']*\'[^\']*\')*[^\']*$)");
				for(String selPredColRangeBin : selPredColRangeBins) {
					selPredColRangeBin = selPredColRangeBin.replace("'", "");
					Pair<String,String> colPair = new Pair<>(selPredColRangeBin.split("%")[0], selPredColRangeBin.split("%")[1]);
					dictVal.add(colPair);
				}
				MINCSelPredColRangeBins.put(key, dictVal);
			} 
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void readIntoSelPredOpBitPos(String fn, HashMap<String,Pair<Integer,Integer>> MINCSelPredOpBitPos) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fn)); 
			String st;
			this.MINCSelPredOpBitCount = 0;
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String key = st.split(":")[0];
				String right = st.split(":")[1];
				String[] bitPos = right.split(",");
				int startPos = Integer.parseInt(bitPos[0]);
				int endPos = Integer.parseInt(bitPos[1]);
				if(endPos > this.MINCSelPredOpBitCount) {
					this.MINCSelPredOpBitCount = endPos+1; // because bitPos starts from 0
				}
				Pair<Integer, Integer> startEndBitPos = new Pair<>(startPos, endPos);
				MINCSelPredOpBitPos.put(key, startEndBitPos);
			} 
		}
		catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void readIntoSelPredColDict(String fn) {
		try {
			BufferedReader br = new BufferedReader(new FileReader(fn)); 
			String st;
			int selColIndex;
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String selColName = st.split(":")[0];
				selColIndex = Integer.parseInt(st.split(":")[1]);
				this.MINCSelPredCols.put(selColName, selColIndex);
			}
			this.MINCSelPredCols = Util.sortByValue(this.MINCSelPredCols);
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public void fetchSchemaElements() {
		for(String key:configDict.keySet()) {
			if(key.equals("MINC_TABLES"))
				readIntoTables(configDict.get(key));
			else if(key.equals("MINC_COLS"))
				readInto(configDict.get(key),this.MINCColumns);
			else if(key.equals("MINC_COL_TYPES"))
				readInto(configDict.get(key),this.MINCColTypes);
			else if(key.equals("MINC_COL_BIT_POS"))
				readIntoMincColBitPos(configDict.get(key));
			else if(key.equals("MINC_JOIN_PREDS"))
				readIntoJoinPredDict(configDict.get(key),this.MINCJoinPreds);
			else if(key.equals("MINC_JOIN_PRED_BIT_POS"))
				this.MINCJoinPredBitCount = readIntoBitPosDict(configDict.get(key),this.MINCJoinPredBitPos);
			else if(key.equals("MINC_SEL_PRED_COLS"))
				readIntoSelPredColDict(configDict.get(key));
			else if(key.equals("MINC_SEL_PRED_COL_RANGE_BINS"))
				readIntoSelPredColRangeBins(configDict.get(key),this.MincSelPredColRangeBins);
			else if(key.equals("MINC_SEL_PRED_COL_RANGE_BIT_POS")) 
				this.MINCSelPredColRangeBitCount = readIntoBitPosDict(configDict.get(key),this.MINCSelPredColRangeBitPos);
			else if(key.equals("MINC_SEL_PRED_OP_BIT_POS"))
				this.MINCSelPredOpBitCount = readIntoBitPosDict(configDict.get(key),this.MINCSelPredOpBitPos);	
		}
	}
	
	public void fetchSchema(String configFile) {
		try {
			configDict = new HashMap<String, String>();
			BufferedReader br = new BufferedReader(new FileReader(configFile)); 
			String st; 
			String homeDir = System.getProperty("user.home");
			if(MINCFragmentIntent.getMachineName().contains("4119510") || MINCFragmentIntent.getMachineName().contains("4119509")
					|| MINCFragmentIntent.getMachineName().contains("4119508") || MINCFragmentIntent.getMachineName().contains("4119507")) {
				homeDir = "/hdd2/vamsiCodeData"; // comment it when you are not running on EN4119510L.cidse.dhcp.adu.edu
			}
			while ((st = br.readLine()) != null) {
				st = st.trim();
				String key = st.split("=")[0];
				String val = homeDir+"/"+st.split("=")[1];
				if(key.contains("MINC_NUM_THREADS") || key.contains("MINC_START_SESS_INDEX") || key.contains("MINC_SEL_OP_CONST")
						|| key.contains("MINC_START_LINE_NUM") || key.contains("MINC_KEEP_PRUNE_MODIFY_REPEATED_QUERIES"))
					val = st.split("=")[1];
				configDict.put(key, val);
			} 
			fetchSchemaElements();
			// System.out.println("Fetched schema elements !");
		}
		catch(Exception e) {
			e.printStackTrace();
		}	
	}
	
	public static void main(String[] args) {
		String homeDir = System.getProperty("user.home");
		System.out.println(MINCFragmentIntent.getMachineName());
		if(MINCFragmentIntent.getMachineName().contains("4119510") || MINCFragmentIntent.getMachineName().contains("4119509")
				|| MINCFragmentIntent.getMachineName().contains("4119508") || MINCFragmentIntent.getMachineName().contains("4119507")) {
			homeDir = "/hdd2/vamsiCodeData"; // comment it when you are not running on EN4119510L.cidse.dhcp.adu.edu
		}
		String configFile = homeDir+"/Documents/DataExploration-Research/MINC/InputOutput/MincJavaConfig.txt";
		SchemaParser schParse = new SchemaParser();
		schParse.fetchSchema(configFile);
	}
}
