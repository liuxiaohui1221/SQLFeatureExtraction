package sql.encoder;

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
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import sql.clickhouse.SchemaParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
//import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.update.Update;
import sql.toolsForMetrics.Global;
import sql.toolsForMetrics.Pair;
import sql.toolsForMetrics.Util;
//import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.parser.CCJSqlParser;

import java.net.InetAddress;

public class MINCFragmentIntent{
	String originalSQL;
	Statement statement;
	String intentBitVector;
	StringBuilder intentBitVecBuilder;
	List<Table> tables = new ArrayList<Table>();
	HashMap<String, Column> colAliases = null;
	HashSet<Column> groupByColumns = new HashSet<Column>();
	HashSet<Column> selectionColumns = new HashSet<Column>();
	HashSet<Column> havingColumns = new HashSet<Column>();
	HashSet<Column> orderByColumns = new HashSet<Column>();
	HashSet<Column> projectionColumns = new HashSet<Column>();
	HashSet<ArrayList<Column>> joinPredicates = new HashSet<ArrayList<Column>>();
	HashSet<String> limitList = new HashSet<String>();
	HashSet<Column> MINColumns = new HashSet<Column>();
	HashSet<Column> MAXColumns = new HashSet<Column>();
	HashSet<Column> AVGColumns = new HashSet<Column>();
	HashSet<Column> SUMColumns = new HashSet<Column>();
	HashSet<Column> COUNTColumns = new HashSet<Column>();
	HashMap<Column,ArrayList<String>> selPredOps;
	HashMap<Column,ArrayList<String>> selPredConstants;
	String queryTypeBitMap;
	String tableBitMap;
	String groupByBitMap;
	String selectionBitMap;
	String havingBitMap;
	String orderByBitMap;
	String projectionBitMap;
	String joinPredicatesBitMap;
	String limitBitMap;
	String MINBitMap;
	String MAXBitMap;
	String AVGBitMap;
	String SUMBitMap;
	String COUNTBitMap;
	String selPredOpBitMap;
	String[] selPredOpList = new String[] {"=", "<>", "<=", ">=", "<", ">", "LIKE"};
	String selPredColRangeBinBitMap;
	public static HashMap<String, String> tableAlias = Global.tableAlias;
	String queryType; // select , insert, update, delete
	SchemaParser schParse; // used for retrieval of schema related information
	boolean includeSelOpConst; // decide whether or not to include selOpConst
	String dataset; // dataset info
	public MINCFragmentIntent(String originalSQL, SchemaParser schParse, boolean includeSelOpConst, String dataset) throws Exception{
		this.originalSQL = originalSQL.toLowerCase();
		this.schParse = schParse;
		this.includeSelOpConst = includeSelOpConst;
		this.dataset = dataset;
		Global.tableAlias = new HashMap<String, String>();
		InputStream stream = new ByteArrayInputStream(this.originalSQL.getBytes(StandardCharsets.UTF_8));
		CCJSqlParser parser = new CCJSqlParser(stream);
		this.statement = null;
		this.intentBitVector = null;
		this.intentBitVecBuilder = new StringBuilder();
		try {
			this.statement = parser.Statement();
			if (this.statement instanceof Select) 
				this.queryType = "select";				
			else if(statement instanceof Update) 
				this.queryType = "update";
			else if(statement instanceof Insert) 
				this.queryType = "insert";
			else if(statement instanceof Delete)
				this.queryType = "delete";
		} catch (Exception e) {	
			e.printStackTrace();
		}
	}
	
	public static String getMachineName(){
		String hostname = null;
		try {
			InetAddress addr;
		    addr = InetAddress.getLocalHost();
		    hostname = addr.getHostName();
		} catch(Exception e) {
			e.printStackTrace();
		}
		return hostname;
	}
	
	public String getIntentBitVector() {
		return this.intentBitVector;
	}
	
	public String cleanString(String str) throws Exception{
		str = str.replace("'", "").replace("`", "").trim();
		str=str.replace("[", "");
		str = str.replace("]", "");
		return str;
	}
	
	public String toString(BitSet b, int size) throws Exception{
       String to_return = "";
       for(int i=0; i<size; i++) {
    	   		if(b.get(i))
    	   			to_return+="1";
    	   		else
    	   			to_return+="0";
       }
       return to_return;
    }
	
	public void writeIntentVectorToTempFile(String query) throws Exception{
		String fileName = "tempVector";
		this.deleteIfExists(fileName);
		BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, true));
		bw.append("query:"+query+"\n");
		bw.append("intentVector:"+this.intentBitVector+"\n");
		bw.append("queryTypeBitMap:"+this.queryTypeBitMap+"\n");
		bw.append("TableBitMap:"+this.tableBitMap+"\n");
		bw.append("SelectionBitMap:"+this.selectionBitMap+"\n");
		bw.append("GroupByBitMap:"+this.groupByBitMap+"\n");
		bw.append("OrderByBitMap:"+this.orderByBitMap+"\n");
		bw.append("ProjectionBitMap:"+this.projectionBitMap+"\n");
		bw.append("HavingBitMap:"+this.havingBitMap+"\n");
		bw.append("JoinPredicatesBitMap:"+this.joinPredicatesBitMap+"\n");
		bw.append("LimitBitMap:"+this.limitBitMap+"\n");
		bw.append("MINBitMap:"+this.MINBitMap+"\n");
		bw.append("MAXBitMap:"+this.MAXBitMap+"\n");
		bw.append("AVGBitMap:"+this.AVGBitMap+"\n");
		bw.append("SUMBitMap:"+this.SUMBitMap+"\n");
		bw.append("COUNTBitMap:"+this.COUNTBitMap+"\n");
		if(this.includeSelOpConst) {
			bw.append("selPredOpBitMap:"+this.selPredOpBitMap+"\n");
			bw.append("selPredColRangeBinBitMap:"+this.selPredColRangeBinBitMap+"\n");
		}
		bw.flush();
		bw.close();
	}
	
	public void printIntentVector() throws Exception{
		System.out.println("Printing FragmentBitVector");
		System.out.println(this.intentBitVector);
		System.out.println("----OPERATOR-WISE FRAGMENT------");
		System.out.println("queryTypeBitMap: "+this.queryTypeBitMap);
		System.out.println("TableBitMap: "+this.tableBitMap);
		System.out.println("TableBitMap: "+this.tableBitMap);
		System.out.println("SelectionBitMap:"+this.selectionBitMap);
		System.out.println("OrderByBitMap: "+this.orderByBitMap);
		System.out.println("ProjectionBitMap: "+this.projectionBitMap);
		System.out.println("HavingBitMap: "+this.havingBitMap);
		System.out.println("JoinPredicatesBitMap: "+this.joinPredicatesBitMap);
		System.out.println("LimitBitMap: "+this.limitBitMap);
		System.out.println("MINBitMap: "+this.MINBitMap);
		System.out.println("MAXBitMap: "+this.MAXBitMap);
		System.out.println("AVGBitMap: "+this.AVGBitMap);
		System.out.println("SUMBitMap: "+this.SUMBitMap);
		System.out.println("COUNTBitMap: "+this.COUNTBitMap);
		if(this.includeSelOpConst) {
			System.out.println("selPredOpBitMap:"+this.selPredOpBitMap);
			System.out.println("selPredColRangeBinBitMap:"+this.selPredColRangeBinBitMap);
		}
	}
	
	public void populateOperatorObjects(SQLParser parser) throws Exception{
		this.tables = parser.getTables();
		this.colAliases = parser.getColAliases();
		this.groupByColumns = parser.getGroupByColumns();
		this.selectionColumns = parser.getSelectionColumns();
		this.havingColumns = parser.getHavingColumns();
		this.orderByColumns = parser.getOrderByColumns();
		this.projectionColumns = parser.getProjectionColumns();
		this.joinPredicates = parser.getJoinPredicates();
		this.limitList = parser.getLimitList();
		this.MINColumns = parser.getMINColumns();
		this.MAXColumns = parser.getMAXColumns();
		this.AVGColumns = parser.getAVGColumns();
		this.SUMColumns = parser.getSUMColumns();
		this.COUNTColumns = parser.getCOUNTColumns();
		if(this.includeSelOpConst) {
			this.selPredOps = parser.getSelPredOps();
			this.selPredConstants = parser.getSelPredConstants();
		}
	}
	
	public void parseQuery() throws Exception {
		SQLParser parser = new SQLParser(this.schParse, this.originalSQL, this.includeSelOpConst, this.dataset);
		parser.createQueryVector(this.statement);
		populateOperatorObjects(parser);
	}
	
	public void createBitVectorForQueryTypes() throws Exception{
		BitSet b = new BitSet(4);
		if(this.queryType.equals("select"))
			b.set(0);
		else if(this.queryType.equals("update"))
			b.set(1);
		else if(this.queryType.equals("insert"))
			b.set(2);
		else if(this.queryType.equals("delete"))
			b.set(3);
		else {
			System.out.println("Invalid queryType!!");
		//	System.exit(0);
		}
		if(this.intentBitVecBuilder.length() == 0) {
			this.queryTypeBitMap = toString(b,4);
			this.intentBitVecBuilder.append(this.queryTypeBitMap);
			//this.intentBitVector = toString(b,4);
			
		}
		else{
			System.out.println("Invalid intent bitvector!!");
		//	System.exit(0);
		}			
	}
	
	public void appendToBitVectorString(String b) throws Exception{
		if(this.intentBitVecBuilder.length()==0) {
			System.out.println("Invalid intent bitvector!!");
		//	System.exit(0);
		}
		else {
			this.intentBitVecBuilder.append(b);
			//this.intentBitVector += b;
		}
	}
	
	public void createBitVectorForTables() throws Exception{
		HashMap<String,Integer> MINCTables = this.schParse.fetchMINCTables();
		BitSet b = new BitSet(MINCTables.size());
		for(Table tab : this.tables) {
			String tableName = this.cleanString(tab.getName().toLowerCase());
			int tableIndex = MINCTables.get(tableName);
			b.set(tableIndex);
		}
		this.tableBitMap = toString(b,MINCTables.size());
		appendToBitVectorString(this.tableBitMap);
	}
	
	public boolean checkIfTableExists(String tableName) throws Exception{
		for(Table tab:this.tables) {
			String queryTableName = this.cleanString(tab.getName().toLowerCase());
			if (tableName.equals(queryTableName))
				return true;
		}
		return false;
	}
	
	public static String[] cleanColArrayString(String colArray) throws Exception{
		colArray = colArray.strip();
		colArray=colArray.replace("[", "");
		colArray=colArray.replace("]", "");
		colArray=colArray.replaceAll("'", "");
		return colArray.split(",\\s*");
	}
	
	
	public String setColumnsFromTable(String tableName, ArrayList<String> colNames) throws Exception{
		if(colNames==null)
			return setAllorNoneColumnsFromTable(tableName, "none");
		if(colNames.size()==1 && colNames.get(0).equals("*"))
			return setAllorNoneColumnsFromTable(tableName, "all");
		HashMap<String,String> MINCColumns = this.schParse.fetchMINCColumns();
		String[] colArray = cleanColArrayString(MINCColumns.get(tableName));
		BitSet b = new BitSet(colArray.length);
		if(colNames != null) {
			for(int i=0; i<colArray.length; i++) {
				if(colNames.contains(colArray[i].toLowerCase()) || colNames.contains(colArray[i].toUpperCase()))
					b.set(i);
			}
		}
		return toString(b,colArray.length);
	}
	
	public String setAllorNoneColumnsFromTable(String tableName, String allorNone) throws Exception{
		assert (allorNone.equals("all") || allorNone.equals("none"));
		HashMap<String,String> MINCColumns = this.schParse.fetchMINCColumns();
		String[] colArray = cleanColArrayString(MINCColumns.get(tableName));
		BitSet b = new BitSet(colArray.length);
		if(allorNone.equals("all")) {
			for(int i=0; i<colArray.length; i++) {
				b.set(i);
			}
		}
		return toString(b,colArray.length);
	}
	
	public String setAllColumns() throws Exception{
		//int[] setIndices = this.tableIndices.stream().toArray();
		HashMap<String,Integer> MINCTables = this.schParse.fetchMINCTables();
		int i=0;
		String b = "";
		for(String key:MINCTables.keySet()) {
			int tabIndex = MINCTables.get(key);
			assert tabIndex == i;
			if(checkIfTableExists(key))
				b+=setAllorNoneColumnsFromTable(key, "all");
			else
				b+=setAllorNoneColumnsFromTable(key, "none");
			i++;
		}
		return b;
	}
	
	public String searchColDictForTableName(String colName) throws Exception{
		for(Table table : this.tables) {
			HashMap<String,String> MINCColumns = this.schParse.fetchMINCColumns();
			String[] colArray = cleanColArrayString(MINCColumns.get(table.getName().replace("`", "").toLowerCase()));
			for(int i=0; i<colArray.length; i++) {
				if (colArray[i].equals(colName))
					return table.getName();
			}
		}
		return null;
	}
	
	public boolean searchForTableName(String tableName) throws Exception {
		for(Table t : this.tables) {
			if(t.getName().toLowerCase().equals(tableName))
				return true;
		}
		return false;
	}
	
	
	public HashMap<String,ArrayList<String>> createTableColumnDict(HashSet<Column> colSet) throws Exception{
		HashMap<String,ArrayList<String>> tableColumnDict = new HashMap<String,ArrayList<String>>();
		for(Column c:colSet) {
			Pair<String,String> tabColName = this.retrieveTabColName(c);
			String tableName = tabColName.first;
			String colName = tabColName.second;
			if(tableName == null)
				continue;
			if (!tableColumnDict.containsKey(tableName))
				tableColumnDict.put(tableName, new ArrayList<String>());
			tableColumnDict.get(tableName).add(colName.toLowerCase());		
		}
		return tableColumnDict;
	}
/*	public String createBitVectorForOpColSetOld(HashSet<Column> colSet) throws Exception {
		String b = "";
		if (colSet.size()==1 && colSet.iterator().next().toString().equals("*")) {
				b = setAllColumns();
				appendToBitVectorString(b);
				return b;
		}
		HashMap<String,ArrayList<String>> tableColumnDict = createTableColumnDict(colSet);
		HashMap<String,Integer> schemaTables = this.schParse.fetchMINCTables();
	//	HashSet<String> tableNames = new HashSet<String>(schemaTables.keySet());
		for(String tableName:schemaTables.keySet()) {
			String bitMapPerTable;
			if(tableColumnDict.containsKey(tableName)) {
				bitMapPerTable = this.setColumnsFromTable(tableName, tableColumnDict.get(tableName));
			}
			else
				bitMapPerTable = this.setColumnsFromTable(tableName, null);
			b+=bitMapPerTable;
		}
		this.appendToBitVectorString(b);
		return b;
	}
*/	
	public BitSet setAllColumnsFromTable(String tableName, BitSet bitVector) throws Exception{
		HashMap<String,String> MINCColumns = this.schParse.fetchMINCColumns();
		String[] colArray = cleanColArrayString(MINCColumns.get(tableName));
		for(String colName : colArray) {
			int bitPos = this.schParse.fetchMINCColBitPos().get(tableName+"."+colName);
			bitVector.set(bitPos);
		}
		return bitVector;
	}
	
	public String createBitVectorForOpColSet(HashSet<Column> colSet) throws Exception {
		String b = "";
		for(Column c : colSet) {
			if(c.toString().equals("*")) {
				b = setAllColumns();
				appendToBitVectorString(b);
				return b;
			}
		}
		/*if (colSet.size()==1 && colSet.iterator().next().toString().equals("*")) {
				b = setAllColumns();
				appendToBitVectorString(b);
				return b;
		}*/
		HashMap<String,ArrayList<String>> tableColumnDict = createTableColumnDict(colSet);
		HashMap<String,Integer> schemaTables = this.schParse.fetchMINCTables();
		HashMap<String,Integer> schemaCols = this.schParse.fetchMINCColBitPos();
		BitSet bitVector = new BitSet(schemaCols.size());
	//	HashSet<String> tableNames = new HashSet<String>(schemaTables.keySet());
		for(String tableName:tableColumnDict.keySet()) {
			ArrayList<String> colNames = tableColumnDict.get(tableName);
			for(String colName : colNames) {
				if(colName.equals("*")) {
					bitVector = setAllColumnsFromTable(tableName, bitVector);
				} else {
					String fullColName = tableName+"."+colName;
					int colBitPos;
				//	try {
						colBitPos = schemaCols.get(fullColName);
						bitVector.set(colBitPos);
				/*	} catch(Exception e) {
						e.printStackTrace();
						continue;
					}  */
				}
			}
		}
		b = toString(bitVector, schemaCols.size());
		this.appendToBitVectorString(b);
		return b;
	}
	
	public void createBitVectorForLimit() throws Exception{
		if(this.limitList.size()==1) {
			assert (this.limitList.toArray()[0].equals("limit"));
			this.appendToBitVectorString("1");
			this.limitBitMap = "1";
		}
		else {
			this.appendToBitVectorString("0");
			this.limitBitMap = "0";
		}
	}
	
	public HashMap<HashSet<String>,ArrayList<HashSet<String>>> convertColumnListToStringSet() throws Exception{
		HashMap<HashSet<String>,ArrayList<HashSet<String>>> joinPredDictQuery = new HashMap<HashSet<String>,ArrayList<HashSet<String>>>();
		for(ArrayList<Column> colPair : this.joinPredicates) {
			HashSet<String> tableNamePair = new HashSet<String>();
			HashSet<String> columnNamePair = new HashSet<String>();
			for(Column c:colPair) {
				Pair<String,String> tabColName = this.retrieveTabColName(c);
				String tableName = tabColName.first;
				String colName = tabColName.second;
				if(tableName == null)
					continue;
				tableNamePair.add(tableName);
				columnNamePair.add(colName);
			}
			if (!joinPredDictQuery.containsKey(tableNamePair))
				joinPredDictQuery.put(tableNamePair, new ArrayList<HashSet<String>>());
			joinPredDictQuery.get(tableNamePair).add(columnNamePair);	
		}
		return joinPredDictQuery;
	}
	
	public int locateHashSetInValues(HashSet<String> querySet, ArrayList<HashSet<String>> valueList) throws Exception {
		int valueIndex = 0;
		for(HashSet<String> key : valueList) {
			if(Util.equals(querySet, key))
				return valueIndex;
			valueIndex++;
		}
		return -1;
	}
		
	public String locateHashSetInKeys(HashSet<String> querySet, HashSet<String> keySet) throws Exception {
		String[] queryArr = querySet.toArray(new String[querySet.size()]);
		if(queryArr.length==1) {
			String key = queryArr[0]+","+queryArr[0];
			if(keySet.contains(key))
				return key;
		}
		else if(queryArr.length==2){
			String key = queryArr[0]+","+queryArr[1];
			if(keySet.contains(key))
				return key;
			key = queryArr[1]+","+queryArr[0];
			if(keySet.contains(key))
				return key;
		}
		else {
			System.out.println("querySet should be a pair and does not contain more than 2 tables !!");
		}
		return null;
	}
	
	public static boolean compareJoinPreds(Pair<String,String> set1, HashSet<String> set2){
		if(set1 == null || set2 ==null){
            return false;
        }
		
		
		HashSet<String> pairToSet = new HashSet<String>();
		pairToSet.add(set1.first.trim());
		pairToSet.add(set1.second.trim());
		
	/*	if(pairToSet.contains("params")) {
			 int a = 1;
		}
		*/
        
       return Util.equals(pairToSet, set2);
	}
	
	public HashMap<String,ArrayList<String>> createJoinDictQuery() throws Exception{
		HashMap<String,ArrayList<String>> joinPredDictQuery = new HashMap<String,ArrayList<String>>();
		for(ArrayList<Column> colPair : this.joinPredicates) {
			String leftTable =null, leftCol=null, rightTable=null, rightCol=null;
			assert colPair.size()==2;
			for(int i=0; i<colPair.size(); i++) {
				Column c = colPair.get(i);
				Pair<String,String> tabColName = this.retrieveTabColName(c);
				String tableName = tabColName.first;
				String colName = tabColName.second;
				if(tableName == null)
					continue;
				if(i==0) {
					leftTable = tableName;
					leftCol = colName;
				} else if(i==1) {
					rightTable = tableName;
					rightCol = colName;
				}
			}
			int leftTableIndex = this.schParse.fetchMINCTables().get(leftTable);
			int rightTableIndex = this.schParse.fetchMINCTables().get(rightTable);
			String joinColPair = null;
			String joinTablePair = null;
			if (leftTableIndex <= rightTableIndex) {
				joinTablePair = leftTable+","+rightTable;
				joinColPair = leftCol + "," + rightCol;
			} else {
				joinTablePair = rightTable+","+leftTable;
				joinColPair = rightCol + "," + leftCol;
			}
			if (!joinPredDictQuery.containsKey(joinTablePair))
				joinPredDictQuery.put(joinTablePair, new ArrayList<String>());
			joinPredDictQuery.get(joinTablePair).add(joinColPair);	
		}
		return joinPredDictQuery;
	}
	
	public void createBitVectorForJoin() throws Exception {
		HashMap<String, ArrayList<String>> joinPredDictQuery = createJoinDictQuery();
		HashMap<String,ArrayList<String>> joinPredDictSchema = this.schParse.fetchMINCJoinPreds();
		HashMap<String,Pair<Integer,Integer>> joinPredBitPosSchema = this.schParse.fetchMINCJoinPredBitPos();
		BitSet joinPredIntentVector = new BitSet(this.schParse.fetchMINCJoinPredBitCount());
		for(String tablePairQuery : joinPredDictQuery.keySet()) {
			ArrayList<String> joinPredListSchema = joinPredDictSchema.get(tablePairQuery);
			ArrayList<String> joinPredListQuery = joinPredDictQuery.get(tablePairQuery);
			Pair<Integer,Integer> startEndBitPos = joinPredBitPosSchema.get(tablePairQuery);
			for(String joinPredQuery : joinPredListQuery) {
				int bitIndex = startEndBitPos.first+joinPredListSchema.indexOf(joinPredQuery);
				joinPredIntentVector.set(bitIndex);
			}
		}
		String b = toString(joinPredIntentVector,this.schParse.fetchMINCJoinPredBitCount());
		this.appendToBitVectorString(b);
		this.joinPredicatesBitMap = b;
	}
	
/*	public void createBitVectorForJoinDeprecated() throws Exception{
		//key is tablePair and value is a list of column pairs
		HashMap<HashSet<String>,ArrayList<HashSet<String>>> joinPredDictQuery = convertColumnListToStringSet();
		HashMap<String,ArrayList<toolsForMetrics.Pair<String,String>>> joinPredDictSchema = this.schParse.fetchMINCJoinPreds();
		HashMap<String,toolsForMetrics.Pair<Integer,Integer>> joinPredBitPosSchema = this.schParse.fetchMINCJoinPredBitPos();
		HashSet<Integer> bitPosToSet = new HashSet<Integer>();
		for(HashSet<String> tablePairQuery : joinPredDictQuery.keySet()) {
			String dictKey = locateHashSetInKeys(tablePairQuery, new HashSet<String>(joinPredDictSchema.keySet()));
			ArrayList<HashSet<String>> joinPredListQuery = joinPredDictQuery.get(tablePairQuery);
			ArrayList<toolsForMetrics.Pair<String,String>> joinPredListSchema = joinPredDictSchema.get(dictKey);
			toolsForMetrics.Pair<Integer,Integer> startEndBitPos = joinPredBitPosSchema.get(dictKey);
			for(HashSet<String> joinPredQuery : joinPredListQuery) {
				int joinPredSchemaIndex = 0;
				for(toolsForMetrics.Pair<String,String> joinPredSchema : joinPredListSchema) {
					if(compareJoinPreds(joinPredSchema, joinPredQuery)) {
						int bitIndex = startEndBitPos.getKey()+joinPredSchemaIndex;
						bitPosToSet.add(bitIndex);
					}
					joinPredSchemaIndex++;	
				}
			}
		}
		BitSet joinPredIntentVector = new BitSet(this.schParse.fetchMINCJoinPredBitCount());
		for(int bitIndex:bitPosToSet) {
			joinPredIntentVector.set(bitIndex);
		}
		this.appendToBitVectorString(toString(joinPredIntentVector,this.schParse.fetchMINCJoinPredBitCount()));
		this.joinPredicatesBitMap = toString(joinPredIntentVector,this.schParse.fetchMINCJoinPredBitCount());
	}
*/	
	public Pair<String, String> replaceColAliases(String tableName, String colName) throws Exception {
		Pair<String, String> tabColName = new Pair<>(tableName, colName.toLowerCase());
		if(this.colAliases==null)
			return tabColName;
		if(this.colAliases.containsKey(tableName+"."+colName)) {
			String fullName = this.colAliases.get(tableName+"."+colName).toString().replace("`", "").toLowerCase();
			if(fullName.contains(".")) {
				String[] tokens = fullName.split("\\.");
				assert tokens.length == 2;
				colName = tokens[1];
			} else {
				colName = fullName;
			}
			tabColName = new Pair<>(tableName, colName.toLowerCase());
		}
		return tabColName;
	}
	public Pair<String, String> retrieveTabColName(Column c) throws Exception{
		Pair<String, String> tabColName;
		String fullName = c.toString().replace("`", "").toLowerCase();
		String tableName;
		String colName = fullName.toLowerCase();
		if(fullName.contains(".")) {
			String[] tokens = fullName.split("\\.");
			assert tokens.length == 2;
			String tableNameAlias = tokens[0].toLowerCase();
			colName = tokens[1].toLowerCase();
			tableName = tableNameAlias; // if there is no tableAlias tableName is being used
			boolean tableExists = searchForTableName(tableName);
			if (!tableExists && Global.tableAlias.size()>0) 
				tableName = Global.tableAlias.get(tableNameAlias).toLowerCase();	
		}
		else {
			// there should be a single table name in the from clause, else simply search for the first table name
			if(this.tables.size()==1) 
				tableName = this.tables.get(0).getName().replace("`", "").toLowerCase();
			else
				tableName = searchColDictForTableName(colName.toLowerCase()).toLowerCase();
			if(tableName != null)
				tableName = tableName.toLowerCase();
		}
		/*if (!tableColumnDict.containsKey(tableName))
			tableColumnDict.put(tableName, new ArrayList<String>());
		tableColumnDict.get(tableName).add(colName.toLowerCase());	*/
		tabColName = replaceColAliases(tableName, colName.toLowerCase());
		return tabColName;
	}
	
	public int fetchMatchingBucketIndex(HashMap<String, Pair<Integer, Integer>> selPredColRangeBitPos, String selColFullName, int binIndex) {
		int lo_index = selPredColRangeBitPos.get(selColFullName).first;
		int hi_index = selPredColRangeBitPos.get(selColFullName).second;
		assert lo_index + binIndex <= hi_index;
		return lo_index + binIndex;
	}
	
	public static Set<String> longestCommonSubstrings(String s, String t) {
	    int[][] table = new int[s.length()][t.length()];
	    int longest = 0;
	    Set<String> result = new HashSet<>();

	    for (int i = 0; i < s.length(); i++) {
	        for (int j = 0; j < t.length(); j++) {
	            if (s.charAt(i) != t.charAt(j)) {
	                continue;
	            }

	            table[i][j] = (i == 0 || j == 0) ? 1
	                                             : 1 + table[i - 1][j - 1];
	            if (table[i][j] > longest) {
	                longest = table[i][j];
	                result.clear();
	            }
	            if (table[i][j] == longest) {
	                result.add(s.substring(i - longest + 1, i + 1));
	            }
	        }
	    }
	    return result;
	}
	
	public static boolean isInteger(String s) {
	    try { 
	        Integer.parseInt(s); 
	    } catch(NumberFormatException e) { 
	        return false; 
	    } catch(NullPointerException e) {
	        return false;
	    }
	    // only got here if we didn't return false
	    return true;
	}
	
	public int findSelColRangeBinString(String constVal, ArrayList<Pair<String, String>> rangeBins, HashMap<String, ArrayList<Pair<String, String>>> selPredColRangeBins, 
			HashMap<String, Pair<Integer, Integer>> selPredColRangeBitPos, String selColFullName) throws Exception {
		String lo_str, hi_str;
		constVal = constVal.replaceAll("^\'|\'$", "");
		for(int binIndex = 0; binIndex < rangeBins.size(); binIndex++) {
			Pair<String, String> rangeBin = rangeBins.get(binIndex);
			lo_str = rangeBin.first;
			hi_str = rangeBin.second;
			// first look for %x% substring comparison
			if(constVal.startsWith("%")){
				String tempStr = constVal.replace("%", "");
				if(lo_str.contains(tempStr) || hi_str.contains(tempStr)) {
					int bucketIndex = fetchMatchingBucketIndex(selPredColRangeBitPos, selColFullName, binIndex);
					return bucketIndex;
				}	
			}
			int lo_compare = constVal.compareTo(lo_str);
			int hi_compare = constVal.compareTo(hi_str);
			if((lo_compare>=0 && hi_compare<=0) || (lo_str.equals("null") && hi_str.equals("null"))) {
				int bucketIndex = fetchMatchingBucketIndex(selPredColRangeBitPos, selColFullName, binIndex);
				return bucketIndex;
			}
		}
		return -1;
	}
	
	public int findSelColRangeBinInteger(String constVal, ArrayList<Pair<String, String>> rangeBins, HashMap<String, ArrayList<Pair<String, String>>> selPredColRangeBins, 
			HashMap<String, Pair<Integer, Integer>> selPredColRangeBitPos, String selColFullName) throws Exception {
		int lo, hi;
		int constValInt = Integer.parseInt(constVal);
		for(int binIndex = 0; binIndex < rangeBins.size(); binIndex++) {
			Pair<String, String> rangeBin = rangeBins.get(binIndex);
			if(rangeBin.first.equals("null") && rangeBin.second.equals("null")) {
				int bucketIndex = fetchMatchingBucketIndex(selPredColRangeBitPos, selColFullName, binIndex);
				return bucketIndex;
			}
			lo = Integer.parseInt(rangeBin.first);
			hi = Integer.parseInt(rangeBin.second);
			if(constValInt>=lo && constValInt<=hi) {
				int bucketIndex = fetchMatchingBucketIndex(selPredColRangeBitPos, selColFullName, binIndex);
				return bucketIndex;
			}
		}
		return -1;
	}
	
	public boolean checkForIntColType(String selColFullName) throws Exception {
		String tableName = selColFullName.split("\\.")[0];
		String colName = selColFullName.split("\\.")[1];
		HashMap<String,String> MINCColumns = this.schParse.fetchMINCColumns();
		HashMap<String, String> MINCColTypes = this.schParse.fetchMINCColTypes();
		String[] colTypeArray = cleanColArrayString(MINCColTypes.get(tableName));
		String[] colArray = cleanColArrayString(MINCColumns.get(tableName));
		for(int i=0; i<colArray.length; i++) {
			if (colArray[i].equals(colName)) {
				String colType = colTypeArray[i];
				if(colType.contains("int"))
					return true;
			}
		}
		return false;
	}
	
	public int findSelColRangeBinToSet(String selColFullName, String constVal) throws Exception {
		HashMap<String, ArrayList<Pair<String, String>>> selPredColRangeBins = this.schParse.fetchMINCSelPredColRangeBins();
		HashMap<String, Pair<Integer, Integer>> selPredColRangeBitPos = this.schParse.fetchMINCSelPredColRangeBitPos();
		ArrayList<Pair<String, String>> rangeBins = selPredColRangeBins.get(selColFullName);
		boolean isInt = isInteger(constVal);
		if(isInt) {
            isInt = checkForIntColType(selColFullName);
        }
		int matchingBucketIndex = -1;
		if(isInt) {
            matchingBucketIndex = findSelColRangeBinInteger(constVal, rangeBins, selPredColRangeBins, selPredColRangeBitPos, selColFullName);
        } else {
            matchingBucketIndex = findSelColRangeBinString(constVal, rangeBins, selPredColRangeBins, selPredColRangeBitPos, selColFullName);
        }
		return matchingBucketIndex;
	}
	
	public void createBitVectorForSelPredColRangeBins() throws Exception {
		String b = "";
		int selColRangeBitMapSize = this.schParse.fetchMINCSelPredColRangeBitCount();
		BitSet bitVector = new BitSet(selColRangeBitMapSize); // col range bit count
		int bitPosToSet;
		for(Column c : this.selPredConstants.keySet()) {
			ArrayList<String> constValList = this.selPredConstants.get(c);
			String selColFullName = retrieveFullColumnName(c);
			for(String constVal : constValList) {
				bitPosToSet = findSelColRangeBinToSet(selColFullName, constVal);
				bitVector.set(bitPosToSet);
			}
		}
		b = toString(bitVector, selColRangeBitMapSize);
		this.appendToBitVectorString(b);
		this.selPredColRangeBinBitMap = b;
		return;
	}
	
	public String retrieveFullColumnName(Column c) throws Exception{
		Pair<String, String> tabColName = retrieveTabColName(c);
		String tableName = tabColName.first;
		String colName = tabColName.second;
		String colFullName = tableName+"."+colName;
		return colFullName;
	}
	
	
	public void createBitVectorForSelPredOps() throws Exception {
		String b = "";
		HashMap<String, Integer> selPredCols = this.schParse.fetchMINCSelPredCols();
		int selBitMapSize = selPredCols.size() * this.selPredOpList.length;
		BitSet bitVector = new BitSet(selBitMapSize);
		for(Column c : this.selPredOps.keySet()) {
			ArrayList<String> opValList = this.selPredOps.get(c);
			String selColFullName = retrieveFullColumnName(c);
			int baseIndex = selPredCols.get(selColFullName) * this.selPredOpList.length;
			for(String opVal : opValList) {
				int offset = Arrays.asList(this.selPredOpList).indexOf(opVal);
				int bitPosToSet = baseIndex + offset;
				bitVector.set(bitPosToSet);
			}
		}
		b = toString(bitVector, selBitMapSize);
		this.appendToBitVectorString(b);
		this.selPredOpBitMap = b;
		return;
	}
	
	public void createFragmentVectors() throws Exception {
		createBitVectorForQueryTypes();
		//System.out.println("this.queryTypeBitMap: "+this.queryTypeBitMap);
		createBitVectorForTables();
		//System.out.println("this.tableBitMap: "+this.tableBitMap+", length: "+this.tableBitMap.toCharArray().length);
		this.projectionBitMap = createBitVectorForOpColSet(this.projectionColumns);
		//System.out.println("this.projectionBitMap: "+this.projectionBitMap+", length: "+this.projectionBitMap.toCharArray().length);
		this.AVGBitMap = createBitVectorForOpColSet(this.AVGColumns);
		//System.out.println("this.AVGBitMap: "+this.AVGBitMap+", length: "+this.AVGBitMap.toCharArray().length);
		this.MINBitMap = createBitVectorForOpColSet(this.MINColumns);
		//System.out.println("this.MINBitMap: "+this.MINBitMap+", length: "+this.MINBitMap.toCharArray().length);
		this.MAXBitMap = createBitVectorForOpColSet(this.MAXColumns);
		//System.out.println("this.MAXBitMap: "+this.MAXBitMap+", length: "+this.MAXBitMap.toCharArray().length);
		this.SUMBitMap = createBitVectorForOpColSet(this.SUMColumns);
		//System.out.println("this.SUMBitMap: "+this.SUMBitMap+", length: "+this.SUMBitMap.toCharArray().length);
		this.COUNTBitMap = createBitVectorForOpColSet(this.COUNTColumns);
		//System.out.println("this.COUNTBitMap: "+this.COUNTBitMap+", length: "+this.COUNTBitMap.toCharArray().length);
		this.selectionBitMap = createBitVectorForOpColSet(this.selectionColumns);
		//System.out.println("this.selectionBitMap: "+this.selectionBitMap+", length: "+this.selectionBitMap.toCharArray().length);
		this.groupByBitMap = createBitVectorForOpColSet(this.groupByColumns);
		//System.out.println("this.groupByBitMap: "+this.groupByBitMap+", length: "+this.groupByBitMap.toCharArray().length);
		this.orderByBitMap = createBitVectorForOpColSet(this.orderByColumns);
		//System.out.println("this.orderByBitMap: "+this.orderByBitMap+", length: "+this.orderByBitMap.toCharArray().length);
		this.havingBitMap = createBitVectorForOpColSet(this.havingColumns);
		//System.out.println("this.havingBitMap: "+this.havingBitMap+", length: "+this.havingBitMap.toCharArray().length);
		createBitVectorForLimit();
		//System.out.println("this.limitBitMap: "+this.limitBitMap+", length: "+this.limitBitMap.toCharArray().length);
//		createBitVectorForJoin();
		//System.out.println("this.joinPredBitMap: "+this.joinPredicatesBitMap+", length: "+this.joinPredicatesBitMap.toCharArray().length);
		if(this.includeSelOpConst) {
			createBitVectorForSelPredOps();
			createBitVectorForSelPredColRangeBins();
		}
		this.intentBitVector = this.intentBitVecBuilder.toString();
	}
	
	public boolean parseQueryAndCreateFragmentVectors() throws Exception {
		if(this.queryType.equals("select") || this.queryType.equals("update") || this.queryType.equals("insert") || this.queryType.equals("delete")) {
			try {
				this.parseQuery();
				this.createFragmentVectors();
			} catch(Exception e) {
				e.printStackTrace();
				return false;
			}
			return true;
		}
		else {
			//System.out.println("It has to be one of Select, Update, Insert or Delete !!");
			return false;
			//System.exit(0);
		}		
	}
	/*public static void readFrom100KFile(String queryFile, String line, String prevSessionID, com.clickhouse.SchemaParser schParse, int queryID, boolean includeSelOpConst, String dataset) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader(queryFile));
		while((line=br.readLine())!=null) {
			if(line.contains("Query")) {
				line = line.replace("\t"," ");
				line = line.replaceAll("\\s+", " ");
				line = line.trim();
				String[] tokens = line.split(" ");
				String query = "";
				for(int i=2; i<tokens.length; i++) {
					if(i==2)
						query = tokens[i];
					else
						query += " "+tokens[i];
				}
				System.out.println("Query: "+query);
				String sessionID = tokens[0];
				if(!sessionID.equals(prevSessionID)) {
					queryID = 0;
					prevSessionID = sessionID;
				} else
					queryID++;
				encoder.MINCFragmentIntent fragmentObj = new encoder.MINCFragmentIntent(query, schParse, includeSelOpConst, dataset);
				try {
					boolean validQuery = fragmentObj.parseQueryAndCreateFragmentVectors();
					if(validQuery)
						fragmentObj.printIntentVector();
					System.out.println("Covered SessionID: "+sessionID+", queryID: "+queryID);
				} catch(Exception e) {
					continue;
				}
			}
		}
	}
	*/
	public static void deleteIfExists(String fileName) throws Exception{
		File outFile = new File(fileName);
		boolean delIfExists = Files.deleteIfExists(outFile.toPath());
	}
	
	public static int updateSessionQueryCount(HashMap<String, Integer> sessionQueryCount, String sessID) throws Exception{
		int queryID;
		try {
			queryID = sessionQueryCount.get(sessID)+1;
		} catch(Exception e) {
			queryID = 1;
		}
		sessionQueryCount.put(sessID, queryID);
		return queryID;
	}
	
	public static ArrayList<String> countMINCLinesPreProcessed(String rawSessFile, int startLineNum) throws Exception{
		System.out.println("Counting lines from "+rawSessFile);
		BufferedReader br = new BufferedReader(new FileReader(rawSessFile));
		ArrayList<String> lines = new ArrayList<String>();
		String line = null;
		int i=0;
		int absCount = 0;
		while ((line=br.readLine())!=null /*  && absCount<3000000+startLineNum*/) {
			if(absCount>=startLineNum) {
				lines.add(line);
				i++;
				if (i%100000 == 0)
					System.out.println("Read "+i+" lines so far and absCount: "+absCount);
			}
			absCount++;
		}
		System.out.println("Read "+i+" lines so far and done with absCount: "+absCount);
		br.close();
		return lines;
	}
	
	public static ArrayList<String> countMINCLines(String rawSessFile, int startLineNum) throws Exception{
		System.out.println("Counting lines from "+rawSessFile);
		BufferedReader br = new BufferedReader(new FileReader(rawSessFile));
		ArrayList<String> lines = new ArrayList<String>();
		String line = null;
		int i=0;
		int absCount = 0;
		while ((line=br.readLine())!=null /*  && absCount<3000000+startLineNum*/) {
			if(absCount>=startLineNum && line.contains("Query")) {
				line = line.replace("\t"," ");
				line = line.replaceAll("\\s+", " ");
				line = line.trim();
				lines.add(line);
				i++;
				if (i%1000000 == 0)
					System.out.println("Read "+i+" lines so far and absCount: "+absCount);
			}
			absCount++;
		}
		System.out.println("Read "+i+" lines so far and done with absCount: "+absCount);
		br.close();
		return lines;
	}
	
	public static ArrayList<String> countBusTrackerLinesPreProcessed(String rawSessFile, int startLineNum) throws Exception{
		System.out.println("Counting lines from "+rawSessFile);
		BufferedReader br = new BufferedReader(new FileReader(rawSessFile));
		ArrayList<String> lines = new ArrayList<String>();
		String line = null;
		int i=0;
		int absCount = 0;
		while ((line=br.readLine())!=null /*  && absCount<3000000+startLineNum*/) {
			if(absCount>=startLineNum) {
				lines.add(line);
				i++;
				if (i%100000 == 0)
					System.out.println("Read "+i+" lines so far and absCount: "+absCount);
			}
			absCount++;
		}
		System.out.println("Read "+i+" lines so far and done with absCount: "+absCount);
		br.close();
		return lines;
	}
	
	public static String cleanUpBusTrackerLine(String line) throws Exception {
		String substr = "((SELECT extract(epoch FROM ";
		String substr2 = ")*1000))";
		//((SELECT extract(epoch FROM c.start_date)*1000)) <= 1480475749583
		if(line.contains(substr) && line.contains(substr2)) {
			line = line.replace(substr, "");
			line = line.replace(substr2, "");
		}
		line = line.replace("$", "");
		line = line.replace("|/", "");
		line = line.replace("^2", "");
		line = line.replace("CURRENT_DATE", "12-30-2019");
		String inputQueryPhrase = "select nm.trip_id,nm.id AS message_id, nm.message, nm.timestamp, nm.category,a.firstname AS first_name, a.lastname AS last_name "
		 		+ "from dv_notes_message nm, dv_account a, (SELECT dv_trip_id, MAX(dv.timestamp) AS maximum FROM dv_notes_message dv WHERE "
		 		+ "dv_agency_id IN (select a.agency_id from m_agency a, m_agency b where a.agency_id_id=b.agency_id_id and b.agency_id=1) "
		 		+ "AND dv_trip_id IN";
		String replacedQueryPhrase = "select nm.trip_id,nm.id AS message_id, nm.message, nm.timestamp, nm.category,a.firstname AS first_name, a.lastname AS last_name "
		 		+ "from dv_notes_message nm, dv_account a, (SELECT dvNotes.trip_id, MAX(dvNotes.timestamp) AS maximum FROM dv_notes_message dvNotes WHERE "
		 		+ "dvNotes.agency_id IN (select c.agency_id from m_agency c, m_agency d where c.agency_id_id=d.agency_id_id and d.agency_id=1) "
		 		+ "AND dvNotes.trip_id IN";
		line = line.replace(inputQueryPhrase, replacedQueryPhrase);
		inputQueryPhrase = "GROUP BY dv_trip_id) "
		 		+ "as nmmax WHERE nm.deleted IS NULL AND a.id=nm.user_id AND nm.trip_id= nmmax.trip_id AND nm.timestamp = nmmax.maximum "
		 		+ "AND nm.agency_id IN (select a.agency_id from m_agency a, m_agency b where a.agency_id_id=b.agency_id_id and b.agency_id=2)";
		replacedQueryPhrase = "GROUP BY dvNotes.trip_id) "
		 		+ "as nmmax WHERE nm.deleted IS NULL AND a.id=nm.user_id AND nm.trip_id= nmmax.trip_id AND nm.timestamp = nmmax.maximum "
		 		+ "AND nm.agency_id IN (select c.agency_id from m_agency c, m_agency d where c.agency_id_id=d.agency_id_id and d.agency_id=2)";
		line = line.replace(inputQueryPhrase, replacedQueryPhrase);
		//System.out.println(line);
		return line;
	}
	
	public static ArrayList<String> countBusTrackerLines(String rawSessFile, int startLineNum) throws Exception{
		System.out.println("Counting lines from "+rawSessFile);
		BufferedReader br = new BufferedReader(new FileReader(rawSessFile));
		ArrayList<String> lines = new ArrayList<String>();
		String line = null;
		int i=0;
		int absCount = 0;
		while ((line=br.readLine())!=null /*  && absCount<3000000+startLineNum*/) {
			if(absCount>=startLineNum) {
				line = line.trim();
				line = cleanUpBusTrackerLine(line);
				lines.add(line);
				i++;
				if (i%1000000 == 0)
					System.out.println("Read "+i+" lines so far and absCount: "+absCount);
			}
			absCount++;
		}
		System.out.println("Read "+i+" lines so far and done with absCount: "+absCount);
		br.close();
		return lines;
	}
	
	public static ArrayList<String> countLinesPreProcessed(String dataset, String rawSessFile, int startLineNum) throws Exception{
		if(dataset.equals("MINC"))
			return countMINCLinesPreProcessed(rawSessFile, startLineNum);
		else if(dataset.equals("BusTracker"))
			return countBusTrackerLinesPreProcessed(rawSessFile, startLineNum);
		else
			return null;
	}
	
	public static ArrayList<String> countLines(String dataset, String rawSessFile, int startLineNum) throws Exception{
		if(dataset.equals("MINC"))
			return countMINCLines(rawSessFile, startLineNum);
		else if(dataset.equals("BusTracker"))
			return countBusTrackerLines(rawSessFile, startLineNum);
		else
			return null;
	}
	
	public static String fetchSessID(String dataset, String line) throws Exception {
		String curSessID = null;
		if(dataset.equals("MINC")) {
			curSessID = line.split(" ")[0];
		} else if(dataset.equals("BusTracker")) {
			String regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"; // to split on comma outside double quotes
			String[] tokens = line.split(regex);
			// format is "startTime","sessID","endTime","execute <unnamed>/statement: Query","parameters: $1 = ..., $2 = ..."
			curSessID = tokens[1];
			curSessID = curSessID.replaceAll("^\"|\"$", ""); // remove starting and trailing double quotes
		}
		return curSessID;
	}
	
	public static ArrayList<Pair<Integer,Integer>> readLinesPerThread(String dataset, int lowerIndexPerThread, int curThreadIndex, int numThreads, int numLinesPerThread, ArrayList<String> sessQueries, ArrayList<Pair<Integer,Integer>> inputSplits, String pruneKeepModifyRepeatedQueries) throws Exception{
		System.out.println("Splitting lines for thread "+curThreadIndex+", numLinesPerThread: "+numLinesPerThread+" with lower Index: "+lowerIndexPerThread);
		int i=0;
		int runningIndex = Math.min(lowerIndexPerThread+numLinesPerThread-1, sessQueries.size()-1);
		String line = sessQueries.get(runningIndex);
		String prevSessID = line.split(" ")[0];
		String curSessID = null;
		runningIndex++;
		while(runningIndex<sessQueries.size()){
			if(pruneKeepModifyRepeatedQueries.equals("PREPROCESS"))
				curSessID = sessQueries.get(runningIndex).trim().split("; ")[0].split(", ")[0].split(" ")[1];
			else
				curSessID = fetchSessID(dataset, sessQueries.get(runningIndex));
			if(curThreadIndex != numThreads-1 && !curSessID.equals(prevSessID))
				break;
			runningIndex++;
		}
		int upperIndexPerThread = runningIndex-1;
		Pair<Integer,Integer> lowerUpperIndexBounds = new Pair<>(lowerIndexPerThread, upperIndexPerThread);
		inputSplits.add(lowerUpperIndexBounds);
		int numLinesAssigned = upperIndexPerThread-lowerIndexPerThread+1;
		System.out.println("Assigned "+numLinesAssigned+" lines to thread "+curThreadIndex+", lowerIndex: "+lowerIndexPerThread+", upperIndex: "+upperIndexPerThread);
		return inputSplits;
	}


	
	public static ArrayList<Pair<Integer,Integer>> splitInputAcrossThreads(String dataset, ArrayList<String> sessQueries, int numThreads, String pruneKeepModifyRepeatedQueries) throws Exception{	
		assert numThreads>0;
		int numLinesPerThread = sessQueries.size()/numThreads;
		assert numLinesPerThread > 1;
		ArrayList<Pair<Integer,Integer>> inputSplits = new ArrayList<Pair<Integer,Integer>>();
		int lowerIndexPerThread = 0;
		for(int i=0; i<numThreads; i++) {
			inputSplits = readLinesPerThread(dataset, lowerIndexPerThread, i, numThreads, numLinesPerThread, sessQueries, inputSplits, pruneKeepModifyRepeatedQueries);
			lowerIndexPerThread = inputSplits.get(i).second+1; // upper index of data for current thread +1 will be the lower
			// index for the next thread
		}
		return inputSplits;
	}
	
	public static ArrayList<String> defineOutputSplits(String tempLogDir, String rawSessFile, int numThreads) throws Exception{
		ArrayList<String> outputSplitFiles = new ArrayList<String>();
		for(int i=0; i<numThreads; i++) {
			String fileName = rawSessFile.split("/")[rawSessFile.split("/").length-1];
			String outFilePerThread = tempLogDir+"/"+fileName+"_SPLIT_OUT_"+i;
			System.out.println("outFilePerThread: "+outFilePerThread);
			outputSplitFiles.add(outFilePerThread);
		}
		return outputSplitFiles;
	}
	
	public static void concatenateOutputFiles(ArrayList<String> outFiles, String intentVectorFile) throws Exception{
		BufferedWriter bw = new BufferedWriter(new FileWriter(intentVectorFile, true));
		for(String outFile : outFiles) {
			BufferedReader br = new BufferedReader(new FileReader(outFile));
			String line =null;
			int i=0;
			String concLine = "";
			while((line=br.readLine())!=null) {
				concLine+=line+"\n";
				if(i%1000 == 0) {
					bw.append(concLine);
					concLine = "";
				}
				i++;
			}
			if(!concLine.equals(""))
				bw.append(concLine);
			deleteIfExists(outFile);
		}
		bw.flush();
		bw.close();
	}
	
	
	public static void readFromRawSessionsFile(String dataset, String tempLogDir, String rawSessFile, String intentVectorFile, String line, SchemaParser schParse, int numThreads, int startLineNum, String pruneKeepModifyRepeatedQueries, boolean includeSelOpConst) throws Exception{
	//	deleteIfExists(intentVectorFile);
	//	System.out.println("Deleted previous intent file");
		ArrayList<String> sessQueries;
		if(pruneKeepModifyRepeatedQueries.equals("PREPROCESS"))
			sessQueries = countLinesPreProcessed(dataset, rawSessFile, startLineNum);
		else
			sessQueries = countLines(dataset, rawSessFile, startLineNum);
		System.out.println("Read sessQueries into main memory");
		ArrayList<Pair<Integer,Integer>> inputSplits = splitInputAcrossThreads(dataset, sessQueries, numThreads, pruneKeepModifyRepeatedQueries);
		System.out.println("Split Input Across Threads");
		ArrayList<String> outputSplitFiles = defineOutputSplits(tempLogDir, rawSessFile, numThreads);
		System.out.println("Defined Output File Splits Across Threads");
		ArrayList<IntentCreatorMultiThread> intentMTs = new ArrayList<IntentCreatorMultiThread>();
		for(int i=0; i<numThreads; i++) {
			IntentCreatorMultiThread intentMT = new IntentCreatorMultiThread(dataset, i, sessQueries, inputSplits.get(i), outputSplitFiles.get(i), schParse, pruneKeepModifyRepeatedQueries, includeSelOpConst);
			intentMT.start();		
		}
	/*	for(encoder.IntentCreatorMultiThread intentMT : intentMTs) {
			intentMT.join();
		}
		*/
	//	concatenateOutputFiles(outputSplitFiles, intentVectorFile);
	}
	
	/*public static void readFromConcurrentSessionsFile(String concSessFile, String intentVectorFile, String line, com.clickhouse.SchemaParser schParse, boolean includeSelOpConst, String dataset) throws Exception{
		BufferedReader br = new BufferedReader(new FileReader(concSessFile));
		deleteIfExists(intentVectorFile);
		HashMap<String, Integer> sessionQueryCount = new HashMap<String, Integer>();
		BufferedWriter bw = new BufferedWriter(new FileWriter(intentVectorFile, true));
		double absQueryID = 0;
		while((line=br.readLine())!=null) {
			if(line.contains("Query") && line.contains("Session")) {
				String[] tokens = line.trim().split(";");
				String query = tokens[1];
				String sessQueryID = tokens[0];
				String sessID = sessQueryID.split(",")[0].split(" ")[1];
				
				encoder.MINCFragmentIntent fragmentObj = new encoder.MINCFragmentIntent(query, schParse, includeSelOpConst, dataset);
				boolean validQuery = fragmentObj.parseQueryAndCreateFragmentVectors();
				*//*if(validQuery)
					fragmentObj.printIntentVector();*//*
				if(validQuery) {
					int queryID = updateSessionQueryCount(sessionQueryCount, sessID);
					absQueryID++;
					if(absQueryID % 100000 == 0) {
						System.out.println("Query: "+query);
						System.out.println("Covered SessionID: "+sessID+", queryID: "+queryID+", absQueryID: "+absQueryID);
					}
					String to_append = "Session "+sessID+", Query "+queryID+"; OrigQuery: "+query+";"+fragmentObj.getIntentBitVector()+"\n";
					bw.append(to_append);
				}
			}
		}
	}
	*/
	public static void main(String[] args) {
//		String homeDir = System.getProperty("user.home");
		System.out.println(MINCFragmentIntent.getMachineName());
//		if(encoder.MINCFragmentIntent.getMachineName().contains("4119510") || encoder.MINCFragmentIntent.getMachineName().contains("4119509")
//				|| encoder.MINCFragmentIntent.getMachineName().contains("4119508") || encoder.MINCFragmentIntent.getMachineName().contains("4119507")) {
//			homeDir = "/hdd2/vamsiCodeData"; // comment it when you are not running on EN4119510L.cidse.dhcp.adu.edu
//		}
//		String configFile = homeDir+"/var/data/MINC/InputOutput/MincJavaConfig.txt";
		String configFile = "/var/data/BusTracker/InputOutput/MincJavaConfig.txt";
		SchemaParser schParse = new SchemaParser();
		schParse.fetchSchema(configFile);
		HashMap<String, String> configDict = schParse.getConfigDict();
		String queryFile = "/Users/postgres/PycharmProjects/HumanIntentEvaluation/sample_100K.log";
		String concSessFile = configDict.get("MINC_CONC_SESS_FILE"); // already configDict prepends the homeDir
		String intentVectorFile = configDict.get("MINC_FRAGMENT_INTENT_VECTOR_FILE");
		String rawSessFile = configDict.get("MINC_RAW_SESS_FILE");
		String tempLogDir = configDict.get("MINC_TEMP_LOG_DIR");
		int numThreads = Integer.parseInt(configDict.get("MINC_NUM_THREADS"));
		int startLineNum = Integer.parseInt(configDict.get("MINC_START_LINE_NUM"));
		String pruneKeepModifyRepeatedQueries = configDict.get("MINC_KEEP_PRUNE_MODIFY_REPEATED_QUERIES");
		boolean includeSelOpConst = Boolean.parseBoolean(configDict.get("MINC_SEL_OP_CONST"));
		String dataset = configDict.get("MINC_DATASET");
		try {
			String line = null;
			String prevSessionID = null;
			int queryID = 0;
			
			//uncomment the following when full run needs to happen on EC2 or on EN4119510L
		//	readFromRawSessionsFile(dataset, tempLogDir, rawSessFile, intentVectorFile, line, schParse, numThreads, startLineNum, pruneKeepModifyRepeatedQueries, includeSelOpConst);

			String query = null;
	/*		String query = "SELECT distinct a.agency_id FROM m_agency a, m_calendar c, m_trip t WHERE c.agency_id = a.agency_id AND t.agency_id = a.agency_id AND "
					+ "a.avl_agency_name =  '8\\b8164b0b579a1a3cde19a106c8e1fca8' AND t.trip_id =  '33\\94f574661cc4d7d3c40a333a0509fd4f' "
					+ "AND c.start_date <= 1480475749583 AND c.end_date+1 >= 1480475749583";
			query = "select st.trip_id, st.stop_sequence, st.estimate_source, st.fullness, st.departure_time_hour, "
					+ "st.departure_time_minute, s.stop_lat, s.stop_lon, t.direction_id, t.route_id, route.route_short_name "
					+ "from m_stop AS s RIGHT JOIN m_stop_time AS st  ON st.agency_id = s.agency_id "
					+ "AND st.stop_id = s.stop_id LEFT JOIN m_trip AS t ON t.agency_id = st.agency_id "
					+ "AND t.trip_id = st.trip_id LEFT JOIN m_route AS route ON t.agency_id = route.agency_id "
					+ "AND t.route_id = route.route_id WHERE st.estimate_source in "
					+ "( '10\\2d9d369aa6dcb27617fe409b5cac85ca',  '14\\dbcdf91e0b5531167767adab3b850514') "
					+ "AND st.agency_id = 1 AND (((departure_time_hour * 60 + departure_time_minute) >= (2-5)  "
					+ "AND (departure_time_hour * 60 + departure_time_minute) <= (3+10)) "
					+ "OR ((departure_time_hour * 60 + departure_time_minute) >= (4-5)  "
					+ "AND (departure_time_hour * 60 + departure_time_minute) <= (5+10)))  "
					+ "order by st.stop_sequence";
			 query = "SELECT s.stop_id AS stop_id, s.stop_name, s.stop_lat, s.stop_lon, ceiling((h_distance(0.0,0.0,s.stop_lat,s.stop_lon)/1.29)/60) "
			 		+ "AS walk_time  FROM m_stop s  WHERE s.stop_lat BETWEEN (1-2) AND (3+4)  AND s.agency_id = 5  AND s.stop_lon BETWEEN (6-7) AND (8+9)  "
			 		+ "ORDER BY (((s.stop_lat-(10))+(s.stop_lon-(11))))";
			 query = "SELECT id, message_title, message, destination_screen, stamp FROM m_messages WHERE (device = 1 OR device IS NULL) AND "
			 		+ "(agency_id = 2 OR agency_id IS NULL) AND (device_id = 3 OR device_id IS NULL) AND (app_version = 4 OR app_version IS NULL) "
			 		+ "AND (NOW() >= start_date OR start_date IS NULL) AND (NOW() < end_date OR end_date IS NULL) "
			 		+ "AND (trigger_cond = 5 OR "
			 		+ "trigger_cond IS NULL) AND (SELECT COUNT(*) FROM m_popup_user_log WHERE device_id = 6 AND "
			 		+ "date_trunc( '3\\1533bfb25649bd25dd740b47c19b84e4', stamp) = 3) < 1ORDER BY num_conditions DESC LIMIT 1";
			 query = "select nm.trip_id,nm.id AS message_id, nm.message, nm.timestamp, nm.category,a.firstname AS first_name, a.lastname AS last_name "
				 		+ "from dv_notes_message nm, dv_account a, (SELECT dvNotes.trip_id, MAX(dvNotes.timestamp) AS maximum FROM dv_notes_message dvNotes WHERE "
				 		+ "dvNotes.agency_id IN (select c.agency_id from m_agency c, m_agency d where c.agency_id_id=d.agency_id_id and d.agency_id=1) "
				 		+ "AND dvNotes.trip_id IN ( '35\\89ad84e1a460f2041220847c65206b20', '33\\9a6cce223e3aa56cfc2128721095071b', "
				 		+ "'34\\57c15fbbf86f09cc1453e35c8c89a357', '33\\4d266129b208b1d32a7d75b9b89ec5ea', '35\\cbd995b9084ac97bc03ef0e7695d4b8d', "
				 		+ "'34\\d21fa0245cc37fec431d5591d6192e89')AND dvNotes.category= '4\\2da45b72d28efeb9a3954206d2ae2fa6' GROUP BY dvNotes.trip_id) "
				 		+ "as nmmax WHERE nm.deleted IS NULL AND a.id=nm.user_id AND nm.trip_id= nmmax.trip_id AND nm.timestamp = nmmax.maximum "
				 		+ "AND nm.agency_id IN (select c.agency_id from m_agency c, m_agency d where c.agency_id_id=d.agency_id_id and d.agency_id=2)"; 
			 
			//query = "SELECT a.agency_timezone FROM m_agency a WHERE a.agency_id = 80";
	*/
			query = "SELECT a.agency_timezone FROM m_agency a WHERE a.agency_id = 80";
//			query = "SELECT M.*, C.`option`, MIN(C.id) as component FROM jos_menu AS M LEFT JOIN jos_components AS C ON M.componentid = C.id "
//					+ "and M.name = C.name and M.ordering = C.ordering WHERE M.published = 1 and M.params=C.params GROUP BY M.sublevel HAVING M.lft = 2 "
//					+ "ORDER BY M.sublevel, M.parent, M.ordering";
			//query = "SELECT id FROM jos_menu WHERE `link` LIKE '%option=com_community&view=profile%' AND `published`=1";
			//query = "SELECT COUNT(*) as count from jos_community_questions as a LEFT JOIN jos_community_groups AS b ON a.parentid = b.id LEFT JOIN jos_community_groups_members AS c ON a.parentid = c.groupid WHERE a.id = 2902 AND (b.id IS NULL OR (b.id IS NOT NULL AND b.approvals=0))";
			//query = "SELECT m.*, c.`option`, MIN(c.id) as component FROM jos_menu AS m LEFT JOIN jos_components AS c ON m.componentid = c.id and m.name = c.name and m.ordering = c.ordering WHERE m.published = 1 and m.params=c.params GROUP BY m.sublevel HAVING m.lft = 2 ORDER BY m.sublevel, m.parent, m.ordering";
			//query = "SELECT AVG(menutype) from jos_menu";
			//query = "SELECT DISTINCT a.*, f.name AS creatorname, b.count, \"\" AS thumbnail, \"\" AS storage, 1 AS display, 1 AS privacy, b.last_updated FROM jos_community_photos_albums AS a LEFT JOIN ((SELECT id, approvals FROM jos_community_groups) UNION (SELECT id, approvals FROM jos_community_courses)) d ON a.groupid = d.id LEFT JOIN jos_community_groups_members AS c ON a.groupid = c.groupid LEFT JOIN (SELECT albumid, creator, COUNT(*) AS count, MAX(created) AS last_updated FROM jos_community_photos WHERE permissions = 0 OR (permissions = 2 AND (creator = 0 OR owner = 0)) GROUP BY albumid, creator) b ON a.id = b.albumid AND a.creator = b.creator INNER JOIN jos_users AS f ON a.creator = f.id WHERE (a.permissions = 0 OR (a.permissions = 2 AND (a.creator = 0 OR a.owner = 0))) AND (a.groupid = 0 OR (a.groupid > 0 AND (d.approvals = 0 OR (d.approvals = 1 AND c.memberid = 0))))";
			//query = "SELECT * from jos_menu AS m, jos_components AS c WHERE m.published = 1 and m.parent=\"X\"";
			//query = "UPDATE `jos_session` SET `time`='1538611062',`userid`='0',`usertype`='',`username`='',`gid`='0',`guest`='1',`client_id`='0',`data`='__default|a:9:{s:15:\\\"session.counter\\\";i:89;s:19:\\\"session.timer.start\\\";i:1538610776;s:18:\\\"session.timer.last\\\";i:1538611055;s:17:\\\"session.timer.now\\\";i:1538611060;s:22:\\\"session.client.browser\\\";s:71:\\\"Mozilla/5.0 (compatible; SEOkicks; +https://www.seokicks.de/robot.html)\\\";s:8:\\\"registry\\\";O:9:\\\"JRegistry\\\":3:{s:17:\\\"_defaultNameSpace\\\";s:7:\\\"session\\\";s:9:\\\"_registry\\\";a:1:{s:7:\\\"session\\\";a:1:{s:4:\\\"data\\\";O:8:\\\"stdClass\\\":0:{}}}s:7:\\\"_errors\\\";a:0:{}}s:4:\\\"user\\\";O:5:\\\"JUser\\\":19:{s:2:\\\"id\\\";i:0;s:4:\\\"name\\\";N;s:8:\\\"username\\\";N;s:5:\\\"email\\\";N;s:8:\\\"password\\\";N;s:14:\\\"password_clear\\\";s:0:\\\"\\\";s:8:\\\"usertype\\\";N;s:5:\\\"block\\\";N;s:9:\\\"sendEmail\\\";i:0;s:3:\\\"gid\\\";i:0;s:12:\\\"registerDate\\\";N;s:13:\\\"lastvisitDate\\\";N;s:10:\\\"activation\\\";N;s:6:\\\"params\\\";N;s:3:\\\"aid\\\";i:0;s:5:\\\"guest\\\";i:1;s:7:\\\"_params\\\";O:10:\\\"JParameter\\\":7:{s:4:\\\"_raw\\\";s:0:\\\"\\\";s:4:\\\"_xml\\\";N;s:9:\\\"_elements\\\";a:0:{}s:12:\\\"_elementPath\\\";a:1:{i:0;s:58:\\\"/var/www/html/minc/libraries/joomla/html/parameter/element\\\";}s:17:\\\"_defaultNameSpace\\\";s:8:\\\"_default\\\";s:9:\\\"_registry\\\";a:1:{s:8:\\\"_default\\\";a:1:{s:4:\\\"data\\\";O:8:\\\"stdClass\\\":0:{}}}s:7:\\\"_errors\\\";a:0:{}}s:9:\\\"_errorMsg\\\";N;s:7:\\\"_errors\\\";a:0:{}}s:8:\\\"view-926\\\";b:1;s:13:\\\"session.token\\\";s:32:\\\"50cf27c9c56d1d64c5a1203e192fc4e6\\\";}' WHERE session_id='buledanlab7lhtd5tpc6jcp5t5'";
			//query = "INSERT INTO `jos_session` ( `session_id`,`time`,`username`,`gid`,`guest`,`client_id` ) VALUES ( '7susns2ghsr6du1vic7g8cgja2','1538611066','','0','1','0' )";
			//query = "DELETE FROM jos_session WHERE ( time < '1538607473' )";
			//query = "SELECT COUNT(*) as count from jos_community_questions as a LEFT JOIN jos_community_groups AS b ON a.parentid = b.id LEFT JOIN jos_community_groups_members AS c ON a.parentid = c.groupid WHERE a.id = 331 AND (b.id IS NULL OR (b.id IS NOT NULL AND b.approvals=0))";
			//query = "UPDATE jos_users SET lastvisitDate = \"2018-10-05 16\"";
			//query = "INSERT INTO `jos_session` ( `session_id`,`time`,`username`,`gid`,`guest`,`client_id` ) VALUES ( 'ok12387ga01bj1pi49f9ffbuh0','1540013023','','0','1','0' )";
			//query = "SELECT count(*) FROM jos_community_usefulness WHERE resourceid = 925";
			//query = "SELECT id, title, module, position, content, showtitle, control, params FROM jos_modules AS m LEFT JOIN jos_modules_menu AS mm ON mm.moduleid = m.id WHERE m.published = 1 AND m.access <= 0 AND m.client_id = 0 AND ( mm.menuid = 53 OR mm.menuid = 0 ) ORDER BY position, ordering";
			//query = "SELECT a.`userid` as _userid , a.`status` as _status , a.`level` as _level , a.`points` as _points, a.`posted_on` as _posted_on, a.`avatar` as _avatar , a.`thumb` as _thumb , a.`invite` as _invite, a.`params` as _cparams, a.`view` as _view, a.`friendcount` as _friendcount, a.`alias` as _alias, s.`userid` as _isonline, u.* FROM jos_community_users as a LEFT JOIN jos_users u ON u.`id`=a.`userid` LEFT OUTER JOIN jos_session s ON s.`userid`=a.`userid` AND s.client_id !='1'WHERE a.`userid`='0'";
			//query = "SELECT * from jos_community_fields_values where value = 'MINC'";
			//query = "SELECT count(*)  FROM jos_community_connection as a, jos_users as b WHERE a.`connect_from`='3637' AND a.`status`=1  AND a.`connect_to`=b.`id` AND NOT EXISTS ( SELECT d.`blocked_userid` FROM `jos_community_blocklist` AS d WHERE d.`userid` = '3637' AND d.`blocked_userid` = a.`connect_to`)  ORDER BY a.`connection_id` DESC";
			//query = "SELECT COUNT(*) FROM `jos_community_groups_members` AS a INNER JOIN `jos_users` AS b WHERE b.id=a.memberid AND a.memberid NOT IN (SELECT userid from jos_community_courses_ta where courseid = 3569) AND a.groupid='3569' AND a.permissions='1'";
			//query = "SELECT DISTINCT a.* FROM jos_community_apps AS a WHERE a.`userid`='72' AND a.`apps`!=\"news_feed\" AND a.`apps`!=\"profile\" AND a.`apps`!=\"friends\" AND a.`apps` IN ('walls') ORDER BY a.`ordering`";
			//query = "SELECT          a.`userid` as _userid ,         a.`status` as _status ,         a.`level`  as _level ,  a.`points`      as _points,     a.`posted_on` as _posted_on,    a.`avatar`      as _avatar ,    a.`thumb`       as _thumb ,     a.`invite`      as _invite,     a.`params`      as _cparams,    a.`view`        as _view,  a.`alias`    as _alias,  a.`friendcount` as _friendcount, s.`userid` as _isonline, u.*  FROM jos_community_users as a  LEFT JOIN jos_users u  ON u.`id`=a.`userid`  LEFT OUTER JOIN jos_session s  ON s.`userid`=a.`userid` WHERE a.`userid` IN (2839,2824,2828)";
			MINCFragmentIntent fragmentObj = new MINCFragmentIntent(query, schParse, includeSelOpConst, dataset);
			boolean validQuery = fragmentObj.parseQueryAndCreateFragmentVectors();
			if(validQuery) {
				fragmentObj.printIntentVector();
				fragmentObj.writeIntentVectorToTempFile(query);
			} 

	
		} catch(Exception e) {
			e.printStackTrace();
		} 		
	}
	
}

/**
 * //readFrom100KFile(queryFile, line, prevSessionID, schParse, queryID);
 * //readFromConcurrentSessionsFile(concSessFile, intentVectorFile, line, schParse);
 * public static void mainOld(String[] args) {
		String queryFile = "/Users/postgres/var/data/CreditCardDataset/NYCCleanedSessions";
		try {
			BufferedReader br = new BufferedReader(new FileReader(queryFile));
			String line = null;
			int sessionID = 1;
			int absQueryID = 1;
			while((line=br.readLine())!=null) {
				String[] queries = line.split(";");
				for(int i=1;i<queries.length-1;i++) {
					String query = queries[i].split("~")[0];
					SQLTokenizer tok = new SQLTokenizer(query);
					tok.parseQueryFetchOpLists();
					System.out.println("Covered SessionID: "+sessionID+", queryID: "+i+", #Queries so far: "+absQueryID);
					absQueryID++;
				}
				sessionID++;
				if(sessionID==3)
					return;
			}
		} catch(Exception e) {
			e.printStackTrace();
		}
	}
	
	public String createBitVectorForOpColSet(HashSet<Column> colSet) throws Exception {
		String b = "";
		if (colSet.size()==1 && colSet.iterator().next().toString().equals("*")) {
				b = setAllColumns();
				appendToBitVectorString(b);
				return b;
		}
		HashMap<String,ArrayList<String>> tableColumnDict = createTableColumnDict(colSet);
		HashMap<String,Integer> schemaTables = this.schParse.fetchMINCTables();
		HashSet<String> tableNames = new HashSet<String>(schemaTables.keySet());
		for(String tableName:tableNames) {
			String bitMapPerTable;
			if(tableColumnDict.containsKey(tableName))
				bitMapPerTable = this.setColumnsFromTable(tableName, tableColumnDict.get(tableName));
			else
				bitMapPerTable = this.setColumnsFromTable(tableName, null);
			b+=bitMapPerTable;
		}
		this.appendToBitVectorString(b);
		return b;
	}
	
	public BitSet setSelPredOpForCol(String colName, ArrayList<String> selPredColOps, BitSet b, int i) throws Exception{
		for(String selPredColOp : selPredColOps) {
			assert selPredColOp.split(".").length == 2; // colName.Op
			String selPredCol = selPredColOp.split(".")[0];
			String selPredOp = selPredColOp.split(".")[1];
			if(colName.equals(selPredCol)) {
				int opIndex = Arrays.asList(this.selPredOpList).indexOf(selPredOp);
				b.set(i+opIndex);
			}
		}
		return b;
	}
	
	public String setSelPredOpsFromTable(String tableName, ArrayList<String> selPredColOps) throws Exception{
		HashMap<String,String> MINCColumns = this.schParse.fetchMINCColumns();
		String[] colArray = cleanColArrayString(MINCColumns.get(tableName));
		BitSet b = new BitSet(colArray.length * this.selPredOpList.length);
		if(selPredColOps != null) {
			for(int i=0; i<colArray.length; i++) {
				b = setSelPredOpForCol(colArray[i], selPredColOps, b, i);
			}
		}
		return toString(b,colArray.length);
	}
	
	public void createBitVectorForSelPredOpsOld() throws Exception {
		String b = "";
		HashMap<String, ArrayList<String>> selPredOpMap = new HashMap<String, ArrayList<String>>();
		for(Column c : this.selPredOps.keySet()) {
			String opVal = this.selPredOps.get(c);
			toolsForMetrics.Pair<String, String> tabColName = retrieveTabColName(c);
			String tableName = tabColName.getKey();
			String colName = tabColName.getValue();
			if(!selPredOpMap.containsKey(tableName)) {
				ArrayList<String> newOpMapEntry = new ArrayList<String>();
				selPredOpMap.put(tableName, newOpMapEntry);
			}
			ArrayList<String> tempOpEntry = selPredOpMap.get(tableName);
			tempOpEntry.add(colName+"."+opVal);
			selPredOpMap.put(tableName, tempOpEntry);
		}
		HashMap<String,Integer> schemaTables = this.schParse.fetchMINCTables();
		for(String tableName:schemaTables.keySet()) {
			String bitMapPerTable;
			if(selPredOpMap.containsKey(tableName)) {
				bitMapPerTable = this.setSelPredOpsFromTable(tableName, selPredOpMap.get(tableName));
			}
			else
				bitMapPerTable = this.setSelPredOpsFromTable(tableName, null);
			b += bitMapPerTable;
		}
		this.appendToBitVectorString(b);
		this.selPredOpBitMap = b;
	}
 * */
