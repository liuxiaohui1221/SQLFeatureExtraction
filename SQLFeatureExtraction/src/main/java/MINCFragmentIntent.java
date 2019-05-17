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
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.AllColumns;
import net.sf.jsqlparser.statement.select.AllTableColumns;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubSelect;
//import net.sf.jsqlparser.statement.select.Union;
import net.sf.jsqlparser.statement.select.WithItem;
import net.sf.jsqlparser.statement.update.Update;
import toolsForMetrics.ExtendedColumn;
import toolsForMetrics.Global;
import toolsForMetrics.Schema;
import toolsForMetrics.SelectItemListParser;
import toolsForMetrics.Util;
import net.sf.jsqlparser.expression.AllComparisonExpression;
import net.sf.jsqlparser.expression.AnyComparisonExpression;
import net.sf.jsqlparser.expression.BinaryExpression;
import net.sf.jsqlparser.expression.CaseExpression;
import net.sf.jsqlparser.expression.Expression;
import net.sf.jsqlparser.expression.Function;
//import net.sf.jsqlparser.expression.InverseExpression;
import net.sf.jsqlparser.expression.Parenthesis;
import net.sf.jsqlparser.expression.StringValue;
import net.sf.jsqlparser.expression.WhenClause;
import net.sf.jsqlparser.expression.operators.arithmetic.Addition;
import net.sf.jsqlparser.expression.operators.arithmetic.Division;
import net.sf.jsqlparser.expression.operators.arithmetic.Multiplication;
import net.sf.jsqlparser.expression.operators.arithmetic.Subtraction;
import net.sf.jsqlparser.expression.operators.conditional.AndExpression;
import net.sf.jsqlparser.expression.operators.conditional.OrExpression;
import net.sf.jsqlparser.expression.operators.relational.Between;
import net.sf.jsqlparser.expression.operators.relational.EqualsTo;
import net.sf.jsqlparser.expression.operators.relational.ExistsExpression;
import net.sf.jsqlparser.expression.operators.relational.ExpressionList;
import net.sf.jsqlparser.expression.operators.relational.GreaterThan;
import net.sf.jsqlparser.expression.operators.relational.GreaterThanEquals;
import net.sf.jsqlparser.expression.operators.relational.InExpression;
import net.sf.jsqlparser.expression.operators.relational.IsNullExpression;
import net.sf.jsqlparser.expression.operators.relational.ItemsList;
import net.sf.jsqlparser.expression.operators.relational.LikeExpression;
import net.sf.jsqlparser.expression.operators.relational.MinorThan;
import net.sf.jsqlparser.expression.operators.relational.MinorThanEquals;
import net.sf.jsqlparser.expression.operators.relational.NotEqualsTo;
import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.parser.ParseException;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.select.Distinct;
import net.sf.jsqlparser.statement.select.FromItem;
import net.sf.jsqlparser.statement.select.Join;
import net.sf.jsqlparser.statement.select.OrderByElement;
import net.sf.jsqlparser.statement.select.PlainSelect;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.select.SelectBody;
import net.sf.jsqlparser.statement.select.SelectExpressionItem;
import net.sf.jsqlparser.statement.select.SelectItem;
import net.sf.jsqlparser.statement.select.SubJoin;
import net.sf.jsqlparser.statement.select.SubSelect;
import java.net.InetAddress;
import java.net.UnknownHostException;
public class MINCFragmentIntent{
	String originalSQL;
	Statement statement;
	String intentBitVector;
	List<Table> tables = new ArrayList<Table>();
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
	public static HashMap<String, String> tableAlias = Global.tableAlias;
	String queryType; // select , insert, update, delete
	SchemaParser schParse; // used for retrieval of schema related information
	public MINCFragmentIntent(String originalSQL, SchemaParser schParse) throws Exception{
		this.originalSQL = originalSQL.toLowerCase();
		this.schParse = schParse;
		Global.tableAlias = new HashMap<String, String>();
		InputStream stream = new ByteArrayInputStream(this.originalSQL.getBytes(StandardCharsets.UTF_8));
		CCJSqlParser parser = new CCJSqlParser(stream);
		this.statement = null;
		this.intentBitVector = null;
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
	}
	
	public void populateOperatorObjects(SQLParser parser) throws Exception{
		this.tables = parser.getTables();
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
	}
	
	public void parseQuery() throws Exception {
		SQLParser parser = new SQLParser(this.originalSQL);
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
		if(this.intentBitVector == null) {
			this.intentBitVector = toString(b,4);
			this.queryTypeBitMap = toString(b,4);
		}
		else{
			System.out.println("Invalid intent bitvector!!");
		//	System.exit(0);
		}			
	}
	
	public void appendToBitVectorString(String b) throws Exception{
		if(this.intentBitVector == null) {
			System.out.println("Invalid intent bitvector!!");
		//	System.exit(0);
		}
		else {
			this.intentBitVector += b.toString();
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
		appendToBitVectorString(toString(b,MINCTables.size()));
	}
	
	public boolean checkIfTableExists(String tableName) throws Exception{
		for(Table tab:this.tables) {
			String queryTableName = this.cleanString(tab.getName().toLowerCase());
			if (tableName.equals(queryTableName))
				return true;
		}
		return false;
	}
	
	public String[] cleanColArrayString(String colArray) throws Exception{
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
			String[] colArray = cleanColArrayString(MINCColumns.get(table.getName().toLowerCase()));
			for(int i=0; i<colArray.length; i++) {
				if (colArray[i].equals(colName))
					return table.getName();
			}
		}
		return null;
	}
	
	public HashMap<String,ArrayList<String>> createTableColumnDict(HashSet<Column> colSet) throws Exception{
		HashMap<String,ArrayList<String>> tableColumnDict = new HashMap<String,ArrayList<String>>();
		for(Column c:colSet) {
			String fullName = c.toString().replace("`", "");
			String tableName;
			String colName = fullName;
			if(fullName.contains(".")) {
				String[] tokens = fullName.split("\\.");
				assert tokens.length == 2;
				String tableNameAlias = tokens[0].toLowerCase();
				colName = tokens[1].toLowerCase();
				tableName = tableNameAlias; // if there is no tableAlias tableName is being used
				if (Global.tableAlias.size()>0) 
					tableName = Global.tableAlias.get(tableNameAlias).toLowerCase();	
			}
			else {
				// there should be a single table name in the from clause, else simply search for the first table name
				if(this.tables.size()==1) 
					tableName = this.tables.get(0).getName().toLowerCase();
				else
					tableName = searchColDictForTableName(colName.toLowerCase());
				if(tableName == null)
					continue;
			}
			if (!tableColumnDict.containsKey(tableName))
				tableColumnDict.put(tableName, new ArrayList<String>());
			tableColumnDict.get(tableName).add(colName);		
		}
		return tableColumnDict;
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
				String fullName = c.toString().replace("`", "");
				String tableName;
				String colName = fullName.toLowerCase();
				if(fullName.contains(".")) {
					String tableNameAlias = fullName.split("\\.")[0].toLowerCase();
					colName = fullName.split("\\.")[1].toLowerCase();
					tableName = tableNameAlias; // if there is no tableAlias tableName is being used
					if (Global.tableAlias.size()>0) 
						tableName = Global.tableAlias.get(tableNameAlias).toLowerCase();	
				}
				else {
					// there should be a single table name in the from clause, else simply search for the first table name
					if(this.tables.size()==1) 
						tableName = this.tables.get(0).getName().toLowerCase();
					else
						tableName = searchColDictForTableName(colName);
					if(tableName == null)
						continue;
				}
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
		pairToSet.add(set1.getKey().trim());
		pairToSet.add(set1.getValue().trim());
		
	/*	if(pairToSet.contains("params")) {
			 int a = 1;
		}
		*/
        
       return Util.equals(pairToSet, set2);
	}
	
	public void createBitVectorForJoin() throws Exception{
		//key is tablePair and value is a list of column pairs
		HashMap<HashSet<String>,ArrayList<HashSet<String>>> joinPredDictQuery = convertColumnListToStringSet();
		HashMap<String,ArrayList<Pair<String,String>>> joinPredDictSchema = this.schParse.fetchMINCJoinPreds();
		HashMap<String,Pair<Integer,Integer>> joinPredBitPosSchema = this.schParse.fetchMINCJoinPredBitPos();
		HashSet<Integer> bitPosToSet = new HashSet<Integer>();
		for(HashSet<String> tablePairQuery : joinPredDictQuery.keySet()) {
			String dictKey = locateHashSetInKeys(tablePairQuery, new HashSet<String>(joinPredDictSchema.keySet()));
			ArrayList<HashSet<String>> joinPredListQuery = joinPredDictQuery.get(tablePairQuery);
			ArrayList<Pair<String,String>> joinPredListSchema = joinPredDictSchema.get(dictKey);
			Pair<Integer,Integer> startEndBitPos = joinPredBitPosSchema.get(dictKey);
			for(HashSet<String> joinPredQuery : joinPredListQuery) {
				int joinPredSchemaIndex = 0;
				for(Pair<String,String> joinPredSchema : joinPredListSchema) {
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
		createBitVectorForJoin();
		//System.out.println("this.joinPredBitMap: "+this.joinPredicatesBitMap+", length: "+this.joinPredicatesBitMap.toCharArray().length);
	}
	
	public boolean parseQueryAndCreateFragmentVectors() throws Exception {
		if(this.queryType == "select" || this.queryType == "update" || this.queryType == "insert" || this.queryType == "delete") {
			try {
				this.parseQuery();
				this.createFragmentVectors();
			} catch(Exception e) {
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
	public static void readFrom100KFile(String queryFile, String line, String prevSessionID, SchemaParser schParse, int queryID) throws Exception{
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
				MINCFragmentIntent fragmentObj = new MINCFragmentIntent(query, schParse);
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
	
	public static ArrayList<String> countLines(String rawSessFile, int startLineNum) throws Exception{
		System.out.println("Counting lines from "+rawSessFile);
		BufferedReader br = new BufferedReader(new FileReader(rawSessFile));
		ArrayList<String> lines = new ArrayList<String>();
		String line = null;
		int i=0;
		int absCount = 0;
		while ((line=br.readLine())!=null  && absCount<300000+startLineNum) {
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
	
	public static ArrayList<Pair<Integer,Integer>> readLinesPerThread(int lowerIndexPerThread, int curThreadIndex, int numThreads, int numLinesPerThread, ArrayList<String> sessQueries, ArrayList<Pair<Integer,Integer>> inputSplits) throws Exception{
		System.out.println("Splitting lines for thread "+curThreadIndex+", numLinesPerThread: "+numLinesPerThread+" with lower Index: "+lowerIndexPerThread);
		int i=0;
		int runningIndex = Math.min(lowerIndexPerThread+numLinesPerThread-1, sessQueries.size()-1);
		String line = sessQueries.get(runningIndex);
		String prevSessID = line.split(" ")[0];
		String curSessID = null;
		runningIndex++;
		while(runningIndex<sessQueries.size()){
			curSessID = sessQueries.get(runningIndex).split(" ")[0];
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
	
	public static ArrayList<Pair<Integer,Integer>> splitInputAcrossThreads(ArrayList<String> sessQueries, int numThreads) throws Exception{	
		assert numThreads>0;
		int numLinesPerThread = sessQueries.size()/numThreads;
		assert numLinesPerThread > 1;
		ArrayList<Pair<Integer,Integer>> inputSplits = new ArrayList<Pair<Integer,Integer>>();
		int lowerIndexPerThread = 0;
		for(int i=0; i<numThreads; i++) {
			inputSplits = readLinesPerThread(lowerIndexPerThread, i, numThreads, numLinesPerThread, sessQueries, inputSplits);
			lowerIndexPerThread = inputSplits.get(i).getValue()+1; // upper index of data for current thread +1 will be the lower index for the next thread
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
	
	
	public static void readFromRawSessionsFile(String tempLogDir, String rawSessFile, String intentVectorFile, String line, SchemaParser schParse, int numThreads, int startLineNum, String pruneKeepModifyRepeatedQueries) throws Exception{
	//	deleteIfExists(intentVectorFile);
	//	System.out.println("Deleted previous intent file");
		ArrayList<String> sessQueries = countLines(rawSessFile, startLineNum);
		System.out.println("Read sessQueries into main memory");
		ArrayList<Pair<Integer,Integer>> inputSplits = splitInputAcrossThreads(sessQueries, numThreads);
		System.out.println("Split Input Across Threads");
		ArrayList<String> outputSplitFiles = defineOutputSplits(tempLogDir, rawSessFile, numThreads);
		System.out.println("Defined Output File Splits Across Threads");
		ArrayList<IntentCreatorMultiThread> intentMTs = new ArrayList<IntentCreatorMultiThread>();
		for(int i=0; i<numThreads; i++) {
			IntentCreatorMultiThread intentMT = new IntentCreatorMultiThread(i, sessQueries, inputSplits.get(i), outputSplitFiles.get(i), schParse, pruneKeepModifyRepeatedQueries);
			intentMT.start();		
		}
	/*	for(IntentCreatorMultiThread intentMT : intentMTs) {
			intentMT.join();
		}
		*/
	//	concatenateOutputFiles(outputSplitFiles, intentVectorFile);
	}
	
	public static void readFromConcurrentSessionsFile(String concSessFile, String intentVectorFile, String line, SchemaParser schParse) throws Exception{
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
				
				MINCFragmentIntent fragmentObj = new MINCFragmentIntent(query, schParse);
				boolean validQuery = fragmentObj.parseQueryAndCreateFragmentVectors();
				/*if(validQuery)
					fragmentObj.printIntentVector();*/
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
		HashMap<String, String> configDict = schParse.getConfigDict();
		String queryFile = "/Users/postgres/PycharmProjects/HumanIntentEvaluation/sample_100K.log";
		String concSessFile = configDict.get("MINC_CONC_SESS_FILE"); // already configDict prepends the homeDir
		String intentVectorFile = configDict.get("MINC_FRAGMENT_INTENT_VECTOR_FILE");
		String rawSessFile = configDict.get("MINC_RAW_SESS_FILE");
		String tempLogDir = configDict.get("MINC_TEMP_LOG_DIR");
		int numThreads = Integer.parseInt(configDict.get("MINC_NUM_THREADS"));
		int startLineNum = Integer.parseInt(configDict.get("MINC_START_LINE_NUM"));
		String pruneKeepModifyRepeatedQueries = configDict.get("MINC_KEEP_PRUNE_MODIFY_REPEATED_QUERIES");
		try {
			String line = null;
			String prevSessionID = null;
			int queryID = 0;
			
			//uncomment the following when full run needs to happen on EC2 or on EN4119510L
			readFromRawSessionsFile(tempLogDir, rawSessFile, intentVectorFile, line, schParse, numThreads, startLineNum, pruneKeepModifyRepeatedQueries);
			
	/*		String query = "SELECT M.*, C.`option`, MIN(C.id) as component FROM jos_menu AS M LEFT JOIN jos_components AS C ON M.componentid = C.id and M.name = C.name and M.ordering = C.ordering WHERE M.published = 1 and M.params=C.params GROUP BY M.sublevel HAVING M.lft = 2 ORDER BY M.sublevel, M.parent, M.ordering";
			//query = "SELECT m.*, c.`option`, MIN(c.id) as component FROM jos_menu AS m LEFT JOIN jos_components AS c ON m.componentid = c.id and m.name = c.name and m.ordering = c.ordering WHERE m.published = 1 and m.params=c.params GROUP BY m.sublevel HAVING m.lft = 2 ORDER BY m.sublevel, m.parent, m.ordering";
			//query = "SELECT AVG(menutype) from jos_menu";
			//query = "SELECT DISTINCT a.*, f.name AS creatorname, b.count, \"\" AS thumbnail, \"\" AS storage, 1 AS display, 1 AS privacy, b.last_updated FROM jos_community_photos_albums AS a LEFT JOIN ((SELECT id, approvals FROM jos_community_groups) UNION (SELECT id, approvals FROM jos_community_courses)) d ON a.groupid = d.id LEFT JOIN jos_community_groups_members AS c ON a.groupid = c.groupid LEFT JOIN (SELECT albumid, creator, COUNT(*) AS count, MAX(created) AS last_updated FROM jos_community_photos WHERE permissions = 0 OR (permissions = 2 AND (creator = 0 OR owner = 0)) GROUP BY albumid, creator) b ON a.id = b.albumid AND a.creator = b.creator INNER JOIN jos_users AS f ON a.creator = f.id WHERE (a.permissions = 0 OR (a.permissions = 2 AND (a.creator = 0 OR a.owner = 0))) AND (a.groupid = 0 OR (a.groupid > 0 AND (d.approvals = 0 OR (d.approvals = 1 AND c.memberid = 0))))";
			//query = "SELECT * from jos_menu AS m, jos_components AS c WHERE m.published = 1 and m.parent=c.parent";
			//query = "UPDATE `jos_session` SET `time`='1538611062',`userid`='0',`usertype`='',`username`='',`gid`='0',`guest`='1',`client_id`='0',`data`='__default|a:9:{s:15:\\\"session.counter\\\";i:89;s:19:\\\"session.timer.start\\\";i:1538610776;s:18:\\\"session.timer.last\\\";i:1538611055;s:17:\\\"session.timer.now\\\";i:1538611060;s:22:\\\"session.client.browser\\\";s:71:\\\"Mozilla/5.0 (compatible; SEOkicks; +https://www.seokicks.de/robot.html)\\\";s:8:\\\"registry\\\";O:9:\\\"JRegistry\\\":3:{s:17:\\\"_defaultNameSpace\\\";s:7:\\\"session\\\";s:9:\\\"_registry\\\";a:1:{s:7:\\\"session\\\";a:1:{s:4:\\\"data\\\";O:8:\\\"stdClass\\\":0:{}}}s:7:\\\"_errors\\\";a:0:{}}s:4:\\\"user\\\";O:5:\\\"JUser\\\":19:{s:2:\\\"id\\\";i:0;s:4:\\\"name\\\";N;s:8:\\\"username\\\";N;s:5:\\\"email\\\";N;s:8:\\\"password\\\";N;s:14:\\\"password_clear\\\";s:0:\\\"\\\";s:8:\\\"usertype\\\";N;s:5:\\\"block\\\";N;s:9:\\\"sendEmail\\\";i:0;s:3:\\\"gid\\\";i:0;s:12:\\\"registerDate\\\";N;s:13:\\\"lastvisitDate\\\";N;s:10:\\\"activation\\\";N;s:6:\\\"params\\\";N;s:3:\\\"aid\\\";i:0;s:5:\\\"guest\\\";i:1;s:7:\\\"_params\\\";O:10:\\\"JParameter\\\":7:{s:4:\\\"_raw\\\";s:0:\\\"\\\";s:4:\\\"_xml\\\";N;s:9:\\\"_elements\\\";a:0:{}s:12:\\\"_elementPath\\\";a:1:{i:0;s:58:\\\"/var/www/html/minc/libraries/joomla/html/parameter/element\\\";}s:17:\\\"_defaultNameSpace\\\";s:8:\\\"_default\\\";s:9:\\\"_registry\\\";a:1:{s:8:\\\"_default\\\";a:1:{s:4:\\\"data\\\";O:8:\\\"stdClass\\\":0:{}}}s:7:\\\"_errors\\\";a:0:{}}s:9:\\\"_errorMsg\\\";N;s:7:\\\"_errors\\\";a:0:{}}s:8:\\\"view-926\\\";b:1;s:13:\\\"session.token\\\";s:32:\\\"50cf27c9c56d1d64c5a1203e192fc4e6\\\";}' WHERE session_id='buledanlab7lhtd5tpc6jcp5t5' and published=1";
			//query = "INSERT INTO `jos_session` ( `session_id`,`time`,`username`,`gid`,`guest`,`client_id` ) VALUES ( '7susns2ghsr6du1vic7g8cgja2','1538611066','','0','1','0' )";
			//query = "DELETE FROM jos_session WHERE ( time < '1538607473' )";
			MINCFragmentIntent fragmentObj = new MINCFragmentIntent(query, schParse);
			boolean validQuery = fragmentObj.parseQueryAndCreateFragmentVectors();
			if(validQuery) {
				fragmentObj.printIntentVector();
				fragmentObj.writeIntentVectorToTempFile(query);
			}
	*/
		} catch(Exception e) {
			e.printStackTrace();
		}			
	}
	
}

/**
 * //readFrom100KFile(queryFile, line, prevSessionID, schParse, queryID);
 * //readFromConcurrentSessionsFile(concSessFile, intentVectorFile, line, schParse);
 * public static void mainOld(String[] args) {
		String queryFile = "/Users/postgres/Documents/DataExploration-Research/CreditCardDataset/NYCCleanedSessions";
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
 * */
