import net.sf.jsqlparser.parser.CCJSqlParser;
import net.sf.jsqlparser.schema.Column;
import net.sf.jsqlparser.schema.Table;
import net.sf.jsqlparser.statement.Statement;
import net.sf.jsqlparser.statement.delete.Delete;
import net.sf.jsqlparser.statement.insert.Insert;
import net.sf.jsqlparser.statement.select.Select;
import net.sf.jsqlparser.statement.update.Update;
import org.apache.calcite.avatica.util.Quoting;
import org.apache.calcite.config.Lex;
import org.apache.calcite.config.NullCollation;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.ClickHouseSqlDialect;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;
import reader.DDLParser;
import reader.ExcelReader;
import reader.StringCleaner;
import toolsForMetrics.Global;
import toolsForMetrics.Pair;
import toolsForMetrics.Util;

import java.io.*;
import java.net.InetAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.BitSet;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class APMFragmentIntent
{
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
  HashMap<Column, ArrayList<String>> selPredOps;
  HashMap<Column, ArrayList<String>> selPredConstants;
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
  String[] selPredOpList = new String[]{"=", "<>", "<=", ">=", "<", ">", "LIKE"};
  String selPredColRangeBinBitMap;
  public static HashMap<String, String> tableAlias = Global.tableAlias;
  String queryType; // select , insert, update, delete
  SchemaParser schParse; // used for retrieval of schema related information
  boolean includeSelOpConst; // decide whether or not to include selOpConst
  String dataset; // dataset info

  List<String> specialColumns = Arrays.asList("group1");

  public APMFragmentIntent(String originalSQL, SchemaParser schParse, boolean includeSelOpConst, String dataset)
      throws Exception
  {
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
      if (this.statement instanceof Select) {
        this.queryType = "select";
      } else if (statement instanceof Update) {
        this.queryType = "update";
      } else if (statement instanceof Insert) {
        this.queryType = "insert";
      } else if (statement instanceof Delete) {
        this.queryType = "delete";
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static String getMachineName()
  {
    String hostname = null;
    try {
      InetAddress addr;
      addr = InetAddress.getLocalHost();
      hostname = addr.getHostName();
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    return hostname;
  }

  public String getIntentBitVector()
  {
    return this.intentBitVector;
  }

  public String cleanString(String str) throws Exception
  {
    str = str.replace("'", "").replace("`", "").trim();
    str = str.replace("[", "");
    str = str.replace("]", "");
    return str;
  }

  public String toString(BitSet b, int size) throws Exception
  {
    String to_return = "";
    for (int i = 0; i < size; i++) {
      if (b.get(i)) {
        to_return += "1";
      } else {
        to_return += "0";
      }
    }
    return to_return;
  }

  public void writeIntentVectorToTempFile(String query) throws Exception
  {
    String fileName = "tempVector";
    this.deleteIfExists(fileName);
    BufferedWriter bw = new BufferedWriter(new FileWriter(fileName, true));
    bw.append("query:" + query + "\n");
    bw.append("intentVector:" + this.intentBitVector + "\n");
    bw.append("queryTypeBitMap:" + this.queryTypeBitMap + "\n");
    bw.append("TableBitMap:" + this.tableBitMap + "\n");
    bw.append("SelectionBitMap:" + this.selectionBitMap + "\n");
    bw.append("GroupByBitMap:" + this.groupByBitMap + "\n");
    bw.append("OrderByBitMap:" + this.orderByBitMap + "\n");
    bw.append("ProjectionBitMap:" + this.projectionBitMap + "\n");
    bw.append("HavingBitMap:" + this.havingBitMap + "\n");
    bw.append("JoinPredicatesBitMap:" + this.joinPredicatesBitMap + "\n");
    bw.append("LimitBitMap:" + this.limitBitMap + "\n");
    bw.append("MINBitMap:" + this.MINBitMap + "\n");
    bw.append("MAXBitMap:" + this.MAXBitMap + "\n");
    bw.append("AVGBitMap:" + this.AVGBitMap + "\n");
    bw.append("SUMBitMap:" + this.SUMBitMap + "\n");
    bw.append("COUNTBitMap:" + this.COUNTBitMap + "\n");
    if (this.includeSelOpConst) {
      bw.append("selPredOpBitMap:" + this.selPredOpBitMap + "\n");
      bw.append("selPredColRangeBinBitMap:" + this.selPredColRangeBinBitMap + "\n");
    }
    bw.flush();
    bw.close();
  }

  public void printIntentVector() throws Exception
  {
    System.out.println("Printing FragmentBitVector");
    System.out.println(this.intentBitVector);
    System.out.println("----OPERATOR-WISE FRAGMENT------");
    System.out.println("queryTypeBitMap: " + this.queryTypeBitMap);
    System.out.println("TableBitMap: " + this.tableBitMap);
    System.out.println("TableBitMap: " + this.tableBitMap);
    System.out.println("SelectionBitMap:" + this.selectionBitMap);
    System.out.println("OrderByBitMap: " + this.orderByBitMap);
    System.out.println("ProjectionBitMap: " + this.projectionBitMap);
    System.out.println("HavingBitMap: " + this.havingBitMap);
    System.out.println("JoinPredicatesBitMap: " + this.joinPredicatesBitMap);
    System.out.println("LimitBitMap: " + this.limitBitMap);
    System.out.println("MINBitMap: " + this.MINBitMap);
    System.out.println("MAXBitMap: " + this.MAXBitMap);
    System.out.println("AVGBitMap: " + this.AVGBitMap);
    System.out.println("SUMBitMap: " + this.SUMBitMap);
    System.out.println("COUNTBitMap: " + this.COUNTBitMap);
    if (this.includeSelOpConst) {
      System.out.println("selPredOpBitMap:" + this.selPredOpBitMap);
      System.out.println("selPredColRangeBinBitMap:" + this.selPredColRangeBinBitMap);
    }
  }

  public void populateOperatorObjects(SQLParser parser) throws Exception
  {
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
    if (this.includeSelOpConst) {
      this.selPredOps = parser.getSelPredOps();
      this.selPredConstants = parser.getSelPredConstants();
    }
  }

  public void parseQuery() throws Exception
  {
    SQLParser parser = new SQLParser(this.schParse, this.originalSQL, this.includeSelOpConst, this.dataset);
    parser.createQueryVector(this.statement);
    populateOperatorObjects(parser);
  }

  public void createBitVectorForQueryTypes() throws Exception
  {
    BitSet b = new BitSet(4);
    if (this.queryType.equals("select")) {
      b.set(0);
    } else if (this.queryType.equals("update")) {
      b.set(1);
    } else if (this.queryType.equals("insert")) {
      b.set(2);
    } else if (this.queryType.equals("delete")) {
      b.set(3);
    } else {
      System.out.println("Invalid queryType!!");
      //	System.exit(0);
    }
    if (this.intentBitVecBuilder.length() == 0) {
      this.queryTypeBitMap = toString(b, 4);
      this.intentBitVecBuilder.append(this.queryTypeBitMap);
      //this.intentBitVector = toString(b,4);

    } else {
      System.out.println("Invalid intent bitvector!!");
      //	System.exit(0);
    }
  }

  public void appendToBitVectorString(String b) throws Exception
  {
    if (this.intentBitVecBuilder.length() == 0) {
      System.out.println("Invalid intent bitvector!!");
      //	System.exit(0);
    } else {
      this.intentBitVecBuilder.append(b);
      //this.intentBitVector += b;
    }
  }

  public void createBitVectorForTables() throws Exception
  {
    HashMap<String, Integer> MINCTables = this.schParse.fetchMINCTables();
    BitSet b = new BitSet(MINCTables.size());
    for (Table tab : this.tables) {
      try {
        String tableName = this.cleanString(tab.getName().toLowerCase());
        int tableIndex = MINCTables.get(tableName);
        b.set(tableIndex);
      }catch (Exception e) {
        e.printStackTrace();
      }
    }
    this.tableBitMap = toString(b, MINCTables.size());
    appendToBitVectorString(this.tableBitMap);
  }

  public boolean checkIfTableExists(String tableName) throws Exception
  {
    for (Table tab : this.tables) {
      String queryTableName = this.cleanString(tab.getName().toLowerCase());
      if (tableName.equals(queryTableName)) {
        return true;
      }
    }
    return false;
  }

  public static String[] cleanColArrayString(String colArray) throws Exception
  {
    colArray = colArray.strip();
    colArray = colArray.replace("[", "");
    colArray = colArray.replace("]", "");
    colArray = colArray.replaceAll("'", "");
    return colArray.split(",\\s*");
  }


  public String setColumnsFromTable(String tableName, ArrayList<String> colNames) throws Exception
  {
    if (colNames == null) {
      return setAllorNoneColumnsFromTable(tableName, "none");
    }
    if (colNames.size() == 1 && colNames.get(0).equals("*")) {
      return setAllorNoneColumnsFromTable(tableName, "all");
    }
    HashMap<String, String> MINCColumns = this.schParse.fetchMINCColumns();
    String[] colArray = cleanColArrayString(MINCColumns.get(tableName));
    BitSet b = new BitSet(colArray.length);
    if (colNames != null) {
      for (int i = 0; i < colArray.length; i++) {
        if (colNames.contains(colArray[i].toLowerCase()) || colNames.contains(colArray[i].toUpperCase())) {
          b.set(i);
        }
      }
    }
    return toString(b, colArray.length);
  }

  public String setAllorNoneColumnsFromTable(String tableName, String allorNone) throws Exception
  {
    assert (allorNone.equals("all") || allorNone.equals("none"));
    HashMap<String, String> MINCColumns = this.schParse.fetchMINCColumns();
    String[] colArray = cleanColArrayString(MINCColumns.get(tableName));
    BitSet b = new BitSet(colArray.length);
    if (allorNone.equals("all")) {
      for (int i = 0; i < colArray.length; i++) {
        b.set(i);
      }
    }
    return toString(b, colArray.length);
  }

  public String setAllColumns() throws Exception
  {
    //int[] setIndices = this.tableIndices.stream().toArray();
    HashMap<String, Integer> MINCTables = this.schParse.fetchMINCTables();
    int i = 0;
    String b = "";
    for (String key : MINCTables.keySet()) {
      int tabIndex = MINCTables.get(key);
      assert tabIndex == i;
      if (checkIfTableExists(key)) {
        b += setAllorNoneColumnsFromTable(key, "all");
      } else {
        b += setAllorNoneColumnsFromTable(key, "none");
      }
      i++;
    }
    return b;
  }

  public String searchColDictForTableName(String colName) throws Exception
  {
    for (Table table : this.tables) {
      HashMap<String, String> MINCColumns = this.schParse.fetchMINCColumns();
      String[] colArray = cleanColArrayString(MINCColumns.get(table.getName().replace("`", "").toLowerCase()));
      for (int i = 0; i < colArray.length; i++) {
        if (colArray[i].equals(colName)) {
          return table.getName();
        }
      }
    }
    return null;
  }

  public boolean searchForTableName(String tableName) throws Exception
  {
    for (Table t : this.tables) {
      if (t.getName().toLowerCase().equals(tableName)) {
        return true;
      }
    }
    return false;
  }


  public HashMap<String, ArrayList<String>> createTableColumnDict(HashSet<Column> colSet) throws Exception
  {
    HashMap<String, ArrayList<String>> tableColumnDict = new HashMap<String, ArrayList<String>>();
    for (Column c : colSet) {
      Pair<String, String> tabColName = this.retrieveTabColName(c);
      String tableName = tabColName.first;
      String colName = tabColName.second;
      if (tableName == null) {
        continue;
      }
      if (!tableColumnDict.containsKey(tableName)) {
        tableColumnDict.put(tableName, new ArrayList<String>());
      }
      tableColumnDict.get(tableName).add(colName.toLowerCase());
    }
    return tableColumnDict;
  }
  public BitSet setAllColumnsFromTable(String tableName, BitSet bitVector) throws Exception
  {
    HashMap<String, String> MINCColumns = this.schParse.fetchMINCColumns();
    String[] colArray = cleanColArrayString(MINCColumns.get(tableName));
    for (String colName : colArray) {
      try {
        int bitPos = this.schParse.fetchMINCColBitPos().get(tableName + "." + colName);
        bitVector.set(bitPos);
      }catch (Exception e) {
        e.printStackTrace();
      }

    }
    return bitVector;
  }

  public String createBitVectorForOpColSet(HashSet<Column> colSet) throws Exception
  {
    String b = "";
    for (Column c : colSet) {
      if (c.toString().equals("*")) {
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
    HashMap<String, ArrayList<String>> tableColumnDict = createTableColumnDict(colSet);
    HashMap<String, Integer> schemaTables = this.schParse.fetchMINCTables();
    HashMap<String, Integer> schemaCols = this.schParse.fetchMINCColBitPos();
    BitSet bitVector = new BitSet(schemaCols.size());
    //	HashSet<String> tableNames = new HashSet<String>(schemaTables.keySet());
    for (String tableName : tableColumnDict.keySet()) {
      ArrayList<String> colNames = tableColumnDict.get(tableName);
      for (String colName : colNames) {
        if (colName.equals("*")) {
          bitVector = setAllColumnsFromTable(tableName, bitVector);
        } else {
          String fullColName = tableName + "." + colName;
          int colBitPos;
          try {
            if(colAliases.containsKey(tableName + ".`" + colName+"`")){
              fullColName=colAliases.get(tableName + ".`" + colName+"`").getColumnName().toLowerCase();
            }
            colBitPos = schemaCols.get(fullColName);
            bitVector.set(colBitPos);
          }
          catch (Exception e) {
            e.printStackTrace();
            continue;
          }
        }
      }
    }
    b = toString(bitVector, schemaCols.size());
    this.appendToBitVectorString(b);
    return b;
  }

  public void createBitVectorForLimit() throws Exception
  {
    if (this.limitList.size() == 1) {
      assert (this.limitList.toArray()[0].equals("limit"));
      this.appendToBitVectorString("1");
      this.limitBitMap = "1";
    } else {
      this.appendToBitVectorString("0");
      this.limitBitMap = "0";
    }
  }

  /*public HashMap<HashSet<String>, ArrayList<HashSet<String>>> convertColumnListToStringSet() throws Exception
  {
    HashMap<HashSet<String>, ArrayList<HashSet<String>>> joinPredDictQuery = new HashMap<HashSet<String>, ArrayList<HashSet<String>>>();
    for (ArrayList<Column> colPair : this.joinPredicates) {
      HashSet<String> tableNamePair = new HashSet<String>();
      HashSet<String> columnNamePair = new HashSet<String>();
      for (Column c : colPair) {
        Pair<String, String> tabColName = this.retrieveTabColName(c);
        String tableName = tabColName.first;
        String colName = tabColName.second;
        if (tableName == null) {
          continue;
        }
        tableNamePair.add(tableName);
        columnNamePair.add(colName);
      }
      if (!joinPredDictQuery.containsKey(tableNamePair)) {
        joinPredDictQuery.put(tableNamePair, new ArrayList<HashSet<String>>());
      }
      joinPredDictQuery.get(tableNamePair).add(columnNamePair);
    }
    return joinPredDictQuery;
  }*/

  public int locateHashSetInValues(HashSet<String> querySet, ArrayList<HashSet<String>> valueList) throws Exception
  {
    int valueIndex = 0;
    for (HashSet<String> key : valueList) {
      if (Util.equals(querySet, key)) {
        return valueIndex;
      }
      valueIndex++;
    }
    return -1;
  }

  public String locateHashSetInKeys(HashSet<String> querySet, HashSet<String> keySet) throws Exception
  {
    String[] queryArr = querySet.toArray(new String[querySet.size()]);
    if (queryArr.length == 1) {
      String key = queryArr[0] + "," + queryArr[0];
      if (keySet.contains(key)) {
        return key;
      }
    } else if (queryArr.length == 2) {
      String key = queryArr[0] + "," + queryArr[1];
      if (keySet.contains(key)) {
        return key;
      }
      key = queryArr[1] + "," + queryArr[0];
      if (keySet.contains(key)) {
        return key;
      }
    } else {
      System.out.println("querySet should be a pair and does not contain more than 2 tables !!");
    }
    return null;
  }

  public static boolean compareJoinPreds(Pair<String, String> set1, HashSet<String> set2)
  {
    if (set1 == null || set2 == null) {
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

  public HashMap<String, ArrayList<String>> createJoinDictQuery() throws Exception
  {
    HashMap<String, ArrayList<String>> joinPredDictQuery = new HashMap<String, ArrayList<String>>();
    for (ArrayList<Column> colPair : this.joinPredicates) {
      String leftTable = null, leftCol = null, rightTable = null, rightCol = null;
      assert colPair.size() == 2;
      for (int i = 0; i < colPair.size(); i++) {
        Column c = colPair.get(i);
        Pair<String, String> tabColName = this.retrieveTabColName(c);
        String tableName = tabColName.first;
        String colName = tabColName.second;
        if (tableName == null) {
          continue;
        }
        if (i == 0) {
          leftTable = tableName;
          leftCol = colName;
        } else if (i == 1) {
          rightTable = tableName;
          rightCol = colName;
        }
      }
      int leftTableIndex = this.schParse.fetchMINCTables().get(leftTable);
      int rightTableIndex = this.schParse.fetchMINCTables().get(rightTable);
      String joinColPair = null;
      String joinTablePair = null;
      if (leftTableIndex <= rightTableIndex) {
        joinTablePair = leftTable + "," + rightTable;
        joinColPair = leftCol + "," + rightCol;
      } else {
        joinTablePair = rightTable + "," + leftTable;
        joinColPair = rightCol + "," + leftCol;
      }
      if (!joinPredDictQuery.containsKey(joinTablePair)) {
        joinPredDictQuery.put(joinTablePair, new ArrayList<String>());
      }
      joinPredDictQuery.get(joinTablePair).add(joinColPair);
    }
    return joinPredDictQuery;
  }

  public void createBitVectorForJoin() throws Exception
  {
    HashMap<String, ArrayList<String>> joinPredDictQuery = createJoinDictQuery();
    HashMap<String, ArrayList<String>> joinPredDictSchema = this.schParse.fetchMINCJoinPreds();
    HashMap<String, Pair<Integer, Integer>> joinPredBitPosSchema = this.schParse.fetchMINCJoinPredBitPos();
    BitSet joinPredIntentVector = new BitSet(this.schParse.fetchMINCJoinPredBitCount());
    for (String tablePairQuery : joinPredDictQuery.keySet()) {
      ArrayList<String> joinPredListSchema = joinPredDictSchema.get(tablePairQuery);
      ArrayList<String> joinPredListQuery = joinPredDictQuery.get(tablePairQuery);
      Pair<Integer, Integer> startEndBitPos = joinPredBitPosSchema.get(tablePairQuery);
      for (String joinPredQuery : joinPredListQuery) {
        int bitIndex = startEndBitPos.first + joinPredListSchema.indexOf(joinPredQuery);
        joinPredIntentVector.set(bitIndex);
      }
    }
    String b = toString(joinPredIntentVector, this.schParse.fetchMINCJoinPredBitCount());
    this.appendToBitVectorString(b);
    this.joinPredicatesBitMap = b;
  }

  public Pair<String, String> replaceColAliases(String tableName, String colName) throws Exception
  {
    Pair<String, String> tabColName = new Pair<>(tableName, colName.toLowerCase());
    if (this.colAliases == null) {
      return tabColName;
    }
    if (this.colAliases.containsKey(tableName + "." + colName)) {
      String fullName = this.colAliases.get(tableName + "." + colName).toString().replace("`", "").toLowerCase();
      if (fullName.contains(".")) {
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

  public Pair<String, String> retrieveTabColName(Column c) throws Exception
  {
    Pair<String, String> tabColName;
    String fullName = c.toString().replace("`", "").toLowerCase();
    String tableName;
    String colName = fullName.toLowerCase();
    if (fullName.contains(".")) {
      String[] tokens = fullName.split("\\.");
      assert tokens.length == 2 || tokens.length == 3;
      String tableNameAlias = tokens[tokens.length - 2].toLowerCase();
      colName = tokens[tokens.length - 1].toLowerCase();
      tableName = tableNameAlias; // if there is no tableAlias tableName is being used
      boolean tableExists = searchForTableName(tableName);
      if (!tableExists && Global.tableAlias.size() > 0) {
        tableName = Global.tableAlias.get(tableNameAlias).toLowerCase();
      }
    } else {
      // there should be a single table name in the from clause, else simply search for the first table name
      if (this.tables.size() == 1) {
        tableName = this.tables.get(0).getName().replace("`", "").toLowerCase();
      } else {
        tableName = searchColDictForTableName(colName.toLowerCase()).toLowerCase();
      }
      if (tableName != null) {
        tableName = tableName.toLowerCase();
      }
    }
		/*if (!tableColumnDict.containsKey(tableName))
			tableColumnDict.put(tableName, new ArrayList<String>());
		tableColumnDict.get(tableName).add(colName.toLowerCase());	*/
    tabColName = replaceColAliases(tableName, colName.toLowerCase());
    return tabColName;
  }

  public int fetchMatchingBucketIndex(
      HashMap<String, Pair<Integer, Integer>> selPredColRangeBitPos,
      String selColFullName,
      int binIndex
  )
  {
    int lo_index = selPredColRangeBitPos.get(selColFullName).first;
    int hi_index = selPredColRangeBitPos.get(selColFullName).second;
    assert lo_index + binIndex <= hi_index;
    return lo_index + binIndex;
  }

  public static boolean isInteger(String s)
  {
    try {
      Integer.parseInt(s);
    }
    catch (NumberFormatException e) {
      return false;
    }
    catch (NullPointerException e) {
      return false;
    }
    // only got here if we didn't return false
    return true;
  }

  public int findSelColRangeBinString(
      String constVal,
      ArrayList<Pair<String, String>> rangeBins,
      HashMap<String, ArrayList<Pair<String, String>>> selPredColRangeBins,
      HashMap<String, Pair<Integer, Integer>> selPredColRangeBitPos,
      String selColFullName
  ) throws Exception
  {
    String lo_str, hi_str;
    constVal = constVal.replaceAll("^\'|\'$", "");
    for (int binIndex = 0; binIndex < rangeBins.size(); binIndex++) {
      Pair<String, String> rangeBin = rangeBins.get(binIndex);
      lo_str = rangeBin.first;
      hi_str = rangeBin.second;
      // first look for %x% substring comparison
      if (constVal.startsWith("%")) {
        String tempStr = constVal.replace("%", "");
        if (lo_str.contains(tempStr) || hi_str.contains(tempStr)) {
          int bucketIndex = fetchMatchingBucketIndex(selPredColRangeBitPos, selColFullName, binIndex);
          return bucketIndex;
        }
      }
      int lo_compare = constVal.compareTo(lo_str);
      int hi_compare = constVal.compareTo(hi_str);
      if ((lo_compare >= 0 && hi_compare <= 0) || (lo_str.equals("null") && hi_str.equals("null"))) {
        int bucketIndex = fetchMatchingBucketIndex(selPredColRangeBitPos, selColFullName, binIndex);
        return bucketIndex;
      }
    }
    return -1;
  }

  public int findSelColRangeBinInteger(
      String constVal,
      ArrayList<Pair<String, String>> rangeBins,
      HashMap<String, ArrayList<Pair<String, String>>> selPredColRangeBins,
      HashMap<String, Pair<Integer, Integer>> selPredColRangeBitPos,
      String selColFullName
  ) throws Exception
  {
    int lo, hi;
    int constValInt = Integer.parseInt(constVal);
    for (int binIndex = 0; binIndex < rangeBins.size(); binIndex++) {
      Pair<String, String> rangeBin = rangeBins.get(binIndex);
      if (rangeBin.first.equals("null") && rangeBin.second.equals("null")) {
        int bucketIndex = fetchMatchingBucketIndex(selPredColRangeBitPos, selColFullName, binIndex);
        return bucketIndex;
      }
      lo = Integer.parseInt(rangeBin.first);
      hi = Integer.parseInt(rangeBin.second);
      if (constValInt >= lo && constValInt <= hi) {
        int bucketIndex = fetchMatchingBucketIndex(selPredColRangeBitPos, selColFullName, binIndex);
        return bucketIndex;
      }
    }
    return -1;
  }

  public boolean checkForIntColType(String selColFullName) throws Exception
  {
    String tableName = selColFullName.split("\\.")[0];
    String colName = selColFullName.split("\\.")[1];
    HashMap<String, String> MINCColumns = this.schParse.fetchMINCColumns();
    HashMap<String, String> MINCColTypes = this.schParse.fetchMINCColTypes();
    String[] colTypeArray = cleanColArrayString(MINCColTypes.get(tableName));
    String[] colArray = cleanColArrayString(MINCColumns.get(tableName));
    for (int i = 0; i < colArray.length; i++) {
      if (colArray[i].equals(colName)) {
        String colType = colTypeArray[i];
        if (colType.contains("int")) {
          return true;
        }
      }
    }
    return false;
  }

  public int findSelColRangeBinToSet(String selColFullName, String constVal) throws Exception
  {
    HashMap<String, ArrayList<Pair<String, String>>> selPredColRangeBins = this.schParse.fetchMINCSelPredColRangeBins();
    HashMap<String, Pair<Integer, Integer>> selPredColRangeBitPos = this.schParse.fetchMINCSelPredColRangeBitPos();
    ArrayList<Pair<String, String>> rangeBins = selPredColRangeBins.get(selColFullName);
    boolean isInt = isInteger(constVal);
    if (isInt) {
      isInt = checkForIntColType(selColFullName);
    }
    int matchingBucketIndex = -1;
    if (isInt) {
      matchingBucketIndex = findSelColRangeBinInteger(
          constVal,
          rangeBins,
          selPredColRangeBins,
          selPredColRangeBitPos,
          selColFullName
      );
    } else {
      matchingBucketIndex = findSelColRangeBinString(
          constVal,
          rangeBins,
          selPredColRangeBins,
          selPredColRangeBitPos,
          selColFullName
      );
    }
    return matchingBucketIndex;
  }

  public void createBitVectorForSelPredColRangeBins() throws Exception
  {
    String b = "";
    int selColRangeBitMapSize = this.schParse.fetchMINCSelPredColRangeBitCount();
    BitSet bitVector = new BitSet(selColRangeBitMapSize); // col range bit count
    int bitPosToSet;
    for (Column c : this.selPredConstants.keySet()) {
      ArrayList<String> constValList = this.selPredConstants.get(c);
      String selColFullName = retrieveFullColumnName(c);
      for (String constVal : constValList) {
        bitPosToSet = findSelColRangeBinToSet(selColFullName, constVal);
        bitVector.set(bitPosToSet);
      }
    }
    b = toString(bitVector, selColRangeBitMapSize);
    this.appendToBitVectorString(b);
    this.selPredColRangeBinBitMap = b;
    return;
  }

  public String retrieveFullColumnName(Column c) throws Exception
  {
    Pair<String, String> tabColName = retrieveTabColName(c);
    String tableName = tabColName.first;
    String colName = tabColName.second;
    String colFullName = tableName + "." + colName;
    return colFullName;
  }


  public void createBitVectorForSelPredOps() throws Exception
  {
    String b = "";
    HashMap<String, Integer> selPredCols = this.schParse.fetchMINCSelPredCols();
    int selBitMapSize = selPredCols.size() * this.selPredOpList.length;
    BitSet bitVector = new BitSet(selBitMapSize);
    for (Column c : this.selPredOps.keySet()) {
      ArrayList<String> opValList = this.selPredOps.get(c);
      String selColFullName = retrieveFullColumnName(c);
      int baseIndex = selPredCols.get(selColFullName) * this.selPredOpList.length;
      for (String opVal : opValList) {
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

  public void createFragmentVectors() throws Exception
  {
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
//    this.COUNTBitMap = createBitVectorForOpColSet(this.COUNTColumns);
    //System.out.println("this.COUNTBitMap: "+this.COUNTBitMap+", length: "+this.COUNTBitMap.toCharArray().length);
    this.selectionBitMap = createBitVectorForOpColSet(this.selectionColumns);
    //System.out.println("this.selectionBitMap: "+this.selectionBitMap+", length: "+this.selectionBitMap.toCharArray().length);
    this.groupByBitMap = createBitVectorForOpColSet(this.groupByColumns);
    //System.out.println("this.groupByBitMap: "+this.groupByBitMap+", length: "+this.groupByBitMap.toCharArray().length);
//    this.orderByBitMap = createBitVectorForOpColSet(this.orderByColumns);
    //System.out.println("this.orderByBitMap: "+this.orderByBitMap+", length: "+this.orderByBitMap.toCharArray().length);
    this.havingBitMap = createBitVectorForOpColSet(this.havingColumns);
    //System.out.println("this.havingBitMap: "+this.havingBitMap+", length: "+this.havingBitMap.toCharArray().length);
    createBitVectorForLimit();
    //System.out.println("this.limitBitMap: "+this.limitBitMap+", length: "+this.limitBitMap.toCharArray().length);
//    createBitVectorForJoin();
    //System.out.println("this.joinPredBitMap: "+this.joinPredicatesBitMap+", length: "+this.joinPredicatesBitMap.toCharArray().length);
    if (this.includeSelOpConst) {
      createBitVectorForSelPredOps();
      createBitVectorForSelPredColRangeBins();
    }
    this.intentBitVector = this.intentBitVecBuilder.toString();
  }

  public boolean parseQueryAndCreateFragmentVectors() throws Exception
  {
    if (this.queryType.equals("select")
        || this.queryType.equals("update")
        || this.queryType.equals("insert")
        || this.queryType.equals("delete")) {
      try {
        this.parseQuery();
        this.createFragmentVectors();
      }
      catch (Exception e) {
        e.printStackTrace();
        return false;
      }
      return true;
    } else {
      //System.out.println("It has to be one of Select, Update, Insert or Delete !!");
      return false;
      //System.exit(0);
    }
  }

  public static void deleteIfExists(String fileName) throws Exception
  {
    File outFile = new File(fileName);
    boolean delIfExists = Files.deleteIfExists(outFile.toPath());
  }

  public static int updateSessionQueryCount(HashMap<String, Integer> sessionQueryCount, String sessID) throws Exception
  {
    int queryID;
    try {
      queryID = sessionQueryCount.get(sessID) + 1;
    }
    catch (Exception e) {
      queryID = 1;
    }
    sessionQueryCount.put(sessID, queryID);
    return queryID;
  }

  public static ArrayList<String> countMINCLinesPreProcessed(String rawSessFile, int startLineNum) throws Exception
  {
    System.out.println("Counting lines from " + rawSessFile);
    BufferedReader br = new BufferedReader(new FileReader(rawSessFile));
    ArrayList<String> lines = new ArrayList<String>();
    String line = null;
    int i = 0;
    int absCount = 0;
    while ((line = br.readLine()) != null /*  && absCount<3000000+startLineNum*/) {
      if (absCount >= startLineNum) {
        lines.add(line);
        i++;
        if (i % 100000 == 0) {
          System.out.println("Read " + i + " lines so far and absCount: " + absCount);
        }
      }
      absCount++;
    }
    System.out.println("Read " + i + " lines so far and done with absCount: " + absCount);
    br.close();
    return lines;
  }

  public static ArrayList<String> countMINCLines(String rawSessFile, int startLineNum) throws Exception
  {
    System.out.println("Counting lines from " + rawSessFile);
    BufferedReader br = new BufferedReader(new FileReader(rawSessFile));
    ArrayList<String> lines = new ArrayList<String>();
    String line = null;
    int i = 0;
    int absCount = 0;
    while ((line = br.readLine()) != null /*  && absCount<3000000+startLineNum*/) {
      if (absCount >= startLineNum && line.contains("Query")) {
        line = line.replace("\t", " ");
        line = line.replaceAll("\\s+", " ");
        line = line.trim();
        lines.add(line);
        i++;
        if (i % 1000000 == 0) {
          System.out.println("Read " + i + " lines so far and absCount: " + absCount);
        }
      }
      absCount++;
    }
    System.out.println("Read " + i + " lines so far and done with absCount: " + absCount);
    br.close();
    return lines;
  }

  public static ArrayList<String> countBusTrackerLinesPreProcessed(String rawSessFile, int startLineNum)
      throws Exception
  {
    System.out.println("Counting lines from " + rawSessFile);
    BufferedReader br = new BufferedReader(new FileReader(rawSessFile));
    ArrayList<String> lines = new ArrayList<String>();
    String line = null;
    int i = 0;
    int absCount = 0;
    while ((line = br.readLine()) != null /*  && absCount<3000000+startLineNum*/) {
      if (absCount >= startLineNum) {
        lines.add(line);
        i++;
        if (i % 100000 == 0) {
          System.out.println("Read " + i + " lines so far and absCount: " + absCount);
        }
      }
      absCount++;
    }
    System.out.println("Read " + i + " lines so far and done with absCount: " + absCount);
    br.close();
    return lines;
  }

  public static String cleanUpBusTrackerLine(String line) throws Exception
  {
    String substr = "((SELECT extract(epoch FROM ";
    String substr2 = ")*1000))";
    //((SELECT extract(epoch FROM c.start_date)*1000)) <= 1480475749583
    if (line.contains(substr) && line.contains(substr2)) {
      line = line.replace(substr, "");
      line = line.replace(substr2, "");
    }
    line = line.replace("$", "");
    line = line.replace("|/", "");
    line = line.replace("^2", "");
    line = line.replace("CURRENT_DATE", "12-30-2019");
    String inputQueryPhrase =
        "select nm.trip_id,nm.id AS message_id, nm.message, nm.timestamp, nm.category,a.firstname AS first_name, a.lastname AS last_name "
        + "from dv_notes_message nm, dv_account a, (SELECT dv_trip_id, MAX(dv.timestamp) AS maximum FROM dv_notes_message dv WHERE "
        + "dv_agency_id IN (select a.agency_id from m_agency a, m_agency b where a.agency_id_id=b.agency_id_id and b.agency_id=1) "
        + "AND dv_trip_id IN";
    String replacedQueryPhrase =
        "select nm.trip_id,nm.id AS message_id, nm.message, nm.timestamp, nm.category,a.firstname AS first_name, a.lastname AS last_name "
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

  public static ArrayList<String> countBusTrackerLines(String rawSessFile, int startLineNum) throws Exception
  {
    System.out.println("Counting lines from " + rawSessFile);
    BufferedReader br = new BufferedReader(new FileReader(rawSessFile));
    ArrayList<String> lines = new ArrayList<String>();
    String line = null;
    int i = 0;
    int absCount = 0;
    while ((line = br.readLine()) != null /*  && absCount<3000000+startLineNum*/) {
      if (absCount >= startLineNum) {
        line = line.trim();
        line = cleanUpBusTrackerLine(line);
        lines.add(line);
        i++;
        if (i % 1000000 == 0) {
          System.out.println("Read " + i + " lines so far and absCount: " + absCount);
        }
      }
      absCount++;
    }
    System.out.println("Read " + i + " lines so far and done with absCount: " + absCount);
    br.close();
    return lines;
  }

  public static ArrayList<String> countLinesPreProcessed(String dataset, String rawSessFile, int startLineNum)
      throws Exception
  {
    if (dataset.equals("MINC")) {
      return countMINCLinesPreProcessed(rawSessFile, startLineNum);
    } else if (dataset.equals("BusTracker")) {
      return countBusTrackerLinesPreProcessed(rawSessFile, startLineNum);
    } else {
      return null;
    }
  }

  public static ArrayList<String> countLines(String dataset, String rawSessFile, int startLineNum) throws Exception
  {
    if (dataset.equals("MINC")) {
      return countMINCLines(rawSessFile, startLineNum);
    } else if (dataset.equals("BusTracker")) {
      return countBusTrackerLines(rawSessFile, startLineNum);
    } else {
      return null;
    }
  }

  public static String fetchSessID(String dataset, String line) throws Exception
  {
    String curSessID = null;
    if (dataset.equals("MINC")) {
      curSessID = line.split(" ")[0];
    } else if (dataset.equals("BusTracker")) {
      String regex = ",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)"; // to split on comma outside double quotes
      String[] tokens = line.split(regex);
      // format is "startTime","sessID","endTime","execute <unnamed>/statement: Query","parameters: $1 = ..., $2 = ..."
      curSessID = tokens[1];
      curSessID = curSessID.replaceAll("^\"|\"$", ""); // remove starting and trailing double quotes
    }
    return curSessID;
  }

  public static ArrayList<Pair<Integer, Integer>> readLinesPerThread(
      String dataset,
      int lowerIndexPerThread,
      int curThreadIndex,
      int numThreads,
      int numLinesPerThread,
      ArrayList<String> sessQueries,
      ArrayList<Pair<Integer, Integer>> inputSplits,
      String pruneKeepModifyRepeatedQueries
  ) throws Exception
  {
    System.out.println("Splitting lines for thread "
                       + curThreadIndex
                       + ", numLinesPerThread: "
                       + numLinesPerThread
                       + " with lower Index: "
                       + lowerIndexPerThread);
    int i = 0;
    int runningIndex = Math.min(lowerIndexPerThread + numLinesPerThread - 1, sessQueries.size() - 1);
    String line = sessQueries.get(runningIndex);
    String prevSessID = line.split(" ")[0];
    String curSessID = null;
    runningIndex++;
    while (runningIndex < sessQueries.size()) {
      if (pruneKeepModifyRepeatedQueries.equals("PREPROCESS")) {
        curSessID = sessQueries.get(runningIndex).trim().split("; ")[0].split(", ")[0].split(" ")[1];
      } else {
        curSessID = fetchSessID(dataset, sessQueries.get(runningIndex));
      }
      if (curThreadIndex != numThreads - 1 && !curSessID.equals(prevSessID)) {
        break;
      }
      runningIndex++;
    }
    int upperIndexPerThread = runningIndex - 1;
    Pair<Integer, Integer> lowerUpperIndexBounds = new Pair<>(lowerIndexPerThread, upperIndexPerThread);
    inputSplits.add(lowerUpperIndexBounds);
    int numLinesAssigned = upperIndexPerThread - lowerIndexPerThread + 1;
    System.out.println("Assigned "
                       + numLinesAssigned
                       + " lines to thread "
                       + curThreadIndex
                       + ", lowerIndex: "
                       + lowerIndexPerThread
                       + ", upperIndex: "
                       + upperIndexPerThread);
    return inputSplits;
  }


  public static ArrayList<Pair<Integer, Integer>> splitInputAcrossThreads(
      String dataset,
      ArrayList<String> sessQueries,
      int numThreads,
      String pruneKeepModifyRepeatedQueries
  ) throws Exception
  {
    assert numThreads > 0;
    int numLinesPerThread = sessQueries.size() / numThreads;
    assert numLinesPerThread > 1;
    ArrayList<Pair<Integer, Integer>> inputSplits = new ArrayList<Pair<Integer, Integer>>();
    int lowerIndexPerThread = 0;
    for (int i = 0; i < numThreads; i++) {
      inputSplits = readLinesPerThread(
          dataset,
          lowerIndexPerThread,
          i,
          numThreads,
          numLinesPerThread,
          sessQueries,
          inputSplits,
          pruneKeepModifyRepeatedQueries
      );
      lowerIndexPerThread = inputSplits.get(i).second
                            + 1; // upper index of data for current thread +1 will be the lower
      // index for the next thread
    }
    return inputSplits;
  }

  public static ArrayList<String> defineOutputSplits(String tempLogDir, String rawSessFile, int numThreads)
      throws Exception
  {
    ArrayList<String> outputSplitFiles = new ArrayList<String>();
    for (int i = 0; i < numThreads; i++) {
      String fileName = rawSessFile.split("/")[rawSessFile.split("/").length - 1];
      String outFilePerThread = tempLogDir + "/" + fileName + "_SPLIT_OUT_" + i;
      System.out.println("outFilePerThread: " + outFilePerThread);
      outputSplitFiles.add(outFilePerThread);
    }
    return outputSplitFiles;
  }

  public static void concatenateOutputFiles(ArrayList<String> outFiles, String intentVectorFile) throws Exception
  {
    BufferedWriter bw = new BufferedWriter(new FileWriter(intentVectorFile, true));
    for (String outFile : outFiles) {
      BufferedReader br = new BufferedReader(new FileReader(outFile));
      String line = null;
      int i = 0;
      String concLine = "";
      while ((line = br.readLine()) != null) {
        concLine += line + "\n";
        if (i % 1000 == 0) {
          bw.append(concLine);
          concLine = "";
        }
        i++;
      }
      if (!concLine.equals("")) {
        bw.append(concLine);
      }
      deleteIfExists(outFile);
    }
    bw.flush();
    bw.close();
  }


  public static void readFromRawSessionsFile(
      String dataset,
      String tempLogDir,
      String rawSessFile,
      String intentVectorFile,
      String line,
      SchemaParser schParse,
      int numThreads,
      int startLineNum,
      String pruneKeepModifyRepeatedQueries,
      boolean includeSelOpConst
  ) throws Exception
  {
    //	deleteIfExists(intentVectorFile);
    //	System.out.println("Deleted previous intent file");
    ArrayList<String> sessQueries;
    if (pruneKeepModifyRepeatedQueries.equals("PREPROCESS")) {
      sessQueries = countLinesPreProcessed(dataset, rawSessFile, startLineNum);
    } else {
      sessQueries = countLines(dataset, rawSessFile, startLineNum);
    }
    System.out.println("Read sessQueries into main memory");
    ArrayList<Pair<Integer, Integer>> inputSplits = splitInputAcrossThreads(
        dataset,
        sessQueries,
        numThreads,
        pruneKeepModifyRepeatedQueries
    );
    System.out.println("Split Input Across Threads");
    ArrayList<String> outputSplitFiles = defineOutputSplits(tempLogDir, rawSessFile, numThreads);
    System.out.println("Defined Output File Splits Across Threads");
    ArrayList<IntentCreatorMultiThread> intentMTs = new ArrayList<IntentCreatorMultiThread>();
    for (int i = 0; i < numThreads; i++) {
      IntentCreatorMultiThread intentMT = new IntentCreatorMultiThread(
          dataset,
          i,
          sessQueries,
          inputSplits.get(i),
          outputSplitFiles.get(i),
          schParse,
          pruneKeepModifyRepeatedQueries,
          includeSelOpConst
      );
      intentMT.start();
    }
	/*	for(IntentCreatorMultiThread intentMT : intentMTs) {
			intentMT.join();
		}
		*/
    //	concatenateOutputFiles(outputSplitFiles, intentVectorFile);
  }

  public static void main(String[] args)
  {
//		String homeDir = System.getProperty("user.home");
    System.out.println(APMFragmentIntent.getMachineName());
//		if(MINCFragmentIntent.getMachineName().contains("4119510") || MINCFragmentIntent.getMachineName().contains("4119509")
//				|| MINCFragmentIntent.getMachineName().contains("4119508") || MINCFragmentIntent.getMachineName().contains("4119507")) {
//			homeDir = "/hdd2/vamsiCodeData"; // comment it when you are not running on EN4119510L.cidse.dhcp.adu.edu
//		}
//		String configFile = homeDir+"/var/data/MINC/InputOutput/MincJavaConfig.txt";
    String configFile = "C:\\buaa\\data\\APM\\Input/ApmJavaConfig.txt";
    SchemaParser schParse = new SchemaParser();
    schParse.fetchSchema(configFile);
    HashMap<String, String> configDict = schParse.getConfigDict();
    boolean includeSelOpConst = Boolean.parseBoolean(configDict.get("MINC_SEL_OP_CONST"));
    String dataset = configDict.get("MINC_DATASET");
    try {

      //uncomment the following when full run needs to happen on EC2 or on EN4119510L
      //	readFromRawSessionsFile(dataset, tempLogDir, rawSessFile, intentVectorFile, line, schParse, numThreads, startLineNum, pruneKeepModifyRepeatedQueries, includeSelOpConst);

      String tsvFilePath = "C:/buaa/data/APM/Input/ApmQuerys.tsv"; // TSV 文件路径
      try (Reader reader = Files.newBufferedReader(Paths.get(tsvFilePath))) {
        CSVParser csvParser = new CSVParser(reader, CSVFormat.TDF.withFirstRecordAsHeader());
        Iterable<CSVRecord> records = csvParser.getRecords();

        // 处理标题行
        List<String> headers = csvParser.getHeaderNames();
        System.out.println("Headers: " + headers);
        int count=0;
        for (CSVRecord record : records) {
          String query = record.get("query");
          if(!ExcelReader.filterSql(query)){
             continue;
          }
          count++;
          String queryIntent = getQueryIntent(query, schParse, includeSelOpConst);
          System.out.println(count+","+queryIntent.length() + "," + queryIntent);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
  }

  public static String getQueryIntent(String query, SchemaParser schParse, boolean includeSelOpConst)
      throws Exception
  {
    query=StringCleaner.cleanString(query);
    query=StringCleaner.correctQuery(query);
    System.out.println("clean query:"+query);
//    SqlParser.Config config = SqlParser.configBuilder()
//                                       .setLex(Lex.JAVA)
//                                       .setParserFactory(SqlParserImpl.FACTORY)
//                                       .setConformance(SqlConformanceEnum.MYSQL_5)
//                                       .build();
//    SqlDialect clickHouseDialect = new ClickHouseDialect();
    // 创建 Calcite SqlParser.Config 配置对象
    SqlParser.Config config = SqlParser.configBuilder()
            .setLex(Lex.JAVA) // ClickHouse方言可能需要LEX设置为Lex.JAVA
            .setParserFactory(SqlParserImpl.FACTORY)
            .setConformance(SqlConformanceEnum.LENIENT)
            .setQuoting(Quoting.BACK_TICK)
//            .setConformance(ClickHouseSqlDialect.DEFAULT.getConformance()) // 使用 ClickHouse 方言的一致性规则
            .build();
    // 创建解析器
    SqlParser parser = SqlParser.create(query, config);
    try {
      // 解析sql
      SqlNode sqlNode = parser.parseQuery(query);
      SqlDialect.Context MY_CONTEXT = SqlDialect.EMPTY_CONTEXT
              .withDatabaseProduct(SqlDialect.DatabaseProduct.CLICKHOUSE)
//              .withIdentifierQuoteString("`")
              .withNullCollation(NullCollation.LOW);
      // 还原某个方言的SQL
      SqlString sqlString = sqlNode.toSqlString(new ClickHouseSqlDialect(MY_CONTEXT));
      query = sqlString.getSql();
      System.out.println("com.clickhouse query:"+query);
    }catch (Exception e){
      e.printStackTrace();
    }

    APMFragmentIntent fragmentObj = new APMFragmentIntent(query, schParse, includeSelOpConst, "APM");
    boolean validQuery = fragmentObj.parseQueryAndCreateFragmentVectors();
    if (validQuery) {
      fragmentObj.printIntentVector();
      fragmentObj.writeIntentVectorToTempFile(query);
    }
    String queryIntent = fragmentObj.getIntentBitVector();
    int maxLen = queryIntent.length();
    System.out.println("MaxLen: " + maxLen);
    return queryIntent;
  }

}

