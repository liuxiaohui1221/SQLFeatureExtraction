import com.clickhouse.ClickhouseSQLParser;
import com.clickhouse.SchemaParser;
import lombok.extern.slf4j.Slf4j;
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
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.impl.SqlParserImpl;
import org.apache.calcite.sql.util.SqlString;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
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

@Slf4j
public class APMFragmentIntent
{
  String originalSQL;
  String intentBitVector;
  StringBuilder intentBitVecBuilder;
  List<String> tables = new ArrayList<>();
  HashMap<String, String> colAliases = null;
  HashSet<String> groupByColumns = new HashSet<>();
  HashSet<String> whereColumns = new HashSet<>();
  HashSet<String> havingColumns = new HashSet<>();
  HashSet<String> orderByColumns = new HashSet<>();
  HashSet<String> projectionColumns = new HashSet<>();
  HashSet<ArrayList<String>> joinPredicates = new HashSet<ArrayList<String>>();
  HashSet<String> limitList = new HashSet<String>();
  HashSet<String> MINColumns = new HashSet<>();
  HashSet<String> MAXColumns = new HashSet<>();
  HashSet<String> AVGColumns = new HashSet<>();
  HashSet<String> SUMColumns = new HashSet<>();
  HashSet<String> COUNTColumns = new HashSet<>();
  HashMap<String, ArrayList<String>> selPredOps;
  HashMap<String, ArrayList<String>> selPredConstants;
  String queryTypeBitMap;
  String tableBitMap;
  String groupByBitMap;
  String whereBitMap;
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
  ClickhouseSQLParser sqlParser;

  public APMFragmentIntent(String originalSQL, SchemaParser schParse, boolean includeSelOpConst, String dataset)
      throws Exception
  {
    this.originalSQL = originalSQL.toLowerCase();
    this.schParse = schParse;
    this.includeSelOpConst = includeSelOpConst;
    this.dataset = dataset;
    Global.tableAlias = new HashMap<String, String>();
    sqlParser = new ClickhouseSQLParser(schParse);
    this.intentBitVector = null;
    this.intentBitVecBuilder = new StringBuilder();
    this.queryType = "select";
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
    bw.append("SelectionBitMap:" + this.whereBitMap + "\n");
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
    System.out.println("SelectionBitMap:" + this.whereBitMap);
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

  public void populateOperatorObjects() throws Exception
  {
    this.tables = sqlParser.getFromTables();
//    this.colAliases = sqlParser.getColAliases();
    this.groupByColumns = sqlParser.getGroupByColumns();
    this.whereColumns = sqlParser.getWhereColumns();
//    this.havingColumns = sqlParser.getHavingColumns();
//    this.orderByColumns = sqlParser.getOrderByColumns();
    this.projectionColumns = sqlParser.getSelectionColumns();
//    this.joinPredicates = sqlParser.getJoinPredicates();
//    this.limitList = sqlParser.getLimitList();
    this.MINColumns = sqlParser.getMinColumns();
    this.MAXColumns = sqlParser.getMaxColumns();
//    this.AVGColumns = sqlParser.getAVGColumns();
    this.SUMColumns = sqlParser.getSumColumns();
//    this.COUNTColumns = sqlParser.getCOUNTColumns();
//    if (this.includeSelOpConst) {
//      this.selPredOps = sqlParser.getSelPredOps();
//      this.selPredConstants = sqlParser.getSelPredConstants();
//    }
  }

  public void parseQuery() throws Exception
  {
    sqlParser.createQueryVector(this.originalSQL);
    populateOperatorObjects();
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
    for (String tableName : this.tables) {
      try {
        tableName = this.cleanString(tableName.toLowerCase());
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
    for (String tab : this.tables) {
      String queryTableName = this.cleanString(tab.toLowerCase());
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
    for (String table : this.tables) {
      HashMap<String, String> MINCColumns = this.schParse.fetchMINCColumns();
      String[] colArray = cleanColArrayString(MINCColumns.get(table.replace("`", "").toLowerCase()));
      for (int i = 0; i < colArray.length; i++) {
        if (colArray[i].equals(colName)) {
          return table.toLowerCase();
        }
      }
    }
    return null;
  }

  public boolean searchForTableName(String tableName) throws Exception
  {
    for (String t : this.tables) {
      if (t.toLowerCase().equals(tableName)) {
        return true;
      }
    }
    return false;
  }


  public HashMap<String, ArrayList<String>> createTableColumnDict(HashSet<String> colSet) throws Exception
  {
    HashMap<String, ArrayList<String>> tableColumnDict = new HashMap<String, ArrayList<String>>();
    for (String c : colSet) {
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

  public String createBitVectorForOpColSet(HashSet<String> colSet) throws Exception
  {
    String b = "";
    for (String c : colSet) {
      if (c.equals("*")) {
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
            /*if(colAliases.containsKey(tableName + ".`" + colName+"`")){
              fullColName=colAliases.get(tableName + ".`" + colName+"`").toLowerCase();
            }*/
            colBitPos = schemaCols.get(fullColName);
            bitVector.set(colBitPos);
          }
          catch (Exception e) {
            e.printStackTrace();
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

  public Pair<String, String> retrieveTabColName(String c) throws Exception
  {
    Pair<String, String> tabColName;
    String fullName = c.replace("`", "").toLowerCase();
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
        tableName = this.tables.get(0).replace("`", "").toLowerCase();
      } else {
        tableName = searchColDictForTableName(colName.toLowerCase());
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
    for (String c : this.selPredConstants.keySet()) {
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

  public String retrieveFullColumnName(String c) throws Exception
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
    for (String c : this.selPredOps.keySet()) {
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
    createBitVectorForQueryTypes();//sql类型4位
    createBitVectorForTables();//sql中要查询的表，位数为表数量
    this.projectionBitMap = createBitVectorForOpColSet(this.projectionColumns);//sql中要查询的列，位数为列数量
    this.AVGBitMap = createBitVectorForOpColSet(this.AVGColumns);//sql中AVG列，位数为列数量
    this.MINBitMap = createBitVectorForOpColSet(this.MINColumns);//sql中MIN列，位数为列数量
    this.MAXBitMap = createBitVectorForOpColSet(this.MAXColumns);//sql中MAX列，位数为列数量
    this.SUMBitMap = createBitVectorForOpColSet(this.SUMColumns);//sql中SUM列，位数为列数量
//    this.COUNTBitMap = createBitVectorForOpColSet(this.COUNTColumns);
    this.whereBitMap = createBitVectorForOpColSet(this.whereColumns);//sql中where条件，位数为列数量
    this.groupByBitMap = createBitVectorForOpColSet(this.groupByColumns);//sql中group by，位数为列数量
    this.orderByBitMap = createBitVectorForOpColSet(this.orderByColumns);
    this.havingBitMap = createBitVectorForOpColSet(this.havingColumns);//sql中having，位数为列数量
    createBitVectorForLimit();//sql中limit，1位
//    createBitVectorForJoin();
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
    String configFile = "input/ApmJavaConfig.txt";
    SchemaParser schParse = new SchemaParser();
    schParse.fetchSchema(configFile);
    HashMap<String, String> configDict = schParse.getConfigDict();
    boolean includeSelOpConst = Boolean.parseBoolean(configDict.get("MINC_SEL_OP_CONST"));
    List<String> sqlList = new ArrayList<>();
    try {
      String tsvFilePath = "input/ApmQuerys.tsv"; // TSV 文件路径
      try (Reader reader = Files.newBufferedReader(Paths.get(tsvFilePath))) {
        CSVParser csvParser = new CSVParser(reader, CSVFormat.TDF.withFirstRecordAsHeader());
        Iterable<CSVRecord> records = csvParser.getRecords();
        // 处理标题行
        List<String> headers = csvParser.getHeaderNames();
        log.info("Headers: " + headers);
        int count=0;
        for (CSVRecord record : records) {
          String query = record.get("query");
          if(!ExcelReader.filterSql(query)){
             continue;
          }
          count++;
          String queryIntent = getQueryIntent(query, schParse, includeSelOpConst);
          log.info(count+",queryIntent.length="+queryIntent.length() + "," + queryIntent);
          sqlList.add("Session 0, Query " + count + "; OrigQuery:" + query + ";" + queryIntent);
        }
      } catch (Exception e) {
        e.printStackTrace();
      }
    }
    catch (Exception e) {
      e.printStackTrace();
    }
    Util.writeSQLListToFile(sqlList, "output/ApmQueryIntent.txt");
  }

  public static String getQueryIntent(String query, SchemaParser schParse, boolean includeSelOpConst)
      throws Exception
  {
    query=StringCleaner.cleanString(query);
    query=StringCleaner.correctQuery(query);
    System.out.println("clean query:"+query);

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

