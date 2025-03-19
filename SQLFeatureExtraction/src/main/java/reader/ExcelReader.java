package reader;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharsetDecoder;
import java.nio.charset.CoderResult;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

public class ExcelReader
{
  private static final AtomicInteger tableIndex = new AtomicInteger(0);
  private static final String tsvFilePath = "C:/buaa/data/APM/clickhouse_sql_1.tsv"; // TSV 文件路径
  private static final String outputFileName = "0318_ApmQuerys.tsv";
  private static final String outputDirector = "output/0318";

  public static void main(String[] args) throws FileNotFoundException {
    Map<String, Set<String>> tablesMap = new HashMap<>();
    Map<String, List<String>> tableColsMap = new HashMap<>();
    Map<String, List<String>> tableColTypesMap = new HashMap<>();
    List<String> cleanQuerys = new ArrayList<>();


    BufferedReader reader = new BufferedReader(new InputStreamReader(
            new FileInputStream(tsvFilePath), StandardCharsets.UTF_8));
    try /*(Reader reader = Files.newBufferedReader(Paths.get(tsvFilePath)))*/ {
      CSVParser csvParser = new CSVParser(reader, CSVFormat.TDF.withFirstRecordAsHeader());
      Iterable<CSVRecord> records = csvParser.getRecords();

      // 处理标题行
      List<String> headers = csvParser.getHeaderNames();
      System.out.println("Headers: " + headers);
      List<String> cleanheaders = new ArrayList<>();
      cleanheaders.add("tables");
      cleanheaders.add("event_time");
      cleanheaders.add("query");
      cleanheaders.add("query_duration_ms");
      cleanheaders.add("read_rows");
      cleanheaders.add("result_rows");
      cleanheaders.add("projections");
      for (CSVRecord record : records) {
        // 通过列名访问值
        String dbtable = record.get("tables");
        String event_time =  record.get("event_time");
        String query = record.get("query");
        String query_duration_ms = record.get("query_duration_ms");
        String read_rows = record.get("read_rows");
        String result_rows = record.get("result_rows");
        String projections = record.get("projections");
        query = StringCleaner.cleanString(query);

        // 解析库名和表名，ddl语句
        parseDBTables(dbtable, tablesMap);

        parseTableColAndTypes(query, tableColsMap, tableColTypesMap);
        boolean isQuery=filterSql(query);

        if(isQuery){
          String row=dbtable+"\t"+event_time+"\t"+query+"\t"+query_duration_ms+"\t"+read_rows+"\t"+result_rows+"\t"+projections;
          cleanQuerys.add(row);
        }
      }

      // 将结果写入文件
      writeDBTablesToFile(tablesMap);
      DDLParser.writeMapsToFile(tableColsMap, tableColTypesMap);
      writeCleanQuerysToFile(cleanheaders,cleanQuerys);

      // 打印库名数量和表名数量
      System.out.println("db count:" + tablesMap.size());
      tablesMap.forEach((dbName, tableNames) -> {
        System.out.println(dbName + ": " + tableNames.size());
      });
      // 打印两个map表名及其字段和类型
      System.out.println("table count:" + tableColsMap.size());
      List<String> tableAndCols = new ArrayList<>();
      tableColsMap.forEach((tableName, cols) -> {
        System.out.println(tableName + ": " + cols.size());
        cols.stream().forEach(col -> {
          tableAndCols.add(tableName + "." + col);
        });
      });
      Collections.sort(tableAndCols);
      writeTableColsBitToFile(tableAndCols);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  private static void writeCleanQuerysToFile(List<String> headers,List<String> cleanQuerys) {

    File outputFile = new File(getOutputDir(), outputFileName);
    try (FileWriter writer = new FileWriter(outputFile)) {
      writer.write(String.join("\t",headers));
      writer.write("\n");
      for (int i = 0; i < cleanQuerys.size(); i++) {
        writer.write(cleanQuerys.get(i) + "\n");
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static boolean filterSql(String query) {
    if(query.toLowerCase().contains("create table")||query.contains("drop table")
            ||query.toLowerCase().contains("create database")
            ||query.toLowerCase().contains("drop database")
    || query.toLowerCase().contains("create materialized view")
    || !query.toLowerCase().contains("select")) {
      return false;
    }
    return true;
  }

  private static void writeTableColsBitToFile(List<String> tableAndCols)
  {
    String fileName = "ApmColBitPos";
    File outputFile = new File(getOutputDir(), fileName + ".txt");
    try (FileWriter writer = new FileWriter(outputFile)) {
      for (int i = 0; i < tableAndCols.size(); i++) {
        writer.write(tableAndCols.get(i) + ":" + i + "\n");
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  private static void parseTableColAndTypes(String cellValue,
      Map<String, List<String>> tableColsMap,
      Map<String, List<String>> tableColTypesMap
  )
  {
//      cellValue = StringCleaner.cleanString(cellValue);
      DDLParser.parseDDL(cellValue, tableColsMap, tableColTypesMap);
  }

  private static void parseDBTables(String cellValue, Map<String, Set<String>> tablesMap)
  {
//    cellValue = StringCleaner.cleanString(cellValue);
      // 假设单元格值是一个字符串表示的数组
      String[] dbtablevalues = cellValue.substring(1, cellValue.length() - 1).split("\\],\\[");
      for (String values : dbtablevalues) {
        values = values.replaceAll("[\\['\"]", ""); // 移除字符串中的 ['"] 和 []
//        System.out.println("line values:" + values);
        //继续按英文逗号划分得到db.table数组
        String[] dbAndTables = values.split(",");
        for (String dbTable : dbAndTables) {
          String[] parts = dbTable.split("\\.");
          if (parts.length != 2) {
            System.out.println("filter invalid db.table info:" + dbTable);
            continue;
          }
          String dbName = parts[0];
          String tableName = parts[parts.length - 1];
          tablesMap.putIfAbsent(dbName, new LinkedHashSet<>());
          if (tableName.contains("_cluster")) {
            tableName = tableName.substring(0, tableName.indexOf("_cluster"));
          }
          if (tableName.contains("_view")) {
            tableName = tableName.substring(0, tableName.indexOf("_view"));
          }
          tablesMap.get(dbName).add(tableName);
        }
      }



  }

  private static void writeDBTablesToFile(Map<String, Set<String>> tablesMap)
  {
    // 将结果写入文件
    // 写入文件
    tablesMap.forEach((dbName, tableNames) -> {
      File outputFile = new File(getOutputDir(), dbName + ".txt");
      tableIndex.set(0);
      try (FileWriter writer = new FileWriter(outputFile)) {
        tableNames.forEach(tableName -> {
          try {
            writer.write(tableName + ":" + tableIndex.getAndIncrement() + "\n");
          }
          catch (IOException e) {
            e.printStackTrace();
          }
        });
      }
      catch (IOException e) {
        e.printStackTrace();
      }
    });
  }

  public static File getOutputDir()
  {
    String projectPath = System.getProperty("user.dir"); // 获取当前工作目录，通常是项目根目录
    File outputDir = new File(projectPath, outputDirector);

    // 确保 output 目录存在
    if (!outputDir.exists() && !outputDir.mkdirs()) {
      throw new IllegalStateException("Cannot create output directory.");
    }
    return outputDir;
  }
}
