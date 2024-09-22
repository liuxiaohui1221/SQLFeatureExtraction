package reader;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellType;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileWriter;
import java.io.IOException;
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

  public static void main(String[] args)
  {
    String tsvFilePath = "/home/xhh/data/APM/clickhouse_sql_1.xlsx"; // TSV 文件路径
    try (FileInputStream file = new FileInputStream(tsvFilePath);
         Workbook workbook = new XSSFWorkbook(file)) {

      Sheet sheet = workbook.getSheetAt(0);
      Map<String, Set<String>> tablesMap = new HashMap<>();
      Map<String, List<String>> tableColsMap = new HashMap<>();
      Map<String, List<String>> tableColTypesMap = new HashMap<>();
      for (Row row : sheet) {
        // 解析库名和表名，ddl语句
        parseDBTables(0, row, tablesMap);

        parseTableColAndTypes(2, row, tableColsMap, tableColTypesMap);
      }
      // 将结果写入文件
      writeDBTablesToFile(tablesMap);
      DDLParser.writeMapsToFile(tableColsMap, tableColTypesMap);

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

    }
    catch (IOException e) {
      e.printStackTrace();
    }
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

  private static void parseTableColAndTypes(
      int index, Row row,
      Map<String, List<String>> tableColsMap,
      Map<String, List<String>> tableColTypesMap
  )
  {
    Cell cell = row.getCell(index);
    if (cell.getCellType() == CellType.STRING) {
      String cellValue = cell.getStringCellValue();
      if ("query".equals(cellValue)) {
        return;
      }
      DDLParser.parseDDL(cellValue, tableColsMap, tableColTypesMap);
    }

  }

  private static void parseDBTables(int index, Row row, Map<String, Set<String>> tablesMap)
  {
    Cell cell = row.getCell(index);

    if (cell.getCellType() == CellType.STRING) {
      String cellValue = cell.getStringCellValue();
      if ("tables".equals(cellValue)) {
        return;
      }
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
          tablesMap.get(dbName).add(tableName);
        }
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
    File outputDir = new File(projectPath, "output");

    // 确保 output 目录存在
    if (!outputDir.exists() && !outputDir.mkdirs()) {
      throw new IllegalStateException("Cannot create output directory.");
    }
    return outputDir;
  }
}
