package reader;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class DDLParser
{

  public static void main(String[] args)
  {
    String ddl = "/* ddl_entry=query-0000010389 */ CREATE TABLE IF NOT EXISTS" +
            " pmone_c9d7321d2e.dwm_request_cluster UUID '0e3ea76b-b206-40e1-9c99-fe5691d868ab' " +
            "\n(\n`ts` DateTime64(3), `type` LowCardinality(String), `group` String, `appid` LowCardinality(String), `appsysid` LowCardinality(String), `agent` LowCardinality(String), `service_type` Nullable(UInt16), \n`path` String, `method` String, `root_appid` LowCardinality(String), `pappid` LowCardinality(Nullable(String)), `pappsysid` LowCardinality(Nullable(String)), `papp_type` Nullable(UInt16), `pagent` LowCardinality(Nullable(String)), `pagent_ip` LowCardinality(Nullable(String)), `uevent_model` LowCardinality(Nullable(String)), `uevent_id` Nullable(String), `user_id` Nullable(String), `session_id` Nullable(String), `host` Nullable(String), `ip_addr` Nullable(String), `province` LowCardinality(Nullable(String)), `city` LowCardinality(Nullable(String)), `page_id` Nullable(String), `page_group` Nullable(String), `status` UInt16, `err_4xx` Nullable(UInt64), `err_5xx` Nullable(UInt64), `status_code` Int16, `tag` LowCardinality(Nullable(String)), `code` String, `is_model` Bool, `exception` UInt64, `biz` UInt64, `fail` UInt64, `httperr` UInt64, `neterr` UInt64, `err` UInt64, `tolerated` UInt64, `frustrated` UInt64, `dur` UInt64) ENGINE = Distributed(ch_cluster_all, pmone_c9d7321d2e, dwm_request)";
    Map<String, List<String>> tableColsMap = new HashMap<>();
    Map<String, List<String>> tableColTypesMap = new HashMap<>();
    parseDDL(ddl, tableColsMap, tableColTypesMap);
  }

  public static void parseDDL(
      String ddl, Map<String, List<String>> tableColsMap,
      Map<String, List<String>> tableColTypesMap
  )
  {
    ddl = ddl.toLowerCase().replaceAll("\n", "");
    String tableNamePattern = "create table if not exists ([^\\.]+)\\.(\\w+)";
    String columnsPattern = "create table.*\\(.*\\)";

    Pattern tablePattern = Pattern.compile(tableNamePattern);
    Pattern columnsPatternCompile = Pattern.compile(columnsPattern);

    Matcher tableMatcher = tablePattern.matcher(ddl);
    Matcher columnsMatcher = columnsPatternCompile.matcher(ddl);

    if (tableMatcher.find()) {
      String dbName = tableMatcher.group(1);
      if (dbName == null) {
        return;
      }
      String tableName = tableMatcher.group(2);
      if (tableName.contains("_cluster")) {
        tableName = tableName.substring(0, tableName.indexOf("_cluster"));
      }
      if(tableName.endsWith("_view")){
        tableName = tableName.substring(0,tableName.length()-5);
      }
      if (columnsMatcher.find()) {
        String columnsStr = columnsMatcher.group(0);
//        System.out.println("columnsStr:" + columnsStr);
        int end = columnsStr.lastIndexOf(") engine") == -1
                  ? columnsStr.lastIndexOf(")")
                  : columnsStr.lastIndexOf(") engine");
        columnsStr = columnsStr.substring(columnsStr.indexOf("(") + 1, end);
        //columns按逗号分隔
        columnsStr = columnsStr.replaceAll("`", "");
        columnsStr = columnsStr.replace("\n", "");
        System.out.println("clean columnsStr:" + columnsStr);
        String[] colTypes = columnsStr.split(",");
        List<String> columnNames = new ArrayList<>();
        List<String> columnTypes = new ArrayList<>();

        for (String entry : colTypes) {
          String[] colTypeArr = entry.strip().split(" ");
          if (colTypeArr.length != 2) {
            System.out.println("filter invalid entry:" + Arrays.toString(colTypeArr) + ",length:" + colTypeArr.length);
            continue;
          }
//          System.out.println("column&type entry:" + entry);
          String columnName = colTypeArr[0];
          String columnType = colTypeArr[1].replaceAll("(\\(.*?\\))?", "");

          columnNames.add(columnName);
          columnTypes.add(columnType);
        }

        if (columnNames.size() > 0 && !tableColsMap.containsKey(tableName)) {
          tableColsMap.putIfAbsent(tableName, new ArrayList<>());
          tableColsMap.get(tableName).addAll(columnNames);
        }
        if (columnTypes.size() > 0 && !tableColTypesMap.containsKey(tableName)) {
          tableColTypesMap.putIfAbsent(tableName, new ArrayList<>());
          tableColTypesMap.get(tableName).addAll(columnTypes);
        }
      }
    }
  }

  public static void writeMapsToFile(
      Map<String, List<String>> tableColsMap,
      Map<String, List<String>> tableColTypesMap
  )
  {
    File colsOutputFile = new File(ExcelReader.getOutputDir(), "ApmColumns.txt");
    File typesOutputFile = new File(ExcelReader.getOutputDir(), "ApmColTypes.txt");

//    分别遍历两个map，将数据写入到两个文件中
    try (FileWriter writer = new FileWriter(colsOutputFile)) {
      for (Map.Entry<String, List<String>> entry : tableColsMap.entrySet()) {
        String line = entry.getKey() + ": [" + String.join(", ", entry.getValue()) + "]\n";
//      System.out.println("line:" + line);
        writer.write(line);
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }

    try (FileWriter writer = new FileWriter(typesOutputFile)) {
      for (Map.Entry<String, List<String>> entry : tableColTypesMap.entrySet()) {
        String line = entry.getKey() + ": [" + String.join(", ", entry.getValue()) + "]\n";
//      System.out.println("line:" + line);
        writer.write(line);
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }

  }

  /*private static void removeFileIfExists(File outputFile)
  {
    // 检查文件是否存在
    if (outputFile.exists()) {
      // 删除文件
      if (outputFile.delete()) {
        System.out.println("文件已成功删除");
      } else {
        System.out.println("无法删除文件");
      }
    } else {
      System.out.println("文件不存在");
    }
  }*/

}
