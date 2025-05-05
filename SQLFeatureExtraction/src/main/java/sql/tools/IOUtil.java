package sql.tools;

import sql.pojo.QueryRecord;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Path;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static sql.reader.ExcelReader.outputDirector;

public class IOUtil
{
  public static final int seed = 999;
  public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy/M/d HH:mm");

  public static File getOutputDir(String outputDirector)
  {
    String projectPath = System.getProperty("user.dir"); // 获取当前工作目录，通常是项目根目录
    String curent_day = LocalDate.now().toString();
    String tempDir = "";
    if (outputDirector.lastIndexOf("/") == -1) {
      tempDir = outputDirector + "/" + curent_day;
    } else {
      tempDir = outputDirector + curent_day;
    }
    File outputDir = new File(projectPath, tempDir);

    // 确保 output 目录存在
    if (!outputDir.exists() && !outputDir.mkdirs()) {
      throw new IllegalStateException("Cannot create output directory.");
    }
    return outputDir;
  }

  public static List<QueryRecord> loadAndParse(
      Path file,
      HashMap<String, Integer> dictAllTables
  ) throws IOException
  {
    List<QueryRecord> records = new ArrayList<>();
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy/M/d H:mm:ss.SSS");
    try (BufferedReader br = new BufferedReader(new FileReader(file.toFile()))) {
      String line;
      br.readLine();
      Random random = new Random(seed);
      while ((line = br.readLine()) != null) {
        String[] parts = line.split("\t");
        if (parts.length < 3) {
          continue;
        }
        int seconds = random.nextInt(60);
        int micseconds = random.nextInt(1000);
        String secStr = seconds + "";
        if (seconds < 10) {
          secStr = "0" + seconds;
        }
        String micsecStr = micseconds + "";
        if (micseconds < 10) {
          micsecStr = "00" + micseconds;
        } else if (micseconds < 100) {
          micsecStr = "0" + micseconds;
        }
        LocalDateTime eventTime = LocalDateTime.parse(parts[1] + ":" + secStr + "." + micsecStr, dtf);
        long duration = Long.parseLong(parts[3]);
        String sql = parts[2];
        String table = extractTable(parts[0]);
        if (!dictAllTables.containsKey(table)) {
          continue;
        }
        records.add(new QueryRecord(
            eventTime,
            getTimeSec(parts[1]),
            duration,
            sql,
            table
        ));
      }
    }
    return records;
  }

  public static long getTimeSec(String timeString)
  {
    long timestamp = 0;
    try {
      Date date = sdf.parse(timeString);
      timestamp = date.getTime();
//      System.out.println("Timestamp: " + timestamp);
    }
    catch (java.text.ParseException e) {
      throw new RuntimeException(e);
    }
    return timestamp / 1000;
  }

  // 从SQL提取表名（需根据实际SQL格式调整）
  public static String extractTable(String tables)
  {
    String table = "";
    // 去掉字符串两端的方括号和单引号
    tables = tables.replace("[", "").replace("]", "").replace("'", "");

    // 按逗号分隔字符串，得到表名数组
    String[] tableNames = tables.split(",");

    // 输出提取的表名
    for (String tab : tableNames) {
//            如果表名以_cluster结尾，则去掉_cluster部分
      if (tab.endsWith("_cluster")) {
        tab = tab.substring(0, tab.length() - 8);
      }
      tab = tab.split("\\.")[1];
      table = tab;
    }
    return table;
  }

  public static void writeFile(Map<String, Integer> stringIntegerMap, String outputD, String filePath)
  {
    File outputFile = new File(outputD);
    // 确保 output 目录存在
    if (!outputFile.exists() && !outputFile.mkdirs()) {
      throw new IllegalStateException("Cannot create output directory.");
    }
    try (FileWriter writer = new FileWriter(new File(outputD, filePath))) {
      stringIntegerMap.forEach((tableName, index) -> {
        try {
          writer.write(tableName + ":" + index + "\n");
        }
        catch (IOException e) {
          e.printStackTrace();
        }
      });
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }

  public static void writeToFile(List<String> windowMetric, String file)
  {
    try (FileWriter writer = new FileWriter(getOutputDir(outputDirector) + file)) {
      windowMetric.forEach((metric) -> {
        try {
          writer.write(metric + "\n");
        }
        catch (IOException e) {
          e.printStackTrace();
        }
      });
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }
}
