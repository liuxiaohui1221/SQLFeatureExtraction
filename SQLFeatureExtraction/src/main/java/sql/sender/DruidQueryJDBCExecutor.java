package sql.sender;

import sql.pojo.QueryWindowRecord;
import sql.tools.IOUtil;

import java.io.IOException;
import java.nio.file.*;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;

import static sql.reader.ExcelReader.candidateTopTables;
import static sql.sender.DruidSqlClient.executeDruidQuery;
import static sql.sender.PredictionClient.predictTemplate;
import static sql.tools.IOUtil.loadAndParse;
import static sql.tools.SQLConverter.convertClickhouseToDruid;

public class DruidQueryJDBCExecutor
{

  // Druid Broker查询端点
  private static final String DRUID_QUERY_URL = "http://localhost:8888/druid/v2/sql";
  private static final List<String> queryCosts = new ArrayList<>();
  private static final AtomicLong totalCost = new AtomicLong(0);
  private static volatile int beforeWindowVector = -1;
  private static final long maxDelayMill = 1000;
  private static final boolean skipCreateTemplateTask = true;

  // SQL事件数据结构
  static class SqlEvent implements Comparable<SqlEvent>
  {
    final LocalDateTime eventTime;
    final String sql;
    String windowVector;

    SqlEvent(LocalDateTime eventTime, String sql)
    {
      this.eventTime = eventTime;
      this.sql = sql;
    }

    SqlEvent(LocalDateTime eventTime, String sql, String windowVector)
    {
      this.eventTime = eventTime;
      this.sql = sql;
      this.windowVector = windowVector;
    }

    @Override
    public int compareTo(SqlEvent o)
    {
      return this.eventTime.compareTo(o.eventTime);
    }
  }

  public static void main(String[] args) throws Exception
  {
    // 参数校验
//    if (args.length != 1) {
//      System.err.println("Usage: java DruidQueryScheduler <sql-file>");
//      System.exit(1);
//    }
    boolean loadOnlyTest = true;
//    String tsvFilePath = "/home/xhh/db_workspace/SQLFeatureExtraction/SQLFeatureExtraction/input/0318_ApmQuerys.tsv";
    String tsvFilePath = "/home/xhh/db_workspace/SQLFeatureExtraction/SQLFeatureExtraction/output/1/2025-05-05/test/test";
    // 1. 读取并解析SQL文件
//    List<SqlEvent> events = parseSqlFile(Paths.get(args[0]));
    candidateTopTables.put("dwm_request", 0);
    List<SqlEvent> events = loadQueries(Path.of(tsvFilePath), candidateTopTables, loadOnlyTest);

    // 2. 创建调度线程池
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    // 3. 计算初始延迟
    LocalDateTime firstEventTime = events.get(0).eventTime;
    long initialDelay = calculateDelay(LocalDateTime.now(), firstEventTime);

    // 4. 调度第一个任务
    ScheduledFuture<?> future = executor.schedule(
        new QueryTask(executor, events, 0),
        initialDelay,
        TimeUnit.MILLISECONDS
    );

    // 添加关闭钩子
    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      executor.shutdownNow();
      System.out.println("\nScheduler shutdown complete.");
    }));
  }

  private static List<SqlEvent> loadQueries(
      Path of,
      HashMap<String, Integer> candidateTopTables,
      boolean loadOnlyTest
  ) throws IOException
  {
    if (loadOnlyTest) {
      return loadTestWindowQueries(of, candidateTopTables);
    }
    return loadAndParse(of, candidateTopTables).stream()/*.filter(r -> r.getSql().contains("GROUP BY"))*/
                                               .map(r -> new SqlEvent(r.getEventTime(), r.getSql())).toList();
  }

  private static List<SqlEvent> loadTestWindowQueries(Path path, HashMap<String, Integer> candidateTopTables)
      throws IOException
  {
    List<SqlEvent> events = new ArrayList<>();
    List<QueryWindowRecord> queryWindowRecords = IOUtil.loadTest(path, candidateTopTables);
    for (QueryWindowRecord record : queryWindowRecords) {
      for (String sql : record.getSqls()) {
        if (sql.length() < 10) {
          continue;
        }
        events.add(new SqlEvent(convertTimestampToLocalDateTime(record.getWindowTimeSec() * 1000), sql,
                                record.getWindowVector()
        ));
      }
    }
    return events;
  }

  public static LocalDateTime convertTimestampToLocalDateTime(long timestamp)
  {
    // 将时间戳转换为 Instant（假设时间戳单位是毫秒）
    Instant instant = Instant.ofEpochMilli(timestamp);
    // 转换为系统默认时区的 LocalDateTime
    return LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
  }
  // 文件解析方法
  private static List<SqlEvent> parseSqlFile(Path filePath) throws Exception
  {
    List<SqlEvent> events = new ArrayList<>();
    DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    try (Scanner scanner = new Scanner(filePath)) {
      while (scanner.hasNextLine()) {
        String line = scanner.nextLine().trim();
        if (line.isEmpty()) {
          continue;
        }

        String[] parts = line.split("\t");
        if (parts.length != 2) {
          System.err.println("Invalid line format: " + line);
          continue;
        }

        LocalDateTime eventTime = LocalDateTime.parse(parts[0], formatter);
        events.add(new SqlEvent(eventTime, parts[1]));
      }
    }
    return events;
  }

  // 计算时间差（处理过去时间）
  private static long calculateDelay(LocalDateTime now, LocalDateTime eventTime)
  {
    Duration duration = Duration.between(now, eventTime);
    return duration.isNegative() ? 0 : duration.toMillis();
  }

  // 查询任务类
  static class QueryTask implements Runnable
  {
    private final ScheduledExecutorService executor;
    private final List<SqlEvent> events;
    private final int currentIndex;

    /*public static void main(String[] args) throws Exception
    {
      String sql =
          "SELECT TIME_FLOOR(__time, 'PT10M') ,sum(\"err_sum\") AS err_RESP, sum(\"fail_sum\") AS fail_RESP, sum(\"frustrated_sum\") AS frustrated_RESP, sum(\"tolerated_sum\") AS slow_RESP, count(1) AS total_RESP \n"
          + "FROM dwm_request WHERE (appsysid = '9ba9403b-b000-4a4e-9d85-8e831bbf9d06') GROUP BY 1 limit 10";
      sendToDruid(sql);
    }*/

    QueryTask(ScheduledExecutorService executor, List<SqlEvent> events, int index)
    {
      this.executor = executor;
      this.events = events;
      this.currentIndex = index;
    }

    @Override
    public void run()
    {
      try {
        SqlEvent currentEvent = events.get(currentIndex);
        System.out.printf(
            "[%s] executing query[%s]: %s%n",
            currentEvent.eventTime.format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
            currentIndex,
            currentEvent.sql
        );
        String druidSQL=convertClickhouseToDruid(currentEvent.sql);
        //预测并发送预查询模板
        int curVector = currentEvent.windowVector.hashCode();
        if (beforeWindowVector != curVector) {
          String status = predictTemplate(currentEvent.eventTime, currentEvent.windowVector, skipCreateTemplateTask);
          System.out.println(status);
          beforeWindowVector = curVector;
        }
        boolean enableMaterializedView = true;
        boolean enableSubQueryReuse = true;
        //默认参数：
        boolean useResultLevelCache = true;
        boolean poulateCache = true;
        boolean useCache = true;
        // 发送Druid查询
        try {
//          sendToDruid(druidSQL);
          long startTime = System.currentTimeMillis();
          Map<String, Object> context = new HashMap<>();
          context.put("useApproximateTopN", false);
          context.put("useApproximateCountDistinct", false);
          context.put("useCache", useCache);
          context.put("populateCache", poulateCache);
          context.put("useResultLevelCache", useResultLevelCache);
          context.put("enableMaterializedView", enableMaterializedView);
          context.put("enableSubQueryReuse", enableSubQueryReuse);
          Object result = executeDruidQuery(
              druidSQL,
              "object",
              true,
              null, context
          );
          long cost = System.currentTimeMillis() - startTime;
          System.out.println("Cost " + cost + "ms");
          queryCosts.add(String.valueOf(cost));
          totalCost.addAndGet(cost);
        }
        catch (Exception e) {
          System.out.println("Error sending query to Druid: " + e.getMessage());
        }

        // 调度下一个任务
        if (currentIndex < events.size() - 1) {
          SqlEvent nextEvent = events.get(currentIndex + 1);
          // if query type is groupBy then delay 30s

          long delay = Math.min(Duration.between(
              currentEvent.eventTime,
              nextEvent.eventTime
          ).toMillis(), maxDelayMill);

          executor.schedule(
              new QueryTask(executor, events, currentIndex + 1),
              delay,
              TimeUnit.MILLISECONDS
          );
        } else {
          System.out.println("All queries executed. avg cost: " + totalCost.get() / events.size());
          System.out.println(useCache
                             + ","
                             + poulateCache
                             + ","
                             + useResultLevelCache
                             + ","
                             + enableMaterializedView
                             + ","
                             + enableSubQueryReuse);
          //保存到csv文件
          IOUtil.writeToFile(
              queryCosts,
              useResultLevelCache + "@" + enableMaterializedView + "_" + enableSubQueryReuse +
              "_queryCosts.csv"
          );

          executor.shutdown();
        }
      }
      catch (Exception e) {
        System.err.println("Error processing query: " + e.getMessage());
      }
    }

    // Druid查询执行方法
    public static void sendToDruid(String query) throws Exception
    {

      // Connect to /druid/v2/sql/avatica/ on your Broker.
      String url = "jdbc:avatica:remote:url=http://localhost:8888/druid/v2/sql/avatica/;transparent_reconnection=true";
      // Set any connection context parameters you need here.
      // Any property from https://druid.apache.org/docs/latest/querying/sql-query-context.html can go here.
      Properties connectionProperties = new Properties();
      connectionProperties.setProperty("sqlTimeZone", "Etc/UTC");
      //To connect to a Druid deployment protected by basic authentication,
      //you can incorporate authentication details from https://druid.apache.org/docs/latest/operations/security-overview
      connectionProperties.setProperty("user", "root");
      connectionProperties.setProperty("password", "123456");

        long startTime = System.currentTimeMillis();
      System.out.printf(
          "[%s] Executing druid query: %s%n",
          LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
          query
      );
        try (
            Connection connection = DriverManager.getConnection(url, connectionProperties);
            final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(query)
        ) {

          while (resultSet.next()) {
//            System.out.printf("Response [%s]\n", resultSet.getString(1));
          }
        }
        finally {
          System.out.println("Cost " + (System.currentTimeMillis() - startTime) + "ms");
        }
    }
  }
}
