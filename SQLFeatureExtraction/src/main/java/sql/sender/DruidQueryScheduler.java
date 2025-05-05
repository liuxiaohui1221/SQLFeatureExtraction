package sql.sender;

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

import static sql.reader.ExcelReader.candidateTopTables;
import static sql.tools.IOUtil.loadAndParse;

public class DruidQueryScheduler
{

  // Druid Broker查询端点
  private static final String DRUID_QUERY_URL = "http://localhost:8888/druid/v2/sql";

  // SQL事件数据结构
  static class SqlEvent implements Comparable<SqlEvent>
  {
    final LocalDateTime eventTime;
    final String sql;

    SqlEvent(LocalDateTime eventTime, String sql)
    {
      this.eventTime = eventTime;
      this.sql = sql;
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
    String tsvFilePath = "/home/xhh/db_workspace/SQLFeatureExtraction/SQLFeatureExtraction/input/0318_ApmQuerys.tsv";
    // 1. 读取并解析SQL文件
//    List<SqlEvent> events = parseSqlFile(Paths.get(args[0]));
    candidateTopTables.put("dwm_request", 0);
    List<SqlEvent> events = loadQueries(Path.of(tsvFilePath), candidateTopTables);
//    records.sort(Comparator.comparing(r -> r.eventTime));
    // 2. 按时间排序
//    events.sort(SqlEvent::compareTo);

    // 3. 创建调度线程池
    ScheduledExecutorService executor = Executors.newSingleThreadScheduledExecutor();

    // 4. 计算初始延迟
    LocalDateTime firstEventTime = events.get(0).eventTime;
    long initialDelay = calculateDelay(LocalDateTime.now(), firstEventTime);

    // 5. 调度第一个任务
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
      HashMap<String, Integer> candidateTopTables
  ) throws IOException
  {
    return loadAndParse(of, candidateTopTables).stream().map(r -> new SqlEvent(r.getEventTime(), r.getSql())).toList();
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

    public static void main(String[] args) throws Exception
    {
      String sql =
          "SELECT TIME_FLOOR(__time, 'PT10M') ,sum(\"err_sum\") AS err_RESP, sum(\"fail_sum\") AS fail_RESP, sum(\"frustrated_sum\") AS frustrated_RESP, sum(\"tolerated_sum\") AS slow_RESP, count(1) AS total_RESP \n"
          + "FROM dwm_request WHERE (appsysid = '9ba9403b-b000-4a4e-9d85-8e831bbf9d06') GROUP BY 1 limit 10";
      sendToDruid(sql);
    }

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
            "[%s] Executing query: %s%n",
            LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
            currentEvent.sql
        );

        // 发送Druid查询
        sendToDruid(currentEvent.sql);

        // 调度下一个任务
        if (currentIndex < events.size() - 1) {
          SqlEvent nextEvent = events.get(currentIndex + 1);
          long delay = Duration.between(
              currentEvent.eventTime,
              nextEvent.eventTime
          ).toMillis();

          executor.schedule(
              new QueryTask(executor, events, currentIndex + 1),
              delay,
              TimeUnit.MILLISECONDS
          );
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

      try (Connection connection = DriverManager.getConnection(url, connectionProperties)) {
        long startTime = System.currentTimeMillis();
        try (
            final Statement statement = connection.createStatement();
            final ResultSet resultSet = statement.executeQuery(query)
        ) {
//          while (resultSet.next()) {
//            System.out.printf("Response [%s]\n", resultSet.getString(1));
//          }
        }
        finally {
          System.out.println("Cost " + (System.currentTimeMillis() - startTime) + "ms");
        }
      }
    }
  }
}
