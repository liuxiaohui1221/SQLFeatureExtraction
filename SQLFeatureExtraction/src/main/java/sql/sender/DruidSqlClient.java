package sql.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;

import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;

public class DruidSqlClient
{

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final CloseableHttpClient httpClient = HttpClients.createDefault();
  private static final String DRUID_SQL_ENDPOINT = "http://localhost:8888/druid/v2/sql";

  /**
   * 执行 Druid SQL 查询
   *
   * @param query         SQL 语句
   * @param resultFormat  响应格式 (object/array/objectLines/arrayLines/csv)
   * @param includeHeader 是否包含列头
   * @param parameters    查询参数列表
   * @return 根据格式返回对应数据结构
   */
  public static Object executeDruidQuery(
      String query,
      String resultFormat,
      boolean includeHeader,
      List<SqlParameter> parameters,
      Map<String, Object> context
  ) throws IOException
  {
    // 构建请求体
    Map<String, Object> requestBody = new HashMap<>();
    requestBody.put("query", query);
    requestBody.put("resultFormat", resultFormat);
    requestBody.put("header", includeHeader);

    if (parameters != null && !parameters.isEmpty()) {
      requestBody.put("parameters", parameters);
    }
    if (context != null) {
      requestBody.put("context", context);
    }
    HttpPost httpPost = new HttpPost(DRUID_SQL_ENDPOINT);
    httpPost.setHeader("Content-Type", "application/json");
    httpPost.setEntity(new StringEntity(mapper.writeValueAsString(requestBody)));
    System.out.printf(
        "[%s] Executing druid query: %s%n",
        LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
        query
    );
    try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
      HttpEntity entity = response.getEntity();
      if (entity == null) {
        throw new IOException("Empty response from Druid");
      }

      String responseBody = EntityUtils.toString(entity);
      int statusCode = response.getStatusLine().getStatusCode();

      if (statusCode != 200) {
        throw new IOException("Druid API Error: " + responseBody);
      }

      return parseResponse(responseBody, resultFormat);
    }
  }

  /**
   * 解析不同格式的响应
   */
  private static Object parseResponse(String responseBody, String resultFormat) throws IOException
  {
    return switch (resultFormat.toLowerCase()) {
      case "object" -> mapper.readValue(responseBody, List.class); // List<Map<String, Object>>
      case "array" -> mapper.readValue(responseBody, List.class); // List<List<Object>>
      case "objectlines", "arraylines", "csv" -> responseBody; // 原始字符串
      default -> throw new IllegalArgumentException("Unsupported result format: " + resultFormat);
    };
  }

  /**
   * SQL 参数包装类
   */
  public static class SqlParameter
  {
    private final String type;
    private final Object value;

    public SqlParameter(String type, Object value)
    {
      this.type = type;
      this.value = value;
    }

    // Jackson 序列化需要 getter
    public String getType() {return type;}

    public Object getValue() {return value;}
  }

  // 使用示例
  public static void main(String[] args)
  {
    try {
      // 示例参数化查询
//      List<SqlParameter> params = Arrays.asList(
//          new SqlParameter("ARRAY", Arrays.asList(999.0, null, 5.5)),
//          new SqlParameter("VARCHAR", "bar")
//      );
      String testsql =
          "SELECT TIME_FLOOR(__time, 'PT10M') ,sum(\"err_sum\") AS err_RESP, sum(\"fail_sum\") AS fail_RESP, sum(\"frustrated_sum\") AS frustrated_RESP, sum(\"tolerated_sum\") AS slow_RESP, count(1) AS total_RESP \n"
          + "FROM dwm_request WHERE (appsysid = '9ba9403b-b000-4a4e-9d85-8e831bbf9d06') GROUP BY 1 limit 10";
      Object result = executeDruidQuery(
          testsql,
          "object",
          true,
          null, null
      );

      if (result instanceof List) {
        ((List<?>) result).forEach(System.out::println);
      } else {
        System.out.println(result);
      }
    }
    catch (IOException e) {
      e.printStackTrace();
    }
  }
}
