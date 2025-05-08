package sql.sender;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.extern.slf4j.Slf4j;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import sql.tools.QueryTemplateConverter;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static sql.tools.QueryTemplateConverter.convertToTemplateJson;

@Slf4j
public class PredictionClient
{

  private static final ObjectMapper mapper = new ObjectMapper();
  private static final CloseableHttpClient httpClient = HttpClients.createDefault();
  private static final String PREDICT_URL = "http://127.0.0.1:6666/predict";
  private static final String CREATE_TEMPLATE_TASK_URL = "http://localhost:8081/druid/indexer/v1/datasources"
                                                         + "/dwm_request/createPreQueryTemplate";

  /**
   * 执行预测请求
   *
   * @param windowVector one-hot查询窗口特征
   * @return 预测结果字符串（根据实际接口响应调整）
   * @throws PredictionException 包含错误信息的自定义异常
   */
  public static String predictTemplate(
      LocalDateTime queryEventTime,
      String windowVector,
      boolean skipCreateTemplateTask
  ) throws PredictionException
  {
    HttpPost httpPost = new HttpPost(PREDICT_URL);
    httpPost.setHeader("Content-Type", "application/json");

    try {
      // 构建请求体
      Map<String, Object> requestBody = new HashMap<>();
      requestBody.put("input", windowVector);
      String jsonBody = mapper.writeValueAsString(requestBody);
      httpPost.setEntity(new StringEntity(jsonBody));

      // 预测下个窗口
      try (CloseableHttpResponse response = httpClient.execute(httpPost)) {
        int statusCode = response.getStatusLine().getStatusCode();
        HttpEntity entity = response.getEntity();
        String responseBody = entity != null ? EntityUtils.toString(entity) : null;
        //转换合并模板
        if (statusCode == 200 && responseBody != null) {
          Collection<QueryTemplateConverter.OutputTemplate> outputTemplates = convertToTemplateJson(
              queryEventTime,
              responseBody
          );
          if (!skipCreateTemplateTask) {
            // 创建预查询模板任务
            for (QueryTemplateConverter.OutputTemplate outputTemplate : outputTemplates) {
              int statusCodeTemplate = createPreQueryTemplate(outputTemplate);
              System.out.println("创建模板状态码: " + statusCodeTemplate);
              if (statusCode != 200) {
                return "创建模板失败";
              }
            }
            return "创建模板成功";
          } else {
            return "跳过创建模板";
          }
        } else {
          throw new PredictionException("API 请求失败", statusCode, responseBody);
        }
      }
    }
    catch (Exception e) {
      e.printStackTrace();
//      throw new PredictionException("请求处理异常: " + e.getMessage(), e);
    }
    return "请求处理异常";
  }

  /**
   * 创建预查询模板
   *
   * @param request 请求体对象
   * @return HTTP 状态码
   * @throws IOException 当请求失败或参数非法时抛出
   */
  public static int createPreQueryTemplate(QueryTemplateConverter.OutputTemplate request) throws IOException
  {
    // 参数校验
    if (request.getDataSource() == null || request.getDataSource().isEmpty()) {
      throw new IllegalArgumentException("dataSource cannot be empty");
    }
    if (request.getIntervalStr() == null || request.getIntervalStr().isEmpty()) {
      throw new IllegalArgumentException("intervalStr cannot be empty");
    }

    try (CloseableHttpClient httpClient = HttpClients.createDefault()) {
      HttpPost httpPost = new HttpPost(CREATE_TEMPLATE_TASK_URL);
      httpPost.setHeader("Content-Type", "application/json");
      httpPost.setHeader("skipCheck", String.valueOf(true)); // 设置 Basic 认证头

      // 序列化请求体
      String jsonBody = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(request);
      httpPost.setEntity(new StringEntity(jsonBody));
      log.info("请求体：{}", jsonBody);
      HttpResponse response = httpClient.execute(httpPost);
      return response.getStatusLine().getStatusCode();
    }
  }

  /**
   * 自定义异常类
   */
  public static class PredictionException extends Exception
  {
    private final int statusCode;
    private final String responseBody;

    public PredictionException(String message, Throwable cause)
    {
      super(message, cause);
      this.statusCode = -1;
      this.responseBody = null;
    }

    public PredictionException(String message, int statusCode, String responseBody)
    {
      super(message + " [Status: " + statusCode + "]");
      this.statusCode = statusCode;
      this.responseBody = responseBody;
    }

    public int getStatusCode() {return statusCode;}

    public String getResponseBody() {return responseBody;}
  }

  // 使用示例
  public static void main(String[] args)
  {
    String sampleInput = "10000100000000000000000001000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000011110000000000000000001000000000000000000010000000000000100011000000000000000000000010000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000001000110000000000000000000000100000000000000000000000000000000000000000100000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000000111100000000000000"; // 您的长二进制字符串

    try {
      String prediction = predictTemplate(LocalDateTime.now(), sampleInput, true);
      System.out.println("结果: " + prediction);
    }
    catch (PredictionException e) {
      System.err.println("预测失败: " + e.getMessage());
      if (e.getResponseBody() != null) {
        System.err.println("错误详情: " + e.getResponseBody());
      }
    }
  }
}

