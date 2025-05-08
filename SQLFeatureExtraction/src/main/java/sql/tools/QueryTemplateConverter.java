package sql.tools;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.core.type.TypeReference;
import lombok.NoArgsConstructor;
import org.jetbrains.annotations.Nullable;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import java.time.ZoneOffset;
import java.util.*;
import java.util.stream.Collectors;
import java.time.LocalDateTime;
import java.security.MessageDigest;
import java.util.stream.Stream;

public class QueryTemplateConverter
{
  public static final TypeReference<Map<String, List<Query>>> MAP_TYPE_REF = new TypeReference<Map<String, List<Query>>>()
  {
  };
  public static final ObjectMapper mapper = new ObjectMapper()
      .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES) // 全局忽略未知字段:ml-citation{ref="5" data="citationList"}
      .setDefaultMergeable(Boolean.TRUE); // 允许合并默认值
  public static final String[] allDimensions = {
      "dwm_request.type",
      "dwm_request.group",
      "dwm_request.appid",
      "dwm_request.appsysid",
      "dwm_request.agent",
      "dwm_request.path",
      "dwm_request.method",
      "dwm_request.root_appid",
      "dwm_request.pappid",
      "dwm_request.pappsysid",
      "dwm_request.pagent",
      "dwm_request.pagent_ip",
      "dwm_request.uevent_model",
      "dwm_request.uevent_id",
      "dwm_request.user_id",
      "dwm_request.session_id",
      "dwm_request.host",
      "dwm_request.ip_addr",
      "dwm_request.province",
      "dwm_request.city",
      "dwm_request.page_id",
      "dwm_request.page_group",
      "dwm_request.tag",
      "dwm_request.service_type",
      "dwm_request.papp_type",
      "dwm_request.status",
      "dwm_request.status_code",
      "dwm_request.code",
      "dwm_request.is_model"
  };
  private static String[] defaultDimensions = {"appid"};
  private static String[] defaultMetrics = {
      "count",
      "dur_sum",
      "dur_min",
      "dur_max"
  };

  // 输入数据结构
  static class InputData
  {
    Map<String, List<Query>> data;
  }

  @NoArgsConstructor
  @JsonIgnoreProperties(ignoreUnknown = true)
  static class Query
  {
    // 按列分组
    List<String> groupByCols = new ArrayList<>();
    // 按列排序
    List<String> orderByCols = new ArrayList<>();
    // 查询列
    List<String> projCols = new ArrayList<>();
    // 查询粒度
    @Nullable
    String queryGranularity;
    // 查询时间
    @Nullable
    String queryTime;
    // 选择列
    @Nullable
    List<String> selCols = new ArrayList<>();
    // 表名
    @Nullable
    List<String> tables = new ArrayList<>();
    // 时间范围
    @Nullable
    String timeRange;
    // 时间偏移
    @Nullable
    String timeOffset;
    // 求和列
    @Nullable
    List<String> sumCols = new ArrayList<>();
    @Nullable
    List<String> maxCols = new ArrayList<>();
    @Nullable
    List<String> minCols = new ArrayList<>();
    // 平均列
    @Nullable
    List<String> avgCols;

    @JsonCreator
    public Query(
        @Nullable @JsonProperty("groupByCols") List<String> groupByCols,
        @Nullable @JsonProperty("orderByCols") List<String> orderByCols,
        @Nullable @JsonProperty("projCols") List<String> projCols,
        @Nullable @JsonProperty("queryGranularity") String queryGranularity,
        @Nullable @JsonProperty("queryTime") String queryTime,
        @Nullable @JsonProperty("selCols") List<String> selCols,
        @Nullable @JsonProperty("tables") List<String> tables,
        @Nullable @JsonProperty("timeRange") String timeRange,
        @Nullable @JsonProperty("timeOffset") String timeOffset,
        @Nullable @JsonProperty("sumCols") List<String> sumCols,
        @Nullable @JsonProperty("maxCols") List<String> maxCols,
        @Nullable @JsonProperty("minCols") List<String> minCols,
        @Nullable @JsonProperty("avgCols") List<String> avgCols
    )
    {
      this.groupByCols = groupByCols != null ? groupByCols : new ArrayList<>();
      this.orderByCols = orderByCols != null ? orderByCols : new ArrayList<>();
      this.projCols = projCols != null ? projCols : new ArrayList<>();
      this.queryGranularity = queryGranularity;
      this.queryTime = queryTime;
      this.selCols = selCols != null ? selCols : new ArrayList<>();
      this.tables = tables;
      this.timeRange = timeRange;
      this.timeOffset = timeOffset;
      this.sumCols = sumCols != null ? sumCols : new ArrayList<>();
      this.maxCols = maxCols != null ? maxCols : new ArrayList<>();
      this.minCols = minCols != null ? minCols : new ArrayList<>();
      this.avgCols = avgCols != null ? avgCols : new ArrayList<>();
    }

    @JsonProperty("groupByCols")
    public @Nullable List<String> getGroupByCols()
    {
      return groupByCols;
    }

    @JsonSetter(value = "groupByCols", nulls = Nulls.SKIP)
    public void setGroupByCols(@Nullable List<String> groupByCols)
    {
      this.groupByCols = groupByCols != null ? groupByCols : new ArrayList<>();
    }

    @JsonProperty("orderByCols")
    public @Nullable List<String> getOrderByCols()
    {
      return orderByCols;
    }

    @JsonSetter(value = "orderByCols", nulls = Nulls.SKIP)
    public void setOrderByCols(@Nullable List<String> orderByCols)
    {
      this.orderByCols = orderByCols;
    }

    @JsonProperty("projCols")
    public @Nullable List<String> getProjCols()
    {
      return projCols;
    }

    @JsonSetter(value = "projCols", nulls = Nulls.SKIP)
    public void setProjCols(@Nullable List<String> projCols)
    {
      this.projCols = projCols != null ? projCols : new ArrayList<>();
    }

    @JsonProperty("queryGranularity")
    public @Nullable String getQueryGranularity()
    {
      return queryGranularity;
    }

    @JsonSetter(value = "queryGranularity", nulls = Nulls.SKIP)
    public void setQueryGranularity(@Nullable String queryGranularity)
    {
      this.queryGranularity = queryGranularity;
    }

    @JsonProperty("queryTime")
    public @Nullable String getQueryTime()
    {
      return queryTime;
    }

    @JsonSetter(value = "queryTime", nulls = Nulls.SKIP)
    public void setQueryTime(@Nullable String queryTime)
    {
      this.queryTime = queryTime;
    }

    @JsonProperty("selCols")
    public @Nullable List<String> getSelCols()
    {
      return selCols;
    }

    @JsonSetter(value = "selCols", nulls = Nulls.SKIP)
    public void setSelCols(@Nullable List<String> selCols)
    {
      this.selCols = selCols != null ? selCols : new ArrayList<>();
    }

    @JsonProperty("tables")
    public @Nullable List<String> getTables()
    {
      return tables;
    }

    @JsonSetter(value = "tables", nulls = Nulls.SKIP)
    public void setTables(@Nullable List<String> tables)
    {
      this.tables = tables;
    }

    @JsonProperty("timeRange")
    public @Nullable String getTimeRange()
    {
      return timeRange;
    }

    @JsonSetter(value = "timeRange", nulls = Nulls.SKIP)
    public void setTimeRange(@Nullable String timeRange)
    {
      this.timeRange = timeRange;
    }

    @JsonProperty("timeOffset")
    public @Nullable String getTimeOffset()
    {
      return timeOffset;
    }

    @JsonSetter(value = "timeOffset", nulls = Nulls.SKIP)
    public void setTimeOffset(@Nullable String timeOffset)
    {
      this.timeOffset = timeOffset;
    }

    @JsonProperty("sumCols")
    public @Nullable List<String> getSumCols()
    {
      return sumCols;
    }

    @JsonProperty("maxCols")
    public @Nullable List<String> getMaxCols()
    {
      return maxCols;
    }

    @JsonProperty("minCols")
    public @Nullable List<String> getMinCols()
    {
      return minCols;
    }

    @JsonSetter(value = "sumCols", nulls = Nulls.SKIP)
    public void setSumCols(@Nullable List<String> sumCols)
    {
      this.sumCols = sumCols != null ? sumCols : new ArrayList<>();
    }

    @JsonSetter(value = "maxCols", nulls = Nulls.SKIP)
    public void setMaxCols(@Nullable List<String> maxCols)
    {
      this.maxCols = maxCols != null ? maxCols : new ArrayList<>();
    }

    @JsonSetter(value = "minCols", nulls = Nulls.SKIP)
    public void setMinCols(@Nullable List<String> minCols)
    {
      this.minCols = minCols != null ? minCols : new ArrayList<>();
    }

    @JsonProperty("avgCols")
    public @Nullable List<String> getAvgCols()
    {
      return avgCols;
    }

    @JsonSetter(value = "avgCols", nulls = Nulls.SKIP)
    public void setAvgCols(@Nullable List<String> avgCols)
    {
      this.avgCols = avgCols != null ? avgCols : new ArrayList<>();
    }

    public List<String> getMetrics()
    {
      return Stream.of(projCols, sumCols, maxCols, minCols, avgCols).flatMap(List::stream).collect(Collectors.toList());
    }
  }

  // 输出模板结构
  public static class OutputTemplate
  {
    String dataSource;
    String intervalStr;
    List<String> dimensions;
    List<String> metrics;
    int lifeTime;
    QueryGranularity queryGranularity;

    public OutputTemplate()
    {
      metrics = new ArrayList<>(Collections.singletonList("count"));
    }

    @JsonCreator
    public OutputTemplate(
        @JsonProperty("dataSource") String dataSource,
        @JsonProperty("intervalStr") String intervalStr,
        @JsonProperty("dimensions") List<String> dimensions,
        @JsonProperty("metrics") List<String> metrics,
        @JsonProperty("lifeTime") int lifeTime,
        @JsonProperty("queryGranularity") QueryGranularity queryGranularity
    )
    {
      this.dataSource = dataSource;
      this.intervalStr = intervalStr;
      this.dimensions = dimensions;
      this.metrics = metrics;
      this.lifeTime = lifeTime;
      this.queryGranularity = queryGranularity;
    }

    @JsonProperty("dataSource")
    public String getDataSource()
    {
      return dataSource;
    }

    @JsonProperty("intervalStr")
    public String getIntervalStr()
    {
      return intervalStr;
    }

    @JsonProperty("dimensions")
    public List<String> getDimensions()
    {
      return dimensions;
    }

    @JsonProperty("metrics")
    public List<String> getMetrics()
    {
      return metrics;
    }

    @JsonProperty("lifeTime")
    public int getLifeTime()
    {
      return lifeTime;
    }

    @JsonProperty("queryGranularity")
    public QueryGranularity getQueryGranularity()
    {
      return queryGranularity;
    }
  }

  static class QueryGranularity
  {
    String type = "period";
    String period;

    @JsonCreator
    public QueryGranularity(String period)
    {
      this.period = period;
    }

    @JsonProperty("type")
    public String getType()
    {
      return type;
    }

    @JsonProperty("period")
    public String getPeriod()
    {
      return period;
    }
  }

  public static void main(String[] args) throws Exception
  {
    // 示例输入数据
    LocalDateTime queryEventTime = LocalDateTime.now();
    String inputJson = "{\"dwm_request\":[{\"groupByCols\":[\"dwm_request.ts\"],"
                       + "\"projCols\":[\"dwm_request.ts\"],\"queryGranularity\":\"604800s\","
                       + "\"queryTime\":\"17h,Fri\",\"selCols\":[\"dwm_request.ts\"],"
                       + "\"tables\":[\"dwm_request\"],\"timeRange\":\"3600s\"},"
                       + "{\"avgCols\":[\"dwm_request.dur\"],"
                       + "\"projCols\":[\"dwm_request.tolerated\",\"dwm_request.frustrated\",\"dwm_request.dur\"],"
                       + "\"queryTime\":\"17h,Fri\",\"sumCols\":[\"dwm_request.tolerated\",\"dwm_request.frustrated\"],"
                       + "\"tables\":[\"dwm_request\"],\"timeOffset\":\"60s\",\"timeRange\":\"3600s\"}]}";
    System.out.println("Input JSON:\n" + inputJson);
    Collection<OutputTemplate> mergedTemplates = convertToTemplateJson(queryEventTime, inputJson);
    // 输出结果
    System.out.println(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(mergedTemplates));
  }

  public static Collection<OutputTemplate> convertToTemplateJson(LocalDateTime queryEventTime, String inputJson)
      throws Exception
  {
    // 1. 解析输入数据
    Map<String, List<Query>> tableQueryTemps = mapper.readValue(inputJson, MAP_TYPE_REF);

    // 2. 生成候选模板
    List<OutputTemplate> candidates = new ArrayList<>();
    for (List<Query> queries : tableQueryTemps.values()) {
      for (Query query : queries) {
        if (query.getQueryTime() == null || query.getTables() == null || query.getTables().size() == 0) {
          continue;
        }
        OutputTemplate outputTemplate = convertToTemplate(query, queryEventTime);
        if (outputTemplate.dimensions == null || outputTemplate.dimensions.size() == 0) {
          //使用默认维度
          outputTemplate.dimensions = Arrays.asList(defaultDimensions);
        }
        if (outputTemplate.metrics == null || outputTemplate.metrics.size() == 0) {
          //使用默认指标
          outputTemplate.metrics = Arrays.asList(defaultMetrics);
        }
        candidates.add(outputTemplate);
      }
    }

    // 3. 合并模板
    Map<String, OutputTemplate> merged = mergeTemplates(candidates);
    // return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(merged.values());
    System.out.println("合并后的候选模板：\n" + mapper.writerWithDefaultPrettyPrinter().writeValueAsString(merged));
    return merged.values();
  }

  private static List<String> selectDimensions(Query query, List<String> allDims)
  {
    // 维度字段（过滤，合并去重）
    List<String> candidateDims = Stream.of(query.projCols, query.selCols, query.groupByCols)
                                       .flatMap(Collection::stream)
                                       .filter(field -> allDims.contains(field))
                                       .map(s -> s.split("\\.")[1]) // 提取字段名
                                       .distinct()
                                       .collect(Collectors.toList());
    return candidateDims;
  }

  // 转换为候选模板
  static OutputTemplate convertToTemplate(Query query, LocalDateTime queryEventTime)
  {
    OutputTemplate template = new OutputTemplate();

    // 基础字段
    template.dataSource = query.tables.get(0);
    template.lifeTime = parseLifeTime(query.queryTime);
    List<String> allDims = Arrays.asList(allDimensions);
    // 模板维度字段处理
    template.dimensions = selectDimensions(query, allDims);

    // 指标字段（根据sum/max/min选择合并生成）
    template.metrics = selectMetrics(query, allDims);

    // 时间相关处理（示例时间范围）
    template.intervalStr = computeIntervalStr(queryEventTime, query.getTimeOffset(), query.getTimeRange());

    // 聚合粒度处理
    template.queryGranularity = new QueryGranularity(parseGranularity(query.queryGranularity));
    return template;
  }

  private static List<String> selectMetrics(Query query, List<String> allDims)
  {
    List<String> candidateMetrics = Stream.of(query.projCols, query.selCols, query.groupByCols, query.sumCols,
                                              query.avgCols, query.maxCols, query.minCols
                                          )
                                          .flatMap(Collection::stream)
                                          .filter(field -> !allDims.contains(field) && !field.equals("dwm_request.ts"))
                                          .map(s -> s.split("\\.")[1]) // 提取字段名
                                          .distinct()
                                          .collect(Collectors.toList());
    List<String> resultMetrics = new ArrayList<>();
    if (query.sumCols != null) {
      candidateMetrics.forEach(col -> {
        resultMetrics.add(col + "_sum");
        resultMetrics.add(col + "_max");
        resultMetrics.add(col + "_min");
      });
    }
    return resultMetrics;
  }

  private static String computeIntervalStr(
      LocalDateTime queryEventTime,
      @Nullable String timeOffset, //eventTimeSec - timeRange End
      @Nullable String timeRange
  )
  {
    if (timeOffset == null) {
      timeOffset = "0";//second
    }
    if (timeRange == null) {
      timeRange = "60";//second
    }
    long timeRangeEnd = queryEventTime.toEpochSecond(ZoneOffset.ofHours(8)) - Long.parseLong(timeOffset.replaceAll(
        "\\D+",
        ""
    ));
    long timeRangeStart = timeRangeEnd - Long.parseLong(timeRange.replaceAll("\\D+", ""));
    Interval interval = new Interval(timeRangeStart * 1000, timeRangeEnd * 1000, DateTimeZone.UTC);
    return interval.toString();
  }

  // 合并模板
  static Map<String, OutputTemplate> mergeTemplates(List<OutputTemplate> candidates)
  {
    Map<String, OutputTemplate> merged = new LinkedHashMap<>();

    for (OutputTemplate t : candidates) {
      // 生成唯一key：表名+维度哈希+截断粒度
      String key = t.dataSource + "_" + truncateGranularity(t.queryGranularity.period)
                   + "_" + hashDimensions(t.dimensions);

      merged.compute(key, (k, v) -> {
        if (v == null) {
          return t;
        }

        // 合并interval
        v.intervalStr = mergeInterval(v.intervalStr, t.intervalStr);

        // 合并维度
        Set<String> dims = new LinkedHashSet<>(v.dimensions);
        dims.addAll(t.dimensions);
        v.dimensions = new ArrayList<>(dims);

        // 合并指标
        Set<String> metrics = new LinkedHashSet<>(v.metrics);
        metrics.addAll(t.metrics);
        v.metrics = new ArrayList<>(metrics);

        // 更新有效期
        v.lifeTime = Math.max(v.lifeTime, t.lifeTime);
        return v;
      });
    }

    return merged;
  }

  private static String mergeInterval(String intervalStr, String intervalStr1)
  {
    Interval interval1 = Interval.parse(intervalStr);
    Interval interval2 = Interval.parse(intervalStr1);
    //如果两个时段存在重叠，则合并成一个时段
    if (interval1.overlaps(interval2)) {
      return new Interval(
          Math.min(interval1.getStartMillis(), interval2.getStartMillis()),
          Math.max(interval1.getEndMillis(), interval2.getEndMillis()),
          DateTimeZone.UTC
      ).toString();
    } else {
      return intervalStr + "," + intervalStr1;
    }
  }

  // 辅助方法：解析生命周期
  static int parseLifeTime(String queryTime)
  {
    if (queryTime == null || queryTime.isEmpty()) {
      return 23;
    }
    String[] parts = queryTime.split(",");
    String h = parts[0].replaceAll("\\D+", "");
    if (h.equals("")) {
      return 23;
    }
    return Integer.parseInt(h);
  }

  // 解析时间粒度（示例转换逻辑）
  static String parseGranularity(String seconds)
  {
    if (seconds == null) {
      return "P1D";
    }
    long sec = Long.parseLong(seconds.replaceAll("\\D+", ""));
    if (sec >= 604800) {
      return "P7D"; // 7天
    }
    if (sec >= 86400) {
      return "P1D";  // 1天
    }
    if (sec >= 3600) {
      return "PT1H";  // 1小时
    }
    return "PT1M";                   // 默认分钟级
  }

  // 截断粒度为小时/天/分钟
  static String truncateGranularity(String period)
  {
    if (period.contains("D")) {
      return "day";
    }
    if (period.contains("H")) {
      return "hour";
    }
    return "minute";
  }

  // 生成维度字段哈希
  static String hashDimensions(List<String> dims)
  {
    try {
      MessageDigest md = MessageDigest.getInstance("SHA-1");
      byte[] hash = md.digest(String.join(",", dims).getBytes());
      return bytesToHex(hash).substring(0, 8); // 取前8位
    }
    catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private static String bytesToHex(byte[] bytes)
  {
    StringBuilder sb = new StringBuilder();
    for (byte b : bytes) {
      sb.append(String.format("%02x", b));
    }
    return sb.toString();
  }
}
