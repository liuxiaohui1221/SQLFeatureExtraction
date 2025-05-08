package sql.tools;

import java.util.Map;
import java.util.regex.Pattern;

import java.util.*;
import java.util.regex.*;
import java.util.function.Function;

import static sql.tools.SqlGroupByReplacer.replaceGroupByWithPosition;

public class SQLConverter {
    // 替换规则定义（兼容JDK 1.8）
    private static final List<ReplacementRule> REPLACE_RULES = Arrays.asList(
            // 规则1: 表名处理（去除_cluster后缀）
            new ReplacementRule(
                    Pattern.compile("FROM\\s+([\\w]+)_cluster\\b"),
                    "FROM $1"
            ),
            new ReplacementRule(
                    Pattern.compile("JOIN\\s+([\\w]+)_cluster\\b"),
                    "JOIN $1"
            ),
            // 规则2: 时间字段ts转为__time
            new ReplacementRule(
                    Pattern.compile("\\bts\\b"),
                    "__time"
            ),
            // 规则2: 字段group转为"group"
            new ReplacementRule(
                    Pattern.compile("\\bgroup\\b"),
                    "\"group\""
            ),
            new ReplacementRule(
                    Pattern.compile("\\bmethod\\b"),
                    "\"method\""
            ),new ReplacementRule(
                    Pattern.compile("\\bmaxOrNull\\b"),
                    "\"max\""
            ),
            new ReplacementRule(
                    Pattern.compile("\\bLIMIT\\s+0\\s*,\\s*(\\d+)"),
                    "LIMIT $1"
            ),
            new ReplacementRule(
                Pattern.compile("avg\\((\\w+)\\)"), "sum($1)/count(*)"
            ),
            new ReplacementRule(
                Pattern.compile("FROM\\s+pmone\\w+\\.(\\w+)\\b"),
                "FROM $1"
            ),

            new ReplacementRule(
                Pattern.compile("\\bis_model\\s*=\\s*true\\b"),
                "is_model = '1'"
            ),
            new ReplacementRule(
                Pattern.compile("\\bis_model\\s*=\\s*false\\b"),
                "is_model = '0'"
            ),
            // 规则3: 时间函数toDateTime64转换（使用函数式处理）
            new ReplacementRule(
                Pattern.compile("toDateTime64\\(\\s*([\\d]+\\.[\\d]*)\\s*,\\s*3\\s*\\)"),
                    new Function<Matcher, String>() {
                        @Override
                        public String apply(Matcher matcher) {
                            String[] parts = matcher.group(1).split("\\.");
                            long seconds = Long.parseLong(parts[0]);
                          if (parts.length > 1) {
                            int millis = Integer.parseInt(parts[1].substring(0, 3));
                            return String.format("MILLIS_TO_TIMESTAMP(%d * 1000 + %d)", seconds, millis);
                          } else {
                            return String.format("MILLIS_TO_TIMESTAMP(%d * 1000)", seconds);
                          }
                        }
                    }
            ),

            // 规则4: 聚合函数转换（sum(dur) -> sum(dur_sum)）
            new ReplacementRule(
                Pattern.compile("(sum|min|max)\\(([\\w]+)\\)"),
                    new Function<Matcher, String>() {
                        @Override
                        public String apply(Matcher matcher) {
                            String func = matcher.group(1);
                            String column = matcher.group(2);
                            String suffix = "";
                            switch(func) {
                                case "sum": suffix = "sum"; break;
                                case "avg": suffix = "avg"; break;
                                case "min": suffix = "min"; break;
                                case "max": suffix = "max"; break;
                            }
                            return String.format("%s(%s_%s)", func, column, suffix);
                        }
                    }
            ),
            // 规则5: 时间窗口函数转换
            new ReplacementRule(
                Pattern.compile("toStartOfInterval\\s*\\(\\s*__time,\\s*toIntervalDay\\((\\d+)\\),\\s*'([\\w/]+)"
                                + "'\\s*\\)"),
                "TIME_FLOOR(__time, 'P$1D')"
            ),
            new ReplacementRule(
                Pattern.compile("toStartOfInterval\\s*\\(\\s*__time,\\s*toIntervalDay\\((\\d+)\\)\\s*\\)"),
                "TIME_FLOOR(__time, 'P$1D')"
            ),
            new ReplacementRule(
                Pattern.compile("toStartOfInterval\\s*\\(\\s*__time,\\s*toIntervalHour\\((\\d+)\\)\\s*\\)"),
                "TIME_FLOOR(__time, 'PT$1H')"
            ),
            new ReplacementRule(
                Pattern.compile(
                    "toStartOfInterval\\s*\\(\\s*__time,\\s*INTERVAL\\s*(\\d+)\\s*(day|hour|minute)\\s*,\\s*'"
                    + "\\S+'\\)"),
                new Function<Matcher, String>()
                {
                  @Override
                  public String apply(Matcher matcher)
                  {
                    String interval = "P";
                    switch (matcher.group(2)) {
                      case "day":
                        interval += matcher.group(1) + "D";
                        break;
                      case "hour":
                        interval += "T" + matcher.group(1) + "H";
                        break;
                      case "minute":
                        interval += "T" + matcher.group(1) + "M";
                        break;
                    }
                    return String.format("TIME_FLOOR(__time, '%s')", interval);
                  }
                }
            ),
            new ReplacementRule(
                Pattern.compile(
                    "toStartOfInterval\\s*\\(\\s*__time\\s*,\\s*INTERVAL\\s*(\\d+)\\s*(day|hour|minute)\\s*\\)"),
                    new Function<Matcher, String>() {
                        @Override
                        public String apply(Matcher matcher) {
                          String interval = "P";
                            switch(matcher.group(2)) {
                              case "day":
                                interval += matcher.group(1) + "D";
                                break;
                              case "hour":
                                interval += "T" + matcher.group(1) + "H";
                                break;
                              case "minute":
                                interval += "T" + matcher.group(1) + "M";
                                break;
                            }
                            return String.format("TIME_FLOOR(__time, '%s')", interval);
                        }
                    }
            ),
            // 规则6: count()-->count(1)
            new ReplacementRule(
                    Pattern.compile("count\\(\\)"),
                    new Function<Matcher, String>() {
                        @Override
                        public String apply(Matcher matcher) {
                          return "count(1)";
                        }
                    }
            )
    );

    // 替换规则封装类
    static class ReplacementRule {
        Pattern pattern;
        String replacement;
        Function<Matcher, String> dynamicReplacer;

        // 简单替换构造器
        ReplacementRule(Pattern pattern, String replacement) {
            this.pattern = pattern;
            this.replacement = replacement;
        }

        // 动态替换构造器
        ReplacementRule(Pattern pattern, Function<Matcher, String> dynamicReplacer) {
            this.pattern = pattern;
            this.dynamicReplacer = dynamicReplacer;
        }
    }

    public static String convertClickhouseToDruid(String clickhouseSQL) {
        String druidSQL = clickhouseSQL;

        for (ReplacementRule rule : REPLACE_RULES) {
            Matcher matcher = rule.pattern.matcher(druidSQL);
            StringBuffer sb = new StringBuffer();

            while (matcher.find()) {
                String replacement;
                if (rule.dynamicReplacer != null) {
                    // 动态替换逻辑
                    replacement = rule.dynamicReplacer.apply(matcher);
                } else {
                    // 简单替换逻辑
                    replacement = matcher.groupCount() >= 1 ?
                            matcher.group(0).replaceAll(rule.pattern.pattern(), rule.replacement) :
                            rule.replacement;
                }
                matcher.appendReplacement(sb, replacement);
            }
            matcher.appendTail(sb);
            druidSQL = sb.toString();
        }
      //group by字段名替换为所在位置
      druidSQL = replaceGroupByWithPosition(druidSQL);
        return druidSQL;
    }

    public static void main(String[] args) {
//        String inputSQL = """
//            SELECT count(1) AS total_RESP, toStartOfInterval(__time, toIntervalDay(7), 'Asia/Shanghai') AS ts_RESP FROM dwm_request WHERE (appid = 'cbpt-y') AND (__time <= MILLIS_TO_TIMESTAMP(1684400279 * 1000 + 999)) AND (__time >= MILLIS_TO_TIMESTAMP(1683728580 * 1000)) GROUP BY 2 ORDER BY ts_RESP ASC
//            """;
      String inputSQL = "SELECT count(1) AS total_RESP, toStartOfInterval(__time, toIntervalHour(1)) AS ts_RESP FROM dwm_request WHERE (appid = 'cbpt-y') AND (__time <= MILLIS_TO_TIMESTAMP(1684400279 * 1000 + 999)) AND (__time >= MILLIS_TO_TIMESTAMP(1684339200 * 1000)) GROUP BY 2 ORDER BY ts_RESP ASC";
      String outputSQL = convertClickhouseToDruid(inputSQL);

      System.out.println("Converted Druid SQL:\n" + outputSQL);
    }
}
