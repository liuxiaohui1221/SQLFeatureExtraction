package sql.tools;

import java.util.Map;
import java.util.regex.Pattern;

import java.util.*;
import java.util.regex.*;
import java.util.function.Function;

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

            // 规则3: 时间函数toDateTime64转换（使用函数式处理）
            new ReplacementRule(
                    Pattern.compile("toDateTime64\\(\\s*([\\d]+\\.[\\d]+)\\s*,\\s*3\\s*\\)"),
                    new Function<Matcher, String>() {
                        @Override
                        public String apply(Matcher matcher) {
                            String[] parts = matcher.group(1).split("\\.");
                            long seconds = Long.parseLong(parts[0]);
                            int millis = Integer.parseInt(parts[1].substring(0, 3));
                            return String.format("MILLIS_TO_TIMESTAMP(%d * 1000 + %d)", seconds, millis);
                        }
                    }
            ),

            // 规则4: 聚合函数转换（sum(dur) -> sum(dur_sum)）
            new ReplacementRule(
                    Pattern.compile("(sum|avg|min|max|count)\\(([\\w]+)\\)"),
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
                    Pattern.compile("toStartOfInterval\\s*\\(\\s*__time\\s*,\\s*INTERVAL\\s*(\\d+)\\s*(day|hour|minute)\\s*,\\s*'([\\w/]+)'\\s*\\)"),
                    new Function<Matcher, String>() {
                        @Override
                        public String apply(Matcher matcher) {
                            String interval = "P" + matcher.group(1);
                            switch(matcher.group(2)) {
                                case "day": interval += "D"; break;
                                case "hour": interval += "H"; break;
                                case "minute": interval += "M"; break;
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

        return druidSQL;
    }

    public static void main(String[] args) {
        String inputSQL = """
                SELECT (count() - sum(frustrated) - sum(tolerated) * 0.5)/count() AS apdex_RESP, avg(dur) AS dur_RESP, sum(err) AS err_RESP, sum(err_4xx) AS err_4xx_RESP, sum(err_5xx) AS err_5xx_RESP, sum(err)/count() AS err_rate_RESP, sum(fail) AS fail_RESP, sum(fail)/count() AS fail_rate_RESP, sum(frustrated) AS frustrated_RESP, sum(frustrated)/count() AS frustrated_rate_RESP, sum(httperr) AS httperr_RESP, maxOrNull(ts) AS last_time_RESP, sum(neterr) AS neterr_RESP, sum(tolerated) AS slow_RESP, sum(tolerated)/count() AS slow_rate_RESP, count() AS total_RESP, method, path FROM dwm_request_cluster 
                WHERE (appid = 'pro-api-g10-xingyun') AND (appsysid = 'bda14c5a-82cd-4087-8499-096b29b541c1') AND (group = 'E01090DB3A6CC1BA') AND (ts <= toDateTime64(1684404959.999, 3)) AND (ts >= toDateTime64(1683800100.000, 3)) GROUP BY method, path ORDER BY last_time_RESP DESC LIMIT 0, 1
                """;

        String outputSQL = convertClickhouseToDruid(inputSQL);
        System.out.println("Converted Druid SQL:\n" + outputSQL);
    }
}
