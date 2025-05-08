package sql.tools;

import java.util.ArrayList;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class SqlGroupByReplacer
{

  public static String replaceGroupByWithPosition(String sql)
  {
    // 提取SELECT字段列表和别名
    List<String> selectAliases = extractSelectAliases(sql);
    if (selectAliases.isEmpty()) {
      return sql;
    }

    // 提取并替换GROUP BY子句
    Pattern groupByPattern = Pattern.compile(
        "(?i)\\bGROUP BY\\s+([^;]+?)(?=\\s*(?:ORDER BY|LIMIT|HAVING|$))",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    );

    Matcher matcher = groupByPattern.matcher(sql);
    StringBuffer sb = new StringBuffer();

    while (matcher.find()) {
      String groupByFields = matcher.group(1).trim();
      String[] fields = groupByFields.split(",");
      List<String> replacedFields = new ArrayList<>();

      for (String field : fields) {
        field = field.trim();
        int position = selectAliases.indexOf(field) + 1;
        replacedFields.add(position > 0 ? String.valueOf(position) : field);
      }

      String replacement = "GROUP BY " + String.join(", ", replacedFields);
      matcher.appendReplacement(sb, replacement.replace("$", "\\$"));
    }
    matcher.appendTail(sb);

    return sb.toString();
  }

  private static List<String> extractSelectAliases(String sql)
  {
    List<String> aliases = new ArrayList<>();
    Matcher selectMatcher = Pattern.compile(
        "SELECT(.*?)FROM",
        Pattern.CASE_INSENSITIVE | Pattern.DOTALL
    ).matcher(sql);

    if (!selectMatcher.find()) {
      return aliases;
    }

    String fieldsPart = selectMatcher.group(1).trim();
    for (String field : splitFields(fieldsPart)) {
      Matcher aliasMatcher = Pattern.compile(
          "\\bAS\\s+\"?([\\w_]+)\"?$",
          Pattern.CASE_INSENSITIVE
      ).matcher(field.trim());

      if (aliasMatcher.find()) {
        aliases.add(aliasMatcher.group(1));
      } /*else {
        aliases.add(field.replaceAll(".*\\s", "").replaceAll("\"", ""));
      }*/
    }
    return aliases;
  }

  private static String[] splitFields(String fieldsPart)
  {
    List<String> fields = new ArrayList<>();
    int depth = 0;
    StringBuilder sb = new StringBuilder();

    for (char c : fieldsPart.toCharArray()) {
      if (c == '(') {
        depth++;
      }
      if (c == ')') {
        depth--;
      }
      if (c == ',' && depth == 0) {
        fields.add(sb.toString().trim());
        sb.setLength(0);
      } else {
        sb.append(c);
      }
    }
    fields.add(sb.toString().trim());
    return fields.toArray(new String[0]);
  }

  public static void main(String[] args)
  {
    String testSQL = "SELECT avg(dur) AS dur_RESP, count() AS total_RESP, method, path "
                     + "FROM pmone_0d5de51f17.dwm_request WHERE (appid = 'cbpt-y') AND (group = '051795f61ee3f1db110d6b94205f903f95e25614') "
                     + "AND (ts <= toDateTime64(1684400699.999, 3)) AND (ts >= toDateTime64(1684339200., 3)) GROUP BY method, path LIMIT 0, 1";
    System.out.println(replaceGroupByWithPosition(testSQL));
  }
}

