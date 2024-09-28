package reader;

public class StringCleaner {
    public static void main(String[] args) {
        String dirtyString = "\n" +
                "clean columnsStr:\\n  ts DateTime64(3),\\n  user_id String,\\n  appid LowCardinality(String),\\n  appsysid LowCardinality(String),\\n  request_group Nullable(String),\\n  request_status Nullable(UInt32),\\n  page_group Nullable(String),\\n  crash_type Nullable(String),\\n  freeze_desc Nullable(String),\\n  exception_class Nullable(String),\\n  exception_url Nullable(String),\\n  exception_name Nullable(String),\\n  sql_group Nullable(String),\\n  sql_status Nullable(UInt32),\\n  ip Nullable(String),\\n  province LowCardinality(Nullable(String)),\\n  city LowCardinality(Nullable(String)),\\n  crash UInt64,\\n  js_err UInt64,\\n  exception UInt64,\\n  freeze UInt64,\\n  request_count UInt64,\\n  request_dur UInt64,\\n  fast UInt64,\\n  render UInt64,\\n  first UInt64,\\n  tolerated UInt64,\\n  frustrated UInt64,\\n  fail UInt64,\\n  err UInt64,\\n  neterr UInt64,\\n  httperr UInt64,\\n  page_count UInt64,\\n  ajax UInt64,\\n  web UInt64,\\n  sql_count UInt64,\\n  sql_tolerated UInt64,\\n  sql_frustrated UInt64\\n";
        String cleanString = cleanString(dirtyString);
        System.out.println("Cleaned String: " + cleanString);
    }

    public static String cleanString(String input) {
        input = input.replaceAll("`", "");
        input = input.replaceAll("`group`", "group1");
        input = input.replaceAll("#", "");
        input = input.replaceAll(";", "");
        input = input.replaceAll("\\\\n", "");
        input = input.replaceAll("\\\\", "");
        // 使用正则表达式替换所有换行符及其前后的空格
        return input.replaceAll("\\s*\\n\\s*", "");
    }

    public static String correctQuery(String query) {
        return query.replaceAll("_cluster", "");
    }
}
