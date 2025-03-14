package demo;

import java.text.SimpleDateFormat;
import java.util.Date;

public class Demo02 {
    public static boolean containsTable(String sql, String tableName) {
        // 转义表名中的特殊字符
        tableName = tableName.replace("$", "\\$")
                .replace(".", "\\.")
                .replace("*", "\\*")
                .replace("+", "\\+")
                .replace("?", "\\?");
        // 构建正则表达式，考虑数据库名前缀和反引号
        String pattern = "(?i)\\b([a-zA-Z0-9_]+\\.)?`?" + tableName + "`?\\b";
        // 使用正则表达式匹配
        return sql.matches(".*" + pattern + ".*");
    }

//    public static void main(String[] args) {
//        String sql = "SELECT * FROM iceberg.mydatabase.my_table\n WHERE id = 1";
//        String tableName = "mydatabase.my_table";
//
//        boolean containsTable = containsTable(sql, tableName);
//        System.out.println("Does the SQL string contain the table? " + containsTable);
//    }
public static void main(String[] args) {
    String timeString = "2023/5/18 18:22";
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy/M/d HH:mm");
    try {
        Date date = sdf.parse(timeString);
        long timestamp = date.getTime();
        System.out.println("Timestamp: " + timestamp);
    } catch (java.text.ParseException e) {
        throw new RuntimeException(e);
    }
    String time="1683799560.000";
    System.out.println(time.substring(0,10));
}
}
