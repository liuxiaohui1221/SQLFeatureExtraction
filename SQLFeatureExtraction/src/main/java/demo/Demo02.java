package demo;

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

    public static void main(String[] args) {
        String sql = "SELECT * FROM iceberg.mydatabase.my_table\n WHERE id = 1";
        String tableName = "mydatabase.my_table";

        boolean containsTable = containsTable(sql, tableName);
        System.out.println("Does the SQL string contain the table? " + containsTable);
    }
}
