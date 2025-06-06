import sql.encoder.APMWindowFragmentIntent;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

public class APMWindowFragmentIntentTest
{
  public static void main(String[] args) throws IOException
  {

    // Test case 1: Normal case with multiple tables and columns
    // read from file
    List<String> sqlList1 = reorganizeSqlList(Path.of(
        "/home/xhh/db_workspace/SQLFeatureExtraction/SQLFeatureExtraction/src/test/java/com/clickhouse/queryintent_test.txt"));
    APMWindowFragmentIntent.reorganizeSqlList(sqlList1, 300, "train");
  }

  private static List<String> reorganizeSqlList(Path file) throws IOException
  {
    List<String> sqlList = new ArrayList<>();
    try (BufferedReader br = new BufferedReader(new FileReader(file.toFile()))) {
      String line;
      while ((line = br.readLine()) != null) {
        sqlList.add(line);
      }
    }
    return sqlList;
  }
}
