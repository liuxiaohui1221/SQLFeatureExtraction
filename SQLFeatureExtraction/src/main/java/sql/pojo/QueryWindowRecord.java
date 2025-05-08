package sql.pojo;

import java.util.List;
import java.util.Objects;

public class QueryWindowRecord
{
  List<String> sqls;
  String windowVector;
  long windowTimeSec;

  public QueryWindowRecord(List<String> sqls, String windowVector, long windowTimeSec)
  {
    this.sqls = sqls;
    this.windowVector = windowVector;
    this.windowTimeSec = windowTimeSec;
  }

  public List<String> getSqls()
  {
    return sqls;
  }

  public String getWindowVector()
  {
    return windowVector;
  }

  public long getWindowTimeSec()
  {
    return windowTimeSec;
  }

  @Override
  public boolean equals(Object o)
  {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    QueryWindowRecord that = (QueryWindowRecord) o;
    return windowTimeSec == that.windowTimeSec && Objects.equals(sqls, that.sqls) && Objects.equals(
        windowVector,
        that.windowVector
    );
  }

  @Override
  public int hashCode()
  {
    return Objects.hash(sqls, windowVector, windowTimeSec);
  }
}
