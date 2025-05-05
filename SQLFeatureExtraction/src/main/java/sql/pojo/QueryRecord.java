package sql.pojo;

import java.time.LocalDateTime;

public class QueryRecord
{
  private final long eventTimeSec;
  LocalDateTime eventTime;
  long duration;
  String sql;
  String table;
  long cost;

  public QueryRecord(LocalDateTime eventTime, long eventTimeSec, long duration, String sql, String table)
  {
    this.eventTime = eventTime;
    this.eventTimeSec = eventTimeSec;
    this.duration = duration;
    this.sql = sql;
    this.table = table;
    this.cost = duration;
  }

  public LocalDateTime getEventTime()
  {
    return eventTime;
  }

  public long getEventTimeSec()
  {
    return eventTimeSec;
  }

  public long getDuration()
  {
    return duration;
  }

  public long getCost()
  {
    return cost;
  }

  public void sumDuration(long duration)
  {
    this.cost += duration;
  }

  public String getTable()
  {
    return this.table;
  }

  public String getSql()
  {
    return this.sql;
  }
}

