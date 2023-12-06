package io.tidb.bigdata.jdbc.core;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Supplier;

public class TiDBJdbcRecordCursor implements AutoCloseable {

  private final Statement statement;
  private final LinkedList<Supplier<ResultSet>> resultSets;
  private final List<String> columns;

  private ResultSet resultSet;

  public TiDBJdbcRecordCursor(
      Statement statement, List<Supplier<ResultSet>> resultSets, List<String> columns) {
    this.statement = statement;
    this.resultSets = new LinkedList<>(resultSets);
    this.columns = columns;
  }

  public Object getObject(int index) throws SQLException {
    return resultSet.getObject(index + 1);
  }

  public Object getObject(String name) throws SQLException {
    return resultSet.getObject(name);
  }

  private boolean nextResultSet() throws SQLException {
    while (!resultSets.isEmpty()) {
      if (resultSet != null) {
        resultSet.close();
      }
      this.resultSet = resultSets.removeFirst().get();
      if (resultSet.next()) {
        return true;
      }
    }
    return false;
  }

  public boolean advanceNextPosition() throws SQLException {
    if (resultSet == null) {
      return nextResultSet();
    }
    if (!resultSet.next()) {
      return nextResultSet();
    }
    return true;
  }

  public int fieldCount() {
    return columns.size();
  }

  @Override
  public void close() {
    try {
      resultSet.close();
    } catch (Exception e) {
      // ignore
    }
    try {
      statement.close();
    } catch (Exception e) {
      // ignore
    }
  }
}
