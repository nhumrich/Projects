import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * @autor: Nick Humrich
 * @date: 7/23/13
 */
public class Transaction {
  private Connection connection;
  PreparedStatement stmt;


  Transaction() {
    connection = getNewConnection();
  }

  public static Transaction start() {
    return new Transaction();
  }

  private Connection getNewConnection() {
    return DataStoreFactory.getStore().getConnection();
  }

  private void commit() throws SQLException {
    stmt.execute();
    connection.commit();
    stmt.close();
    connection.close();
  }

  private void rollback() throws SQLException {
    connection.rollback();
    stmt.close();
    connection.close();
  }

  public void newStatement(String message) throws SQLException {
    if (connection.isClosed()) {
      connection = getNewConnection();
    }

    try {
      if (stmt != null && !stmt.isClosed()) {
        stmt.close();
      }
      stmt = connection.prepareStatement(message);
    } catch (SQLException e) {
      connection.close();
      throw new SQLException("Could not create prepared statement", e);
    }
  }

  public void setString(int column, String value) throws SQLException {
    if (value == null) value = "";
    try {
    stmt.setString(column, value);
    } catch (Exception e) {
      rollback();
      throw new SQLException("Could not setString with column " + column + " and value " + value, e);
    }
  }

  public void setInt(int column, int value) throws SQLException {
    try {
      stmt.setInt(column, value);
    } catch (SQLException e) {
      rollback();
      throw new SQLException("Could not setString with column " + column + " and value " + value, e);
    }
  }

  public void send() throws SQLException {
    try {
      commit();
    } catch (SQLException e) {
      rollback();
      throw new SQLException("Error with Transaction: " + e.getMessage(), e);
    }
  }

  public int getIdOfLastInsert() throws SQLException {
    int id = 0;
    newStatement("SELECT LAST_INSERT_ID();");
    ResultSet resultSet = sendQuery();
    if (resultSet.next()) {
     id = resultSet.getInt(1);
    }
    closeResultSet(resultSet);
    return id;
  }

  public ResultSet sendQuery() throws SQLException {
    ResultSet resultSet = null;
    try {
      resultSet = stmt.executeQuery();
    }
    catch (SQLException e) {
      rollback();
      throw new SQLException("Could not retrieve Result set", e);
    }
    return resultSet;
  }

  public void closeResultSet(ResultSet resultSet) throws SQLException {
    stmt.close();
    resultSet.close();
    connection.close();
  }
}
