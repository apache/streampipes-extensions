/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.streampipes.sinks.databases.jvm.postgresql;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.logging.api.Logger;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.schema.EventProperty;
import org.apache.streampipes.model.schema.EventPropertyNested;
import org.apache.streampipes.model.schema.EventPropertyPrimitive;
import org.apache.streampipes.sinks.databases.jvm.jdbcclient.JdbcClient;
import org.apache.streampipes.vocabulary.XSD;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class PostgresJdbcClient extends JdbcClient {

  protected String schemaName;
  protected boolean schemaExists = false;
  protected boolean isToDropTable = false;

//  /**
//   * A wrapper class for all supported SQL data types (INT, BIGINT, FLOAT, DOUBLE, VARCHAR(255)).
//   * If no matching type is found, it is interpreted as a String (VARCHAR(255))
//   */
  protected enum SqlAttribute {
  INTEGER("INT"), LONG("BIGINT"), FLOAT("FLOAT"), DOUBLE("DOUBLE PRECISION"), STRING("TEXT"), BOOLEAN("BOOLEAN");
  private final String sqlName;

  SqlAttribute(String s) {
    sqlName = s;
  }
}
//
//    /**
//     * Tries to identify the data type of the object {@code o}. In case it is not supported, it is
//     * interpreted as a String (VARCHAR(255))
//     *
//     * @param o The object which should be identified
//     * @return An {@link SqlAttribute} of the identified type
//     */
//    public static SqlAttribute getFromObject(final Object o) {
//      SqlAttribute r;
//      if (o instanceof Integer) {
//        r = SqlAttribute.INTEGER;
//      } else if (o instanceof Long) {
//        r = SqlAttribute.LONG;
//      } else if (o instanceof Float) {
//        r = SqlAttribute.FLOAT;
//      } else if (o instanceof Double) {
//        r = SqlAttribute.DOUBLE;
//      } else if (o instanceof Boolean) {
//        r = SqlAttribute.BOOLEAN;
//      } else {
//        r = SqlAttribute.STRING;
//      }
//      return r;
//    }
//
//    public static SqlAttribute getFromUri(final String s) {
//      SqlAttribute r;
//      if (s.equals(XSD._integer.toString())) {
//        r = SqlAttribute.INTEGER;
//      } else if (s.equals(XSD._long.toString())) {
//        r = SqlAttribute.LONG;
//      } else if (s.equals(XSD._float.toString())) {
//        r = SqlAttribute.FLOAT;
//      } else if (s.equals(XSD._double.toString())) {
//        r = SqlAttribute.DOUBLE;
//      } else if (s.equals(XSD._boolean.toString())) {
//        r = SqlAttribute.BOOLEAN;
//      } else {
//        r = SqlAttribute.STRING;
//      }
//      return r;
//    }
//
//    /**
//     * Sets the value in the prepardStatement {@code ps}
//     *
//     * @param p     The needed info about the parameter (index and type)
//     * @param value The value of the object, which should be filled in the
//     * @param ps    The prepared statement, which will be filled
//     * @throws SpRuntimeException When the data type in {@code p} is unknown
//     * @throws SQLException       When the setters of the statement throw an
//     *                            exception (e.g. {@code setInt()})
//     */
//    public static void setValue(Parameterinfo p, Object value, PreparedStatement ps)
//        throws SQLException, SpRuntimeException {
//      switch (p.type) {
//        case INTEGER:
//          ps.setInt(p.index, (Integer) value);
//          break;
//        case LONG:
//          ps.setLong(p.index, (Long) value);
//          break;
//        case FLOAT:
//          ps.setFloat(p.index, (Float) value);
//          break;
//        case DOUBLE:
//          ps.setDouble(p.index, (Double) value);
//          break;
//        case BOOLEAN:
//          ps.setBoolean(p.index, (Boolean) value);
//          break;
//        case STRING:
//          ps.setString(p.index, value.toString());
//          break;
//        default:
//          throw new SpRuntimeException("Unknown SQL datatype");
//      }
//    }
//
//    @Override
//    public String toString() {
//      return sqlName;
//    }
//  }

//  /**
//   * Contains all information needed to "fill" a prepared statement (index and the data type)
//   */
//  protected static class Parameterinfo {
//    private int index;
//    private SqlAttribute type;
//
//    public Parameterinfo(final int index, final SqlAttribute type) {
//      this.index = index;
//      this.type = type;
//    }
//  }

  public PostgresJdbcClient() {
    super();
  }

  protected void initializeJdbc(List<EventProperty> eventProperties,
                                String host, Integer port,
                                String databaseName,
                                String tableName,
                                String user,
                                String password,
                                String allowedRegEx,
                                String driver,
                                String urlName,
                                Logger logger,
                                String schemaName,
                                boolean isToDropTable) throws SpRuntimeException {
    super.initializeJdbc(
        eventProperties,
        host,
        port,
        databaseName,
        tableName,
        user,
        password,
        allowedRegEx,
        driver,
        urlName,
        logger);
    this.schemaName = schemaName;
    this.isToDropTable = isToDropTable;

    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      throw new SpRuntimeException("Driver '" + driver + "' not found.");
    }

    connect();
    ensureDatabaseExists();
    ensureSchemaExists();
    ensureTableExists();
  }


  /**
   * If this method returns successfully a schema with the name in {@link PostgresJdbcClient#schemaName} exists in the database
   * with the given database name exists on the server, specified by the url.
   *
   * @throws SpRuntimeException If the table does not exist and could not be created
   */
  protected void ensureSchemaExists() throws SpRuntimeException {
    try {
      // Database should exist by now so we can establish a connection
      c = DriverManager.getConnection(url + databaseName, user, password);
      st = c.createStatement();
      ResultSet rs = c.getMetaData().getSchemas();

      boolean isItExisting = false;
      while (rs.next()) {
        String schema = rs.getString("TABLE_SCHEM");
        if (schema.toLowerCase().equals(schemaName.toLowerCase())) {
          isItExisting = true;
        }
      }
      if (!isItExisting) {
        createSchema();
      }

      schemaExists = true;
      rs.close();
    } catch (SQLException e) {
      closeAll();
      throw new SpRuntimeException(e.getMessage());
    }
  }


  /**
   * If this method returns successfully a table with the name in {@link PostgresJdbcClient#tableName} exists in the database
   * with the given database name exists on the server, specified by the url.
   *
   * @throws SpRuntimeException If the table does not exist and could not be created
   */
  @Override
  protected void ensureTableExists() throws SpRuntimeException {
    try {
      // Database should exist by now so we can establish a connection
      c = DriverManager.getConnection(url + databaseName, user, password);
      st = c.createStatement();
      ResultSet rs = c.getMetaData().getTables(null, null, tableName, null);
      while (rs.next()) {
        // same table names can exists in different schmemas
        if (rs.getString("TABLE_SCHEM").toLowerCase().equals(schemaName.toLowerCase())) {
          if (isToDropTable) {
            createTable();
          }
          validateTable();
        } else {
          createTable();
        }
      }
      tableExists = true;
      rs.close();
    } catch (SQLException e) {
      closeAll();
      throw new SpRuntimeException(e.getMessage());
    }
  }



  /**
   * Prepares a statement for the insertion of values or the
   *
   * @param event The event which should be saved to the Postgres table
   * @throws SpRuntimeException When there was an error in the saving process
   */
  @Override
  protected void save(final Event event) throws SpRuntimeException {
    //TODO: Add batch support (https://stackoverflow.com/questions/3784197/efficient-way-to-do-batch-inserts-with-jdbc)
    checkConnected();
    Map<String, Object> eventMap = event.getRaw();
    if (event == null) {
      throw new SpRuntimeException("event is null");
    }

    if (!schemaExists) {
      // Creates the schema
      createSchema();
      schemaExists = true;
    }

    if (!tableExists) {
      // Creates the table
      createTable();
      tableExists = true;
    }
    try {
      executePreparedStatement(eventMap);
    } catch (SQLException e) {
      if (e.getSQLState().substring(0, 2).equals("42")) {
        // If the table does not exists (because it got deleted or something, will cause the error
        // code "42") we will try to create a new one. Otherwise we do not handle the exception.
        logger.warn("Table '" + tableName + "' was unexpectedly not found and gets recreated.");
        tableExists = false;
        createTable();
        tableExists = true;

        try {
          executePreparedStatement(eventMap);
        } catch (SQLException e1) {
          throw new SpRuntimeException(e1.getMessage());
        }
      } else {
        throw new SpRuntimeException(e.getMessage());
      }
    }
  }


  /**
   * Creates a schema with the name {@link PostgresJdbcClient#schemaName}
   *
   * @throws SpRuntimeException If the {@link PostgresJdbcClient#schemaName}  is not allowed, if executeUpdate throws an SQLException
   */
  protected void createSchema() throws SpRuntimeException {
    checkConnected();
    checkRegEx(tableName, "Tablename");

    StringBuilder statement = new StringBuilder("CREATE SCHEMA ");
    statement.append(schemaName).append(";");
    try {
      st.executeUpdate(statement.toString());
    } catch (SQLException e) {
      throw new SpRuntimeException(e.getMessage());
    }
  }

  @Override
  protected void createTable() throws SpRuntimeException {
    checkConnected();
    checkRegEx(tableName, "Tablename");

    if (isToDropTable) {
      StringBuilder statement = new StringBuilder("DROP TABLE IF EXISTS ");
      statement.append(schemaName);
      statement.append(".");
      statement.append(tableName);
      statement.append(";");

      try {
        st.executeUpdate(statement.toString());
      } catch (SQLException e) {
        throw new SpRuntimeException(e.getMessage());
      }
    }

    StringBuilder statement = new StringBuilder("CREATE TABLE ");
    statement.append(schemaName);
    statement.append(".");
    statement.append(tableName).append(" ( ");
    statement.append(extractEventProperties(eventProperties)).append(" );");

    try {
      st.executeUpdate(statement.toString());
    } catch (SQLException e) {
      e.getErrorCode();
      if (e.getSQLState().equals("42P07")) {
        throw new SpRuntimeException("Table already exists. Change option \"DROP TABLE\" to prevent this error. Error Message: " + e.getMessage());
      } else {
        throw new SpRuntimeException("Something went wrong during table creation with error message: " + e.getMessage());
      }
    }
  }
}
