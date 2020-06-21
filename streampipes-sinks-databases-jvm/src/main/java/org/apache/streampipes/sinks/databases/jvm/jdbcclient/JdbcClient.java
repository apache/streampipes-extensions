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

package org.apache.streampipes.sinks.databases.jvm.jdbcclient;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.logging.api.Logger;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.schema.EventProperty;
import org.apache.streampipes.model.schema.EventPropertyNested;
import org.apache.streampipes.model.schema.EventPropertyPrimitive;
import org.apache.streampipes.vocabulary.XSD;

import java.sql.*;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class JdbcClient {
  protected String allowedRegEx;
  protected String url;

  protected String databaseName;
  protected String tableName;
  protected String user;
  protected String password;
  protected String urlName;
  protected String host;
  protected Integer port;

  protected boolean tableExists = false;

  protected Logger logger;

  protected Connection c = null;
  protected Statement st = null;
  protected PreparedStatement ps = null;

  /**
   * The list of properties extracted from the graph
   */
  protected List<EventProperty> eventProperties;
  /**
   * The parameters in the prepared statement {@code ps} together with their index and data type
   */
  protected HashMap<String, Parameterinfo> parameters = new HashMap<>();

  /**
   * A wrapper class for all supported SQL data types (INT, BIGINT, FLOAT, DOUBLE, VARCHAR(255)).
   * If no matching type is found, it is interpreted as a String (VARCHAR(255))
   */


  public JdbcClient() {
  }

  protected void initializeJdbc(List<EventProperty> eventProperties,
                                String host,
                                Integer port,
                                String databaseName,
                                String tableName,
                                String user,
                                String password,
                                String allowedRegEx,
                                String driver,
                                String urlName,
                                Logger logger) throws SpRuntimeException {
    this.eventProperties = eventProperties;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.user = user;
    this.password = password;
    this.allowedRegEx = allowedRegEx;
    this.logger = logger;
    // needed for fallback
    this.urlName = urlName;
    this.host = host;
    this.port = port;
    this.url = "jdbc:" + urlName + "://" + host + ":" + port + "/" + databaseName;
    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      throw new SpRuntimeException("Driver '" + driver + "' not found.");
    }

    connect();
    ensureTableExists();
  }

  /**
   * Connects to the HadoopFileSystem Server and initilizes {@link JdbcClient#c} and
   * {@link JdbcClient#st}
   *
   * @throws SpRuntimeException When the connection could not be established (because of a
   *                            wrong identification, missing database etc.)
   */
  protected void connect() throws SpRuntimeException {
    try {
      c = DriverManager.getConnection(url, user, password);
    } catch (SQLException e) {
      // host or port is wrong -- Class 08  Connection Exception
      if (e.getSQLState().substring(0, 2).equals("08")) {
        throw new SpRuntimeException("Connection can't be established. Check host or port setting: \n" + e.getMessage());
      }
      // username or password is wrong -- Class 28  Invalid Authorization Specification
      else if (e.getSQLState().substring(0, 2).equals("28")) {
        throw new SpRuntimeException("User authentication error. Check username or password: \n" + e.getMessage());
      } else if (e.getSQLState().substring(0, 2).equals("3D")){
        try {
          String urlFallback= "jdbc:" + urlName + "://" + host + ":" + port + "/";
          c = DriverManager.getConnection(urlFallback, user, password);
          ensureDatabaseExists();
        } catch (SQLException e2) {
          closeAll();
          throw new SpRuntimeException("Neither chosen database name nor username database exists to connect to. Please check settings  : \n" + e.getMessage());
        }
      } else {
        throw new SpRuntimeException("Could not establish a connection with the server: " + e.getMessage());
      }
    }
  }

  /**
   * If this method returns successfully a database with the given name exists on the server, specified by the url.
   *
   * @throws SpRuntimeException If the database does not exists and could not be created
   */
  protected void ensureDatabaseExists() throws SpRuntimeException {
    checkRegEx(databaseName, "databasename");

    try {
      // Checks whether the database already exists (using catalogs has not worked with postgres)
      st = c.createStatement();
      st.executeUpdate("CREATE DATABASE " + databaseName + ";");
      logger.info("Created new database '" + databaseName + "'");
    } catch (SQLException e1) {
      if (e1.getSQLState().equals("42501")){
        // insufficient_privilege
        closeAll();
        throw new SpRuntimeException("Chosen user has no rights to create a database: " + e1.getMessage());
      } else {
        closeAll();
        throw new SpRuntimeException("Error while creating database: " + e1.getMessage());
      }
    }
    closeAll();
  }

  /**
   * If this method returns successfully a table with the name in {@link JdbcClient#tableName} exists in the database
   * with the given database name exists on the server, specified by the url.
   *
   * @throws SpRuntimeException If the table does not exist and could not be created
   */
  protected void ensureTableExists() throws SpRuntimeException {
    try {
      // Database should exist by now so we can establish a connection
      c = DriverManager.getConnection(url, user, password);
      st = c.createStatement();
      ResultSet rs = c.getMetaData().getTables(null, null, tableName, null);
      if (rs.next()) {
        //validateTable();
      } else {
        createTable();
      }
      tableExists = true;

      if (!userHasPriviligesOnTable()){
        throw new SpRuntimeException("User \"" + user + "\" has no priviliges to write into table " + tableName);
      }

      rs.close();
    } catch (SQLException e) {
      closeAll();
      throw new SpRuntimeException(e.getMessage());
    }
  }

  /**
   * Clears, fills and executes the saved prepared statement {@code ps} with the data found in
   * event. To fill in the values it calls {@link JdbcClient#fillPreparedStatement(Map)}.
   *
   * @param event Data to be saved in the SQL table
   * @throws SQLException       When the statement cannot be executed
   * @throws SpRuntimeException When the table name is not allowed or it is thrown
   *                            by {@link SqlAttribute#setValue(Parameterinfo, Object, PreparedStatement)}
   */
  protected void executePreparedStatement(final Map<String, Object> event)
      throws SQLException, SpRuntimeException {
    checkConnected();
    if (ps != null) {
      ps.clearParameters();
    }
    fillPreparedStatement(event);
    ps.executeUpdate();
  }

  /**
   * Prepares a statement for the insertion of values or the
   *
   * @param event The event which should be saved to the Postgres table
   * @throws SpRuntimeException When there was an error in the saving process
   */
  protected void save(final Event event) throws SpRuntimeException {
    //TODO: Add batch support (https://stackoverflow.com/questions/3784197/efficient-way-to-do-batch-inserts-with-jdbc)
    checkConnected();
    Map<String, Object> eventMap = event.getRaw();
    if (event == null) {
      throw new SpRuntimeException("event is null");
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

  protected void fillPreparedStatement(final Map<String, Object> event)
      throws SQLException, SpRuntimeException {
    fillPreparedStatement(event, "");
  }

  /**
   * Fills a prepared statement with the actual values base on {@link JdbcClient#parameters}. If
   * {@link JdbcClient#parameters} is empty or not complete (which should only happen once in the
   * begining), it calls {@link JdbcClient#generatePreparedStatement(Map)} to generate a new one.
   *
   * @param event
   * @param pre
   * @throws SQLException
   * @throws SpRuntimeException
   */
  protected void fillPreparedStatement(final Map<String, Object> event, String pre)
      throws SQLException, SpRuntimeException {
    // checkConnected();
    //TODO: Possible error: when the event does not contain all objects of the parameter list
    for (Map.Entry<String, Object> pair : event.entrySet()) {
      String newKey = pre + pair.getKey();
      if (pair.getValue() instanceof Map) {
        // recursively extracts nested values
        fillPreparedStatement((Map<String, Object>) pair.getValue(), newKey + "_");
      } else {
        if (!parameters.containsKey(newKey)) {
          //TODO: start the for loop all over again
          generatePreparedStatement(event);
        }
        Parameterinfo p = parameters.get(newKey);
        SqlAttribute.setValue(p, pair.getValue(), ps);
      }
    }
  }

  /**
   * Initializes the variables {@link JdbcClient#parameters} and {@link JdbcClient#ps}
   * according to the parameter event.
   *
   * @param event The event which is getting analyzed
   * @throws SpRuntimeException When the tablename is not allowed
   * @throws SQLException       When the prepareStatment cannot be evaluated
   */
  protected void generatePreparedStatement(final Map<String, Object> event)
      throws SQLException, SpRuntimeException {
    // input: event
    // wanted: INSERT INTO test4321 ( randomString, randomValue ) VALUES ( ?,? );
    checkConnected();
    parameters.clear();
    StringBuilder statement1 = new StringBuilder("INSERT INTO ");
    StringBuilder statement2 = new StringBuilder("VALUES ( ");
    checkRegEx(tableName, "Tablename");
    statement1.append(tableName).append(" ( ");

    // Starts index at 1, since the parameterIndex in the PreparedStatement starts at 1 as well
    extendPreparedStatement(event, statement1, statement2, 1);

    statement1.append(" ) ");
    statement2.append(" );");
    String finalStatement = statement1.append(statement2).toString();
    ps = c.prepareStatement(finalStatement);
  }

  protected int extendPreparedStatement(final Map<String, Object> event,
                                        StringBuilder s1, StringBuilder s2, int index) throws SpRuntimeException {
    return extendPreparedStatement(event, s1, s2, index, "", "");
  }

  /**
   * @param event
   * @param s1
   * @param s2
   * @param index
   * @param preProperty
   * @param pre
   * @return
   * @throws SpRuntimeException
   */
  protected int extendPreparedStatement(final Map<String, Object> event,
                                        StringBuilder s1, StringBuilder s2, int index, String preProperty, String pre)
      throws SpRuntimeException {
    checkConnected();
    for (Map.Entry<String, Object> pair : event.entrySet()) {
      if (pair.getValue() instanceof Map) {
        index = extendPreparedStatement((Map<String, Object>) pair.getValue(), s1, s2, index,
            pair.getKey() + "_", pre);
      } else {
        checkRegEx(pair.getKey(), "Columnname");
        parameters.put(pair.getKey(), new Parameterinfo(index, SqlAttribute.getFromObject(pair.getValue())));
        s1.append(pre).append("\"").append(preProperty).append(pair.getKey().toLowerCase()).append("\"");
        s2.append(pre).append("?");
        index++;
      }
      pre = ", ";
    }
    return index;
  }

  /**
   * Creates a table with the name {@link JdbcClient#tableName} and the
   * properties {@link JdbcClient#eventProperties}. Calls
   * {@link JdbcClient#extractEventProperties(List)} internally with the
   * {@link JdbcClient#eventProperties} to extract all possible columns.
   *
   * @throws SpRuntimeException If the {@link JdbcClient#tableName}  is not allowed, if
   *                            executeUpdate throws an SQLException or if {@link JdbcClient#extractEventProperties(List)}
   *                            throws an exception
   */
  protected void createTable() throws SpRuntimeException {
    checkConnected();
    checkRegEx(tableName, "Tablename");

    StringBuilder statement = new StringBuilder("CREATE TABLE \"");
    statement.append(tableName).append("\" ( ");
    statement.append(extractEventProperties(eventProperties)).append(" );");

    try {
      st.executeUpdate(statement.toString());
    } catch (SQLException e) {
      throw new SpRuntimeException(e.getMessage());
    }
  }

  /**
   * Creates a SQL-Query with the given Properties (SQL-Injection safe). Calls
   * {@link JdbcClient#extractEventProperties(List, String)} with an empty string
   *
   * @param properties The list of properties which should be included in the query
   * @return A StringBuilder with the query which needs to be executed in order to create the table
   * @throws SpRuntimeException See {@link JdbcClient#extractEventProperties(List, String)} for details
   */
  protected StringBuilder extractEventProperties(List<EventProperty> properties)
      throws SpRuntimeException {
    return extractEventProperties(properties, "");
  }

  /**
   * Creates a SQL-Query with the given Properties (SQL-Injection safe). For nested properties it
   * recursively extracts the information. EventPropertyList are getting converted to a string (so
   * in SQL to a VARCHAR(255)). For each type it uses {@link SqlAttribute#getFromUri(String)}
   * internally to identify the SQL-type from the runtimeType.
   *
   * @param properties  The list of properties which should be included in the query
   * @param preProperty A string which gets prepended to all property runtimeNames
   * @return A StringBuilder with the query which needs to be executed in order to create the table
   * @throws SpRuntimeException If the runtimeName of any property is not allowed
   */
  protected StringBuilder extractEventProperties(List<EventProperty> properties, String preProperty)
      throws SpRuntimeException {
    // output: "randomString VARCHAR(255), randomValue INT"
    StringBuilder s = new StringBuilder();
    String pre = "";
    for (EventProperty property : properties) {
      // Protection against SqlInjection

      checkRegEx(property.getRuntimeName(), "Column name");
      if (property instanceof EventPropertyNested) {
        // if it is a nested property, recursively extract the needed properties
        StringBuilder tmp = extractEventProperties(((EventPropertyNested) property).getEventProperties(),
            preProperty + property.getRuntimeName() + "_");
        if (tmp.length() > 0) {
          s.append(pre).append(tmp);
        }
      } else {
        // Adding the name of the property (e.g. "randomString")
        // Or for properties in a nested structure: input1_randomValue
        // "pre" is there for the ", " part
        s.append(pre).append("\"").append(preProperty).append(property.getRuntimeName()).append("\" ");

        // adding the type of the property (e.g. "VARCHAR(255)")
        if (property instanceof EventPropertyPrimitive) {
          s.append(SqlAttribute.getFromUri(((EventPropertyPrimitive) property).getRuntimeType()).sqlName);
        } else {
          // Must be an EventPropertyList then
          s.append(SqlAttribute.getFromUri(XSD._string.toString()).sqlName);
        }
      }
      pre = ", ";
    }

    return s;
  }

  /**
   * Checks if the input string is allowed (regEx match and length > 0)
   *
   * @param input           String which is getting matched with the regEx
   * @param regExIdentifier Information about the use of the input. Gets included in the exception message
   * @throws SpRuntimeException If {@code input} does not match with {@link JdbcClient#allowedRegEx}
   *                            or if the length of {@code input} is 0
   */
  protected final void checkRegEx(String input, String regExIdentifier) throws SpRuntimeException {
    if (!input.matches(allowedRegEx) || input.length() == 0) {
      throw new SpRuntimeException(regExIdentifier + " '" + input
          + "' not allowed (allowed: '" + allowedRegEx + "') with a min length of 1");
    }
  }



  protected void validateTable() throws SpRuntimeException {
    //TODO: Add validation of an existing table
    HashMap<String, Integer> columnsMap = new HashMap<>();

    try {
      ResultSet column = c.getMetaData().getColumns(null, null, tableName, null);
      while (column.next()) {
        String columnName = column.getString("COLUMN_NAME");
        Integer datatype = Integer.parseInt(column.getString("DATA_TYPE"));
        columnsMap.put(columnName, datatype);
      }
    } catch (SQLException throwables) {
      throwables.printStackTrace();
    }
    //todo
    // create hashmap from event properties with like HashMap<String, Integer> eventMap = new HashMap<>();
    HashMap<String, Integer> eventMap = new HashMap<>();

    //eventProperties;
    if (false) {
      //todo later (!columnsMap.equals(eventMap))
      throw new SpRuntimeException("Table '" + tableName + "' does not match the eventproperties");
    }
  }

  /**
   * Closes all open connections and statements of JDBC
   */
  protected void closeAll() {
    boolean error = false;
    try {
      if (st != null) {
        st.close();
        st = null;
      }
    } catch (SQLException e) {
      error = true;
      logger.warn("Exception when closing the statement: " + e.getMessage());
    }
    try {
      if (c != null) {
        c.close();
        c = null;
      }
    } catch (SQLException e) {
      error = true;
      logger.warn("Exception when closing the connection: " + e.getMessage());
    }
    try {
      if (ps != null) {
        ps.close();
        ps = null;
      }
    } catch (SQLException e) {
      error = true;
      logger.warn("Exception when closing the prepared statement: " + e.getMessage());
    }
    if (!error) {
      logger.info("Shutdown all connections successfully.");
    }
  }

  protected void checkConnected() throws SpRuntimeException {
    if (c == null) {
      throw new SpRuntimeException("Connection is not established.");
    }
  }


  private boolean userHasPriviligesOnTable() {
    //TODO test with other dbÄs and choose type to allow / only check insert or also update and other terms?
    boolean output = false;
    try {
      c = DriverManager.getConnection(url, user, password);
      st = c.createStatement();
      ResultSet rs = c.getMetaData().getTablePrivileges(null, null, tableName);
      ResultSetMetaData rsmd = rs.getMetaData();

      //todo pic correct colum to check wwith
      int cols = rsmd.getColumnCount();

      if (!rs.next()){
        // table does not exist, so user has rights
        output = true;
      } else {
        while (rs.next()) {
          for (int i = 1; i <= cols; i++) {
            //todo save readout and compare rights in insert or update yes
//            System.out.println(rs.getString(i));
          }
        }
      }
      rs.close();
    } catch (SQLException e) {
      closeAll();
      //nothing should go wrong in c and st, rs is checked
      e.printStackTrace();
    }
    return output;
  }
}
