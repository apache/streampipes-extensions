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
import org.apache.streampipes.sinks.databases.jvm.jdbcclient.SqlAttribute;
import org.apache.streampipes.vocabulary.SO;
import org.apache.streampipes.vocabulary.XSD;

import java.sql.*;
import java.util.List;
import java.util.Map;


public class PostgresJdbcClient extends JdbcClient {

  protected String schemaName;
  protected boolean schemaExists = false;
  protected boolean isToDropTable = false;


  public PostgresJdbcClient() {
    super();
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
                                Logger logger,
                                String schemaName,
                                boolean isToDropTable) throws SpRuntimeException {
    super.eventProperties = eventProperties;
    super.databaseName = databaseName;
    super.tableName = tableName;
    super.user = user;
    super.password = password;
    super.allowedRegEx = allowedRegEx;
    super.logger = logger;
    // needed for fallback
    super.urlName = urlName;
    super.host = host;
    super.port = port;
    super.url = "jdbc:" + urlName + "://" + host + ":" + port + "/" + databaseName;
    this.schemaName = schemaName;
    this.isToDropTable = isToDropTable;

    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      throw new SpRuntimeException("Driver '" + driver + "' not found.");
    }
    connect();
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
      c = DriverManager.getConnection(url, user, password);
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
      c = DriverManager.getConnection(url, user, password);
      st = c.createStatement();
      ResultSet rs = c.getMetaData().getTables(null, null, tableName, null);

      // no table at all found in db so create table
      if (!rs.next()){
        createTable();
        tableExists = true;
      } else {
        while (rs.next()) {
          // same table names can exists in different schmemas
          if (rs.getString("TABLE_SCHEM").toLowerCase().equals(schemaName.toLowerCase())) {
            if (isToDropTable) {
              createTable();
              tableExists = true;
            }
            validateTable();
          } else {
            createTable();
            tableExists = true;
          }
        }
      }
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

  @Override
  protected void generatePreparedStatement(final Map<String, Object> event)
      throws SQLException, SpRuntimeException {
    // input: event
    // wanted: INSERT INTO test4321 ( randomString, randomValue ) VALUES ( ?,? );
    checkConnected();
    parameters.clear();
    StringBuilder statement1 = new StringBuilder("INSERT INTO ");
    StringBuilder statement2 = new StringBuilder("VALUES ( ");
    checkRegEx(tableName, "Tablename");
    checkRegEx(schemaName, "Tablename");
    statement1.append(schemaName).append(".");
    statement1.append(tableName).append(" ( ");

    // Starts index at 1, since the parameterIndex in the PreparedStatement starts at 1 as well
    extendPreparedStatement(event, statement1, statement2, 1);

    statement1.append(" ) ");
    statement2.append(" );");
    String finalStatement = statement1.append(statement2).toString();
    ps = c.prepareStatement(finalStatement);
  }

  @Override
  protected StringBuilder extractEventProperties(List<EventProperty> properties, String preProperty)
      throws SpRuntimeException {

    StringBuilder s = new StringBuilder();
    String pre = "";
    for (EventProperty property : properties) {

      // Protection against SQL-Injection
      checkRegEx(property.getRuntimeName(), "Column name");

      if (property instanceof EventPropertyNested) {

        // If is is a nested property, recursively extract the required properties
        StringBuilder tmp = extractEventProperties(((EventPropertyNested) property).getEventProperties(),
            preProperty + property.getRuntimeName() + "_");
        if (tmp.length() > 0) {
          s.append(pre).append(tmp);
        }
      } else {
        // Adding the name of the property (e.g. "randomString")
        s.append(pre).append(preProperty).append(property.getRuntimeName()).append(" ");

        // Adding the type of the property (e.g. "VARCHAR(255)")
        if (property instanceof EventPropertyPrimitive) {
          // use PG_DOUBLE instead of DEFAULT
//          if ((((EventPropertyPrimitive) property).getRuntimeType().equals(XSD._double.toString()))) {
//            s.append(SqlAttribute.PG_DOUBLE.sqlName);
//          } else {
            s.append(SqlAttribute.getFromUri(((EventPropertyPrimitive) property).getRuntimeType()).sqlName);
//          }
        } else {
          // Must be an EventPropertyList then
          s.append(SqlAttribute.getFromUri(XSD._string.toString()).sqlName);
        }
      }
      pre = ", ";
    }

    return s;
  }


}
