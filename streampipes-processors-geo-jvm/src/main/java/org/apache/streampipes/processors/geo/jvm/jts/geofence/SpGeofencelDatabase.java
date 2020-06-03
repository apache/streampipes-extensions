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

package org.apache.streampipes.processors.geo.jvm.jts.geofence;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.logging.api.Logger;
import org.apache.streampipes.model.schema.EventProperty;
import org.locationtech.jts.geom.Geometry;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.List;

public class SpGeofencelDatabase extends PostgresJdbcClient {


  protected String geofenceName;
  protected String url;
  protected String databaseName;
  protected String tableName;


  // DB Tables
  private final String PG_COLUMN_ID = "id";
  private final String PG_COLUMN_CREATED = "created_at";
  private final String PG_COLUMN_UPDATED = "updated_at";
  private final String PG_COLUMN_GEOFENCENAME = "geofencename";
  private final String PG_COLUMN_GEOMETRY = "geom";


  public SpGeofencelDatabase() {
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
                                boolean isToDropTable,
                                String geofenceName) throws SpRuntimeException {
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
        logger,
        schemaName,
        isToDropTable);

    this.url = "jdbc:" + urlName + "://" + host + ":" + port + "/";
    this.geofenceName = geofenceName;
    this.databaseName = databaseName;
    this.tableName = tableName;
    this.url = "jdbc:" + urlName + "://" + host + ":" + port + "/";

    try {
      Class.forName(driver);
    } catch (ClassNotFoundException e) {
      throw new SpRuntimeException("Driver '" + driver + "' not found.");
    }

    connect();
  }

  protected void connect() throws SpRuntimeException {
    try {
      c = DriverManager.getConnection(url, user, password);
      ensureSchemaExists(this.url, databaseName);
      createGeofenceTable();
      createGeofenceTables();

    } catch (SQLException e) {
      throw new SpRuntimeException("PostGIS Service is not implemented: " + e.getMessage());
    }
  }


  private String createGeofenceTableQuery(){

    StringBuilder statement = new StringBuilder("CREATE TABLE ");
    statement.append(schemaName);
    statement.append(".");
    statement.append(tableName).append(" ( ");
    statement.append(PG_COLUMN_ID + " SERIAL PRIMARY KEY, ");
    statement.append(PG_COLUMN_CREATED + " TIMESTAMP, ");
    statement.append(PG_COLUMN_UPDATED + " TIMESTAMP, ");
    statement.append(PG_COLUMN_GEOFENCENAME + " TEXT NOT NULL UNIQUE, ");
    statement.append(PG_COLUMN_GEOMETRY + " GEOMETRY");
    statement.append(" );");

    return statement.toString();
  }
  private boolean createGeofenceTable() throws SpRuntimeException {
    boolean returnValue = false;
    try {
      c = DriverManager.getConnection(url + databaseName, user, password);
      st = c.createStatement();
      ResultSet rs = c.getMetaData().getTables(null, null, tableName, null);
      //if table does not exist create it
      if (!rs.next()) {
        try {
          st.executeUpdate(createGeofenceTableQuery());
          returnValue = true;
        } catch (SQLException e) {
          throw new SpRuntimeException("Something went wrong during table creation with error message: " + e.getMessage());
        }
      } else {
        // ok so table exist but in the right schema? and if not, create table
        if (!(rs.getString("TABLE_SCHEM").toLowerCase().equals(schemaName.toLowerCase()))) {
          try {
            st.executeUpdate(createGeofenceTableQuery());
            returnValue = true;
          } catch (SQLException e) {
            throw new SpRuntimeException("Something went wrong during table creation with error message: " + e.getMessage());
          }
        }
      }
      rs.close();
    } catch (SQLException e) {
      closeAll();
      throw new SpRuntimeException(e.getMessage());
    }

    return returnValue;
  }

  public void createGeofenceTables() throws SpRuntimeException {
    try {
      c = DriverManager.getConnection(url + databaseName, user, password);
      st = c.createStatement();

      LocalDateTime settime = LocalDateTime.now();

      StringBuilder statement = new StringBuilder("INSERT INTO ");
      statement.append(schemaName);
      statement.append(".");
      statement.append(tableName);
      statement.append(" (" + PG_COLUMN_CREATED + ", " + PG_COLUMN_GEOFENCENAME + ")");
      statement.append(" VALUES ('" + settime + "' , '" + geofenceName + "');");

      st.executeUpdate(statement.toString());

    } catch (SQLException e1) {
      // Unique-Constraint validation try with recursive solution
      closeAll();
      throw new SpRuntimeException("Geofence Name is already taken. Please try another name" +e1.getMessage());
    }
  }


  public boolean updateGeofenceTable(Geometry geom) throws SpRuntimeException {
    boolean result;

    LocalDateTime settime = LocalDateTime.now();

    try {
      c = DriverManager.getConnection(url + databaseName, user, password);
      st = c.createStatement();

      StringBuilder statement = new StringBuilder("UPDATE ");
      statement.append(schemaName);
      statement.append(".");
      statement.append(tableName);
      statement.append(" SET ");
      statement.append(PG_COLUMN_UPDATED).append(" = '").append(settime).append("',");
      statement.append(PG_COLUMN_GEOMETRY).append(" = ");
      statement.append("ST_GeomFromText('").append(geom.toText()).append("' ,").append(geom.getSRID()).append(")");
      statement.append(" WHERE ");
      statement.append(PG_COLUMN_GEOFENCENAME).append(" = '").append(geofenceName).append("'");
      statement.append(";");

      st.executeUpdate(statement.toString());
      result = true;

    } catch (SQLException e) {
      throw new SpRuntimeException("Something went wrong during table creation with error message :" + e.getMessage());
    }

    return result;
  }


  public boolean deleteTableEntry() throws SpRuntimeException {
    boolean result;

    try {
      c = DriverManager.getConnection(url + databaseName, user, password);
      st = c.createStatement();


      StringBuilder statement = new StringBuilder("DELETE FROM ");
      statement.append(schemaName);
      statement.append(".");
      statement.append(tableName);
      statement.append(" WHERE ");
      statement.append(PG_COLUMN_GEOFENCENAME).append(" = '").append(geofenceName).append("';");

      st.executeUpdate(statement.toString());
      result = true;

    } catch (SQLException e) {
      closeAll();
      throw new SpRuntimeException(e.getMessage());
    }
    return result;
  }
}
