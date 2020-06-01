package org.apache.streampipes.processors.geo.jvm.jts.geofence;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.logging.api.Logger;
import org.locationtech.jts.geom.Geometry;

import java.sql.*;
import java.time.LocalDateTime;

public class SpGeofencelDatabase extends PostgresJdbcClient {

  private String host;
  private Integer port;
  private String dbname;
  protected String user;
  protected String password;
  private String schema;
  private String driver;
  private String urlname;
  private String url;

  protected String allowedRegEx;
  protected Logger logger;
  protected String geofenceName;


  // DB Tables
  protected final String geofenceMainTable = "geofences";
  private final String PG_COLUMN_ID = "id";
  private final String PG_COLUMN_CREATED = "created_at";
  private final String PG_COLUMN_UPDATED = "updated_at";
  private final String PG_COLUMN_GEOFENCENAME = "geofencename";
  private final String PG_COLUMN_GEOMETRY = "geom";


  public SpGeofencelDatabase() {
    super();
  }

  protected void initializeJdbc(String allowedRegEx,
                                String geofenceName,
                                Logger logger) throws SpRuntimeException {
    this.allowedRegEx = allowedRegEx;
    this.logger = logger;
    this.geofenceName = geofenceName;

    this.host = "localhost";
    this.port = 54321;
    this.dbname = "geo_streampipes";
    this.user = "geo_streampipes";
    this.password = "bQgu\"FUR_VH6z>~j";
    this.schema = "geofence";
    this.driver = "org.postgresql.Driver";
    this.urlname = "postgresql";
    this.url = "jdbc:" + urlname + "://" + host + ":" + port + "/";

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
      ensureSchemaExists(url, dbname);
      createGeofenceTable();
      createGeofenceTables();

    } catch (SQLException e) {
      throw new SpRuntimeException("PostGIS Service is not implemented: " + e.getMessage());
    }
  }

  private boolean createGeofenceTable() throws SpRuntimeException {
    boolean returnValue = false;
    try {
      c = DriverManager.getConnection(url + dbname, user, password);
      st = c.createStatement();
      ResultSet rs = c.getMetaData().getTables(null, null, geofenceMainTable, null);
      while (rs.next()) {
        // same table names can exists in different schmemas
        if (!rs.getString("TABLE_SCHEM").toLowerCase().equals(schemaName.toLowerCase())) {

          StringBuilder statement = new StringBuilder("CREATE TABLE ");
          statement.append(schemaName);
          statement.append(".");
          statement.append(geofenceMainTable).append(" ( ");
          statement.append(PG_COLUMN_ID + " SERIAL PRIMARY KEY,");
          statement.append(PG_COLUMN_CREATED + " TIMESTAMP,");
          statement.append(PG_COLUMN_UPDATED + " TIMESTAMP,");
          statement.append(PG_COLUMN_GEOFENCENAME + " TEXT NOT NULL UNIQUE");
          statement.append(PG_COLUMN_GEOMETRY + " GEOMETRY,");
          statement.append(" );");

          try {
            st.executeUpdate(statement.toString());
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
      c = DriverManager.getConnection(url + dbname, user, password);
      st = c.createStatement();

      LocalDateTime settime = LocalDateTime.now();


      StringBuilder statement = new StringBuilder("INSERT INTO ");
      statement.append(schemaName);
      statement.append(".");
      statement.append(geofenceMainTable);
      statement.append(" (" + PG_COLUMN_CREATED + ", " + PG_COLUMN_GEOFENCENAME + ")");
      statement.append(" VALUES ('" + settime + "' , '" + geofenceName + "');");

      st.executeUpdate(statement.toString());

    } catch (SQLException e1) {
      // Unique-Constraint validation try with recursive solution
      closeAll();
      throw new SpRuntimeException("Geofence Name is already taken. Please try another name");
    }
  }


  public boolean updateGeofenceTable(Geometry geom) throws SpRuntimeException {
    boolean result;

    LocalDateTime settime = LocalDateTime.now();

    try {
      c = DriverManager.getConnection(url + dbname, user, password);
      st = c.createStatement();

      StringBuilder statement = new StringBuilder("UPDATE ");
      statement.append(schemaName);
      statement.append(".");
      statement.append(geofenceMainTable);
      statement.append(" SET ");
      statement.append(PG_COLUMN_UPDATED).append(" = ").append(settime).append(",");
      statement.append(PG_COLUMN_GEOMETRY).append(" = ");
      statement.append("ST_GeomFromText('").append(geom.toText()).append("' ,").append(geom.getSRID()).append(")");
      statement.append("WHERE name = ").append(geofenceName);
      statement.append(";");

      st.executeUpdate(statement.toString());
      result = true;

    } catch (SQLException e) {
      throw new SpRuntimeException(e.getMessage());
    }

    return result;
  }


  public boolean deleteTableEntry() throws SpRuntimeException {
    boolean result;

    try {
      c = DriverManager.getConnection(url + dbname, user, password);
      st = c.createStatement();


      StringBuilder statement = new StringBuilder("DELETE FROM ");
      statement.append(schemaName);
      statement.append(".");
      statement.append(geofenceMainTable);
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
