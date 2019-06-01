package org.streampipes.processors.geo.jvm.database.geofence.helper;


import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.streampipes.processors.geo.jvm.helpers.AreaSpOperator;
import org.postgresql.util.PSQLException;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.LocalDateTime;


public class SpInternalDatabase {


    private String host;
    private Integer port;
    private String dbName;
    private String user;
    private String password;
    private String login;


    private final String schemaName = "geofence";


    public SpInternalDatabase(String host, Integer port, String dbName, String user, String password) {
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
        this.password = password;
        this.login = "jdbc:postgresql://" + this.host + ":" + this.port + "/" + this.dbName;
    }



    public Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(this.login, this.user, this.password);
            //System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    public void closeConnection(Connection conn) {
        try {
            conn.close();
            //System.out.println("Disconnected from PostgreSQL server.");
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    public String createTableEntries(Connection conn, String geofenceName, int count ){

        if (count > 0){
            geofenceName += count;
        }

        try (Statement stmt = conn.createStatement()){


            LocalDateTime settime = LocalDateTime.now();


            String createMain = "INSERT INTO " + schemaName + ".main (time, name)" +
                    " VALUES ('" + settime  + "' , '" + geofenceName + "');";
            stmt.executeUpdate(createMain);

            String createInfo = "INSERT INTO " + schemaName + ".info (name)" +
                    " VALUES ('" + geofenceName + "')";

            stmt.executeUpdate(createInfo);



        } catch (PSQLException e1){
            // Unique-Constraint validation try with recursive solution
            count +=1;
            geofenceName = createTableEntries(conn, geofenceName, count);
        } catch ( SQLException e) {
            e.printStackTrace();

        }

        return geofenceName;

    }


    public boolean updateGeofenceTable(Connection conn, String geofenceName, Geometry geom, int unit, double m_value ){

        boolean result = false;

        int epsg = geom.getSRID();
        int decimalPosition = -1;
        AreaSpOperator area = new AreaSpOperator(decimalPosition);
        Double resultArea = null;
        String unitAsString;

        if (geom instanceof Polygon){
            area.calcArea((Polygon) geom);

        } else if (geom instanceof MultiPolygon) {
            area.calcArea((MultiPolygon) geom);
        }

        if (unit != 1 | area.getAreaValue() != -9999){
            area.convertUnit(unit);
        }

        resultArea = area.getAreaValue();
        unitAsString = area.getAreaUnit();



        LocalDateTime settime = LocalDateTime.now();

        try (Statement stmt = conn.createStatement()){


            // update time in main
            String updateTime = "UPDATE " + schemaName + ".main " +
                    "SET time = '" + settime +"' WHERE name = '" + geofenceName + "';";


            stmt.executeUpdate(updateTime);


            String updateGeofence = "UPDATE " + schemaName + ".info "
                    + "SET geom = ST_GeomFromText('" + geom.toText() +"' ," +  epsg + "),"
                    +  " wkt = '" + geom.toText() + "',"
                    + "epsg = " + epsg + ","
                    + " area = " + resultArea + ","
                    +  "areaunit = '" + unitAsString + "',"
                    + "m = " + m_value
                    + " WHERE name = '" + geofenceName + "';";


            stmt.executeUpdate(updateGeofence);

            result = true;


        } catch (SQLException e) {
            e.printStackTrace();

        }

        return result;



    }

    public boolean deleteTableEntry(Connection conn, String geofenceName){
        boolean result = false;


        try (Statement stmt = conn.createStatement()){



            String query = "DELETE FROM " + schemaName + ".main WHERE name = '" +  geofenceName + "';";
            stmt.executeUpdate(query);

            result = true;


        } catch (SQLException e) {
            e.printStackTrace();

        }

        return result;
    }



}
