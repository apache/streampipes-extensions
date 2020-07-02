package org.apache.streampipes.sinks.databases.jvm.jdbcclient;

import java.util.HashMap;

public class SqlLockup {

  private HashMap<Integer, String> lockupTable;

  public SqlLockup() {
    lockupTable = new HashMap<>();
    lockupTable.put(4, "INT");
    lockupTable.put(6, "FLOAT");
    lockupTable.put(8, "DOUBLE");
    lockupTable.put(12, "TEXT");
    lockupTable.put(16, "BOOLEAN");


    lockupTable.put(93, "TIMESTAMP");
    lockupTable.put(1111, "GEOMETRY");
  }

  public HashMap<Integer, String> getLockupTable() {
    return lockupTable;
  }


//  protected String getKeyFromValue(int value) {
//
//
//  }
}







    //  ARRAY (2003),
//  BIG_INT(-5),
//  BINARY(-2),
//  BIT( -7),
//  BLOB(2004),
//  BOOLEAN( 16),
//  Char(1),
//  CLOB (2005),
//  DATE(91),
//  DATALINK( 70),
//  DECIMAl( 3),
//  DISTINCT(2001),
//  DOUBLE( 8),
//  FLOAT( 6),
//  INTEGER( 4),
//  JAVAOBJECT (2000),
//  LONG_VAR_CHAR (-16),
//  NCHAR (-15),
//  NCLOB (2011),
//  VARCHARR( 12),
//  VARBINARY( -3),
//  TINY_INT (-6),
//  TIMESTAMPTZ(2014),
//  TIMESTAMP(93),
//  TIME( 92),
//  STRUCT( 2002),
//  SQLXML(2009),
//  SMALLINT( 5),
//  ROWID (-8),
//  REFCURSOR(2012),
//  REF (2006),
//  REAL (7),
//  NVARCHAR (-9),
//  NUMERIC(2),
//  NULL(0),
//
//    INTEGER("INT"),
//        LONG("BIGINT"),
//        FLOAT("FLOAT"),
//        DOUBLE("DOUBLE"),
//        STRING("VARCHAR(255)"),
//        BOOLEAN("BOOLEAN"),
//        //MYSQL
//        MYSQL_DATETIME("DATETIME"),
//        //POSTGRES / POSTGIS
//        PG_DOUBLE("NUMERIC");

