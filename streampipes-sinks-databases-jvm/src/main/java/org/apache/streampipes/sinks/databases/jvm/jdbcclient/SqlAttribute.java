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
import org.apache.streampipes.vocabulary.XSD;

import java.sql.PreparedStatement;
import java.sql.SQLException;


/**
 * A wrapper class for all supported SQL data types (INT, BIGINT, FLOAT, DOUBLE, VARCHAR(255)).
 * If no matching type is found, it is interpreted as a String (VARCHAR(255))
 */
public enum SqlAttribute {
  // DEFAULT
  INTEGER("INT"),
  LONG("BIGINT"),
  FLOAT("FLOAT"),
  DOUBLE("DOUBLE"),
  STRING("VARCHAR(255)"),
  BOOLEAN("BOOLEAN"),
  //MYSQL
  MYSQL_DATETIME("DATETIME"),
  //POSTGRES / POSTGIS
  PG_DOUBLE("NUMERIC");
  private final String sqlName;

  SqlAttribute(String s) {
    sqlName = s;
  }

  /**
   * Tries to identify the data type of the object {@code o}. In case it is not supported, it is
   * interpreted as a String (VARCHAR(255))
   *
   * @param o The object which should be identified
   * @return An {@link SqlAttribute} of the identified type
   */
  public static SqlAttribute getFromObject(final Object o) {
    SqlAttribute r;
    if (o instanceof Integer) {
      r = SqlAttribute.INTEGER;
    } else if (o instanceof Long) {
      r = SqlAttribute.LONG;
    } else if (o instanceof Float) {
      r = SqlAttribute.FLOAT;
    } else if (o instanceof Double) {
      r = SqlAttribute.DOUBLE;
    } else if (o instanceof Boolean) {
      r = SqlAttribute.BOOLEAN;
    } else {
      r = SqlAttribute.STRING;
    }
    return r;
  }

  public static SqlAttribute getFromUri(final String s) {
    SqlAttribute r;
    if (s.equals(XSD._integer.toString())) {
      r = SqlAttribute.INTEGER;
    } else if (s.equals(XSD._long.toString())) {
      r = SqlAttribute.LONG;
    } else if (s.equals(XSD._float.toString())) {
      r = SqlAttribute.FLOAT;
    } else if (s.equals(XSD._double.toString())) {
      r = SqlAttribute.DOUBLE;
    } else if (s.equals(XSD._boolean.toString())) {
      r = SqlAttribute.BOOLEAN;
    } else {
      r = SqlAttribute.STRING;
    }
    return r;
  }

  /**
   * Sets the value in the prepardStatement {@code ps}
   *
   * @param p     The needed info about the parameter (index and type)
   * @param value The value of the object, which should be filled in the
   * @param ps    The prepared statement, which will be filled
   * @throws SpRuntimeException When the data type in {@code p} is unknown
   * @throws SQLException       When the setters of the statement throw an
   *                            exception (e.g. {@code setInt()})
   */
  public static void setValue(Parameterinfo p, Object value, PreparedStatement ps)
      throws SQLException, SpRuntimeException {
    switch (p.type) {
      case INTEGER:
        ps.setInt(p.index, (Integer) value);
        break;
      case LONG:
        ps.setLong(p.index, (Long) value);
        break;
      case FLOAT:
        ps.setFloat(p.index, (Float) value);
        break;
      case DOUBLE:
        ps.setDouble(p.index, (Double) value);
        break;
      case BOOLEAN:
        ps.setBoolean(p.index, (Boolean) value);
        break;
      case STRING:
        ps.setString(p.index, value.toString());
        break;
      default:
        throw new SpRuntimeException("Unknown SQL datatype");
    }
  }

  @Override
  public String toString() {
    return sqlName;
  }
}
