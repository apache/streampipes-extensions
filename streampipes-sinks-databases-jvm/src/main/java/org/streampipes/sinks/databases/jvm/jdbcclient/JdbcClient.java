/*
 * Copyright 2018 FZI Forschungszentrum Informatik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.streampipes.sinks.databases.jvm.jdbcclient;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.streampipes.commons.exceptions.SpRuntimeException;
import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.model.schema.EventProperty;
import org.streampipes.model.schema.EventPropertyNested;
import org.streampipes.model.schema.EventPropertyPrimitive;
import org.streampipes.vocabulary.XSD;


public class JdbcClient {
    private String allowedRegEx;
    private String urlName;

    private Integer port;
    private String host;
    private String databaseName;
    private String tableName;
    private String user;
    private String password;
    private String schemaName;

    private boolean tableExists = false;


    private Logger logger;

    private Connection c = null;
    private Statement st = null;
    private PreparedStatement ps = null;


    //=============== parameter setup


    /**
     * The list of properties extracted from the graph
     */
    private List<EventProperty> eventProperties;
    /**
     * The parameters in the prepared statement {@code ps} together with their index and data type
     */
    private HashMap<String, Parameterinfo> parameters = new HashMap<>();

    /**
     * A wrapper class for all supported SQL data types (INT, BIGINT, FLOAT, DOUBLE, VARCHAR(255)).
     * If no matching type is found, it is interpreted as a String (VARCHAR(255))
     */
    private enum SqlAttribute {
        INTEGER("INT"), LONG("BIGINT"), FLOAT("FLOAT"), DOUBLE("DOUBLE"), STRING("text");
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
            } else {
                r = SqlAttribute.STRING;
            }
            return r;
        }

        public static SqlAttribute getFromUri(final String s) {
            SqlAttribute r;
            if (s.equals(XSD._integer)) {
                r = SqlAttribute.INTEGER;
            } else if (s.equals(XSD._long)) {
                r = SqlAttribute.LONG;
            } else if (s.equals(XSD._float)) {
                r = SqlAttribute.FLOAT;
            } else if (s.equals(XSD._double)) {
                r = SqlAttribute.DOUBLE;
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

    /**
     * Contains all information needed to "fill" a prepared statement (index and the data type)
     */
    private static class Parameterinfo {
        private int index;
        private SqlAttribute type;

        private Parameterinfo(final int index, final SqlAttribute type) {
            this.index = index;
            this.type = type;
        }
    }



    //================= Constructor
    public JdbcClient(List<EventProperty> eventProperties,
                      String host,
                      Integer post,
                      String databaseName,
                      String tableName,
                      String user,
                      String password,
                      String allowedRegEx,
                      String driver,
                      String urlName,
                      String schemaName,
                      Logger logger) throws SpRuntimeException {
        this.host = host;
        this.port = post;
        this.databaseName = databaseName;
        this.tableName = tableName;
        this.user = user;
        this.password = password;
        this.allowedRegEx = allowedRegEx;
        this.urlName = urlName;
        this.schemaName = schemaName;
        this.logger = logger;
        this.eventProperties = eventProperties;
        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new SpRuntimeException("Driver '" + driver + "' not found.");
        }

        validate();
        connect();
    }



	// ============= database methods

	/**
	 * Checks whether the given {@link JdbcClient#host} and the {@link JdbcClient#databaseName}
   * are allowed
	 *
	 * @throws SpRuntimeException If either the hostname or the databasename is not allowed
	 */
	private void validate() throws SpRuntimeException {
		// Validates the database name and the attributes
    // See following link for regular expressions:
    // https://stackoverflow.com/questions/106179/regular-expression-to-match-dns-hostname-or-ip-address
		String ipRegex = "^(([0-9]|[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])\\.){3}([0-9]|"
        + "[1-9][0-9]|1[0-9]{2}|2[0-4][0-9]|25[0-5])$";
    String hostnameRegex = "^(([a-zA-Z0-9]|[a-zA-Z0-9][a-zA-Z0-9\\-]*[a-zA-Z0-9])\\.)*"
        + "([A-Za-z0-9]|[A-Za-z0-9][A-Za-z0-9\\-]*[A-Za-z0-9])$";
		//TODO: replace regex with validation method (import org.apache.commons.validator.routines.InetAddressValidator;)
    // https://stackoverflow.com/questions/3114595/java-regex-for-accepting-a-valid-hostname-ipv4-or-ipv6-address)
		/*if (!host.matches(ipRegex) && !host.matches(hostnameRegex)) {
			throw new SpRuntimeException("Error: Hostname '" + postgreSqlHost
					+ "' not allowed");
		}*/
		checkRegEx(databaseName, "Databasename");
	}


    /**
     * creates the connection. multiple error handling included on getSQLState() error code handling.
     * @param url connection url
     * @return a Connection class
     * @throws SpRuntimeException
     */
    private Connection buildConnection(String url) throws SpRuntimeException{
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(url, user, password);
            // if successful connection is


        } catch (SQLException e) {
            // host or port is wrong -- Class 08  Connection Exception
            if (e.getSQLState().substring(0, 2).equals("08")){
                conn.close();
                throw new SpRuntimeException("Connection can't be established. Check host or port setting: \n" + e.getMessage());
            }
            // username or password is wrong -- Class 28  Invalid Authorization Specification
            else if(e.getSQLState().substring(0, 2).equals("28")){
                conn.close();
                throw new SpRuntimeException("User authentication error. Check username or password: \n" + e.getMessage());

            } // database does not exist
            else if ((e.getSQLState().equals("3D000"))){
                conn.close();
                throw new SpRuntimeException("User authentication error. Check username or password: \n" + e.getMessage());
            } else {
                conn.close();
                throw new SpRuntimeException("unexpected error during connection. Error code: " + e.getSQLState() +"\n"
                        + e.getMessage());

            }
        } finally {
            //returns the conn class, if exceptions connections is null! Thats why it has to be tested later on
            return conn;
        }

    }


    /**
     * checks if database, schema or table name exists, depending on strings inside connection.
     * they are kind of static and should not be changed . this Strings should be passed into the function
     * @param conn
     * @param query
     * @return
     */
    private boolean checkExistInPG(Connection conn, String query) {


        boolean exists = false;

        System.out.println(query);
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)){

//            Statement stmt = conn.createStatement();
//            ResultSet rs = stmt.executeQuery(query);

            if(rs.next()) {
                exists = rs.getBoolean(1);
            }



        } catch (SQLException e) {
            System.out.println("============================================================");
            throw new SpRuntimeException("Check if database, table or schema  exists went wrong: " + e.getSQLState() +"\n" + e.getMessage());
            //e.printStackTrace();
        } finally {
            return exists;
        }

    }


    /**
     * checks if connection is open and is not null
     * @param conn connection class
     * @return true or false
     */
    private boolean isConnectionOpen(Connection conn){
        return conn != null;
    }

    /**
     * method to create the database, automatically string query is created and try to establish via executeUpdate
     * @param conn
     * @return
     */
    private boolean createDatabase(Connection conn){
        String query = "CREATE DATABASE " + databaseName + ";";
        boolean  success = false;


        try (Statement stmt = conn.createStatement()) {

            stmt.executeUpdate(query);
            success = true;
            logger.info("Created new database '" + databaseName + "'");


        } catch (SQLException e){
            //todo no rights from user to create database
            throw new SpRuntimeException("Creating Database failed : " + e.getSQLState()  + "\n" + e.getMessage());
        } finally {
            return success;
        }

    }


    /**
     * creates the postgres schema
     * @param conn Connection conn class
     * @return true if creation was successful
     */
    private boolean createSchema(Connection conn){

        String query = "CREATE SCHEMA " + schemaName + ";";
        boolean  created = false;
        try (Statement stmt = conn.createStatement()){


            stmt.executeUpdate(query);
            created = true;
            logger.info("Schema '" + schemaName + "' created ");

        } catch (SQLException e){
            //todo no rights from user to create schema
            throw new SpRuntimeException("Creating Schema failed : " + e.getSQLState()  + "\n" + e.getMessage());
        } finally {
            return created;

        }



    }


    /**
     * deletes the table from database
     * @return
     * @throws SpRuntimeException
     */
    private boolean dropTable() throws SpRuntimeException {


        String query = "DROP TABLE IF EXISTS "+ schemaName + "." + tableName + ";";
        boolean  success = false;


        try (Statement stmt = c.createStatement()){

            stmt.executeUpdate(query);
            success = true;
            logger.info("Table '" + tableName + "' successful deleted");

        } catch (SQLException e){
            //todo no rights from user to delete table
            throw new SpRuntimeException("Error during dropping the table: " + e.getSQLState() +"\n"
                    + e.getMessage());
        } finally {
            return success;
        }
    }


    /**
     * creates the connection. first checks valid databasename. Then tries to build up connection. First with user input. If database does not exist,
     * username will be used to get a connection. if this is not successful postgres as standard user will be used  (but unlikely that username and password is correct)
     * After connection is open schema and table will be created. It will be checked if schema and table will be set. Variable tableExists will be set to true for latwe check in c´createTable.
     * If everything was successful Statement variable will be linked to connection.
     *
     * @throws SpRuntimeException
     */
    private void connect() throws SpRuntimeException {

	    String checkTableName = "SELECT EXISTS (SELECT table_name FROM information_schema.tables WHERE table_schema = '"+ schemaName + "' AND table_name = '"+tableName+"') AS result;";
        String checkDatabaseName = "SELECT EXISTS (SELECT 1 FROM pg_database WHERE datname = '"+ databaseName + "') AS result;";
        String checkSchemaName = "SELECT EXISTS (SELECT nspname FROM pg_catalog.pg_namespace WHERE nspname = '" + schemaName +"') AS result;";


        checkRegEx(databaseName, "databasename");

        // problem: if no database name is set, username will be assumed as database name. this works for standard user +
        // postgres but not for other usernames. so first check if connection with username is working, otherwise use
        // standard user postgres

        // first try to use the plain input from the user. to go the short way
        String u = "jdbc:" + urlName + "://" + host + ":" + port + "/" + databaseName;

        c = buildConnection(u);

        //connection can't be established
        if (!isConnectionOpen(c)){
            // possible reason: database does not exists or other exceptions.
            // First check: following string uses Username as database name automatically
            u = "jdbc:" + urlName + "://" + host + ":" + port + "/";
            c = buildConnection(u);

            //if connection is open database can be created
            if (isConnectionOpen(c)) {
                // to be sure that database name is not used already
                if (!checkExistInPG(c, checkDatabaseName)) {
                    //database will be created
                    createDatabase(c);
                    // and connection will be created
                    u = "jdbc:" + urlName + "://" + host + ":" + port + "/" + databaseName;
                    c = buildConnection(u);
                    // if connection still fails something else is wrong and can't be fixed
                    if (!isConnectionOpen(c)){
                        logger.warn("Connection failed");
                        closeAll();
                    }

                }
            } else {
                // last try is to use the postgres standard user to open a connection

                u = "jdbc:" + urlName + "://" + host + ":" + port + "/postgres" ;
                c = buildConnection(u);

                // connection with postgres user was successful
                if (isConnectionOpen(c)) {
                    if (!checkExistInPG(c,checkDatabaseName )) {
                        //database will be created
                        createDatabase(c);
                        // and connection will be created
                        u = "jdbc:" + urlName + "://" + host + ":" + port + "/" + databaseName;
                        c = buildConnection(u);
                        // if connection still fails something else is wrong and can't be fixed
                        if (!isConnectionOpen(c)){
                            logger.warn("Connection failed als really nothing helps");
                            closeAll();
                        }

                    }
                }
            }
            logger.warn("Last try to establishing the connection failed. Check user input or postgres user rights");
        }



        if (isConnectionOpen(c)) {
            logger.info("Connection to database successfully established ");

            //check if schema exists
            if (!(checkExistInPG(c, checkSchemaName))) {
                createSchema(c);
            }


            //validates if table already exists
            if (!(checkExistInPG(c, checkTableName))) {
                createTable();
                tableExists = true;
            } else {
                // if table already exists, boolean has to be set to true as well due checking in createTable later on
                tableExists = true;

            }
            logger.info("Table created successfully");


        } else {
            logger.warn("Connection FAILED ");

        }

        try {
            //sets st from null to correct statement
            st = c.createStatement();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }








     /**
	 * Connects to the HadoopFileSystem Server and initilizes {@link JdbcClient#c} and
	 * {@link JdbcClient#st}
	 *
	 * @throws SpRuntimeException When the connection could not be established (because of a
	 * wrong identification, missing database etc.)
	 */
    private void createTable() throws SpRuntimeException {
        checkRegEx(tableName, "Tablename");


        StringBuilder statement = new StringBuilder("CREATE TABLE ");
        statement.append(schemaName);
        statement.append(".");
        statement.append(tableName).append(" ( ");
        statement.append(extractEventProperties(eventProperties)).append(" );");


        try (Statement stmt = c.createStatement()){
            stmt.executeUpdate(statement.toString());
            tableExists = true;
        } catch (SQLException e) {
            throw new SpRuntimeException(e.getMessage());
        }
    }


	//TODO: Add batch support (https://stackoverflow.com/questions/3784197/efficient-way-to-do-batch-inserts-with-jdbc)
	/**
	 * Prepares a statement for the insertion of values or the
	 *
	 * @param event The event which should be saved to the Postgres table
	 * @throws SpRuntimeException When there was an error in the saving process
	 */
	public void save(final Event event) throws SpRuntimeException {
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


    /**
     * Closes all open connections and statements of JDBC
     */
    public void closeAll() {
        boolean error = false;
        try {
            if (st != null) {
                st.close();
            }
        } catch (SQLException e) {
            error = true;
            logger.warn("Exception when closing the statement: " + e.getMessage());
        }
        try {
            if (c != null) {
                c.close();
            }
        } catch (SQLException e) {
            error = true;
            logger.warn("Exception when closing the connection: " + e.getMessage());
        }
        try {
            if (ps != null) {
                ps.close();
            }
        } catch (SQLException e) {
            error = true;
            logger.warn("Exception when closing the prepared statement: " + e.getMessage());
        }
        if(!error) {
            logger.info("Shutdown all connections successfully.");
        }
    }


	// ================= Parameter methods


	/**
	 * Clears, fills and executes the saved prepared statement {@code ps} with the data found in
   * event. To fill in the values it calls {@link JdbcClient#fillPreparedStatement(Map)}.
	 *
	 * @param event Data to be saved in the SQL table
	 * @throws SQLException When the statement cannot be executed
	 * @throws SpRuntimeException When the table name is not allowed or it is thrown
	 * by {@link SqlAttribute#setValue(Parameterinfo, Object, PreparedStatement)}
	 */
	private void executePreparedStatement(final Map<String, Object> event)
			throws SQLException, SpRuntimeException {
		if (ps != null) {
			ps.clearParameters();
		}
		fillPreparedStatement(event);
		ps.executeUpdate();
	}

  private void fillPreparedStatement(final Map<String, Object> event)
      throws SQLException, SpRuntimeException {
    fillPreparedStatement(event, "");
  }

  /**
   * Fills a prepared statement with the actual values base on {@link JdbcClient#parameters}. If
   * {@link JdbcClient#parameters} is empty or not complete (which should only happen once in the
   * begining), it calls {@link JdbcClient#generatePreparedStatement(Map)} to generate a new one.
   * @param event
   * @param pre
   * @throws SQLException
   * @throws SpRuntimeException
   */
  private void fillPreparedStatement(final Map<String, Object> event, String pre)
      throws SQLException, SpRuntimeException {
    //TODO: Possible error: when the event does not contain all objects of the parameter list
    for (Map.Entry<String, Object> pair : event.entrySet()) {
      String newKey = pre + pair.getKey();
      if (pair.getValue() instanceof Map) {
        // recursively extracts nested values
        fillPreparedStatement((Map<String, Object>)pair.getValue(), newKey + "_");
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
	 * @param  event The event which is getting analyzed
	 * @throws SpRuntimeException When the tablename is not allowed
	 * @throws SQLException When the prepareStatment cannot be evaluated
	 */
	private void generatePreparedStatement(final Map<String, Object> event)
			throws SQLException, SpRuntimeException {
		// input: event
		// wanted: INSERT INTO test4321 ( randomString, randomValue ) VALUES ( ?,? );
		parameters.clear();
		StringBuilder statement1 = new StringBuilder("INSERT  INTO ");
		StringBuilder statement2 = new StringBuilder("VALUES ( ");
		checkRegEx(tableName, "Tablename");
		statement1.append(schemaName).append(".");
		statement1.append(tableName).append(" ( ");

    // Starts index at 1, since the parameterIndex in the PreparedStatement starts at 1 as well
		extendPreparedStatement(event, statement1, statement2, 1);

		statement1.append(" ) ");
		statement2.append(" );");
		String finalStatement = statement1.append(statement2).toString();
		ps = c.prepareStatement(finalStatement);
	}


  private int extendPreparedStatement(final Map<String, Object> event,
    StringBuilder s1, StringBuilder s2, int index) throws SpRuntimeException {
    return extendPreparedStatement(event, s1, s2, index, "", "");
  }

  /**
   *
   * @param event
   * @param s1
   * @param s2
   * @param index
   * @param preProperty
   * @param pre
   * @return
   * @throws SpRuntimeException
   */
  private int extendPreparedStatement(final Map<String, Object> event,
      StringBuilder s1, StringBuilder s2, int index, String preProperty, String pre)
      throws SpRuntimeException {
	  for (Map.Entry<String, Object> pair : event.entrySet()) {
	    if (pair.getValue() instanceof Map) {
	      index = extendPreparedStatement((Map<String, Object>)pair.getValue(), s1, s2, index,
            pair.getKey() + "_", pre);
      } else {
        checkRegEx(pair.getKey(), "Columnname");
        parameters.put(pair.getKey(),
            new Parameterinfo(index, SqlAttribute.getFromObject(pair.getValue())));
        s1.append(pre).append("\"").append(preProperty).append(pair.getKey()).append("\"");
        s2.append(pre).append("?");
        index++;
      }
      pre = ", ";
    }
    return index;
  }


  /**
   * Creates a SQL-Query with the given Properties (SQL-Injection save). Calls
   * {@link JdbcClient#extractEventProperties(List, String)} with an empty string
   *
   * @param properties The list of properties which should be included in the query
   * @return A StringBuilder with the query which needs to be executed in order to create the table
   * @throws SpRuntimeException See {@link JdbcClient#extractEventProperties(List, String)} for details
   */
	private StringBuilder extractEventProperties(List<EventProperty> properties)
      throws SpRuntimeException {
    return extractEventProperties(properties, "");
  }

  /**
   * Creates a SQL-Query with the given Properties (SQL-Injection save). For nested properties it
   * recursively extracts the information. EventPropertyList are getting converted to a string (so
   * in SQL to a VARCHAR(255)). For each type it uses {@link SqlAttribute#getFromUri(String)}
   * internally to identify the SQL-type from the runtimeType.
   *
   * @param properties The list of properties which should be included in the query
   * @param preProperty A string which gets prepended to all property runtimeNames
   * @return A StringBuilder with the query which needs to be executed in order to create the table
   * @throws SpRuntimeException If the runtimeName of any property is not allowed
   */
	private StringBuilder extractEventProperties(List<EventProperty> properties, String preProperty)
      throws SpRuntimeException {
	  //IDEA: test, if the string is empty and maybe throw an exception (if it is the bottom layer)
	  // output: "randomString VARCHAR(255), randomValue INT"
    StringBuilder s = new StringBuilder();
    String pre = "";
    for (EventProperty property : properties) {
      // Protection against SqlInjection

      checkRegEx(property.getRuntimeName(), "Column name");
      if (property instanceof EventPropertyNested) {
        // if it is a nested property, recursively extract the needed properties
        StringBuilder tmp = extractEventProperties(((EventPropertyNested)property).getEventProperties(),
            preProperty + property.getRuntimeName() + "_");
        if(tmp.length() > 0) {
          s.append(pre).append(tmp);
        }
      } else {
        // Adding the name of the property (e.g. "randomString")
        // Or for properties in a nested structure: input1_randomValue
        // "pre" is there for the ", " part
        s.append(pre).append("\"").append(preProperty).append(property.getRuntimeName()).append("\" ");

        // adding the type of the property (e.g. "VARCHAR(255)")
	      if (property instanceof EventPropertyPrimitive) {
          s.append(SqlAttribute.getFromUri(((EventPropertyPrimitive)property).getRuntimeType()));
        } else {
	        // Must be an EventPropertyList then
          s.append(SqlAttribute.getFromUri(XSD._string.toString()));
        }
      }
      pre = ", ";
    }

	  return s;
	}

  /**
   * Checks if the input string is allowed (regEx match and length > 0)
   *
   * @param input String which is getting matched with the regEx
   * @param as Information about the use of the input. Gets included in the exception message
   * @throws SpRuntimeException If {@code input} does not match with {@link JdbcClient#allowedRegEx}
   * or if the length of {@code input} is 0
   */
	private void checkRegEx(String input, String as) throws SpRuntimeException {
    if (!input.matches(allowedRegEx) || input.length() == 0) {
      throw new SpRuntimeException(as + " '" + input
          + "' not allowed (allowed: '" + allowedRegEx + "') with a min length of 1");
    }
  }


  //=========================== unknown group

    private void validateTable() throws SpRuntimeException {
        if(false) {
            throw new SpRuntimeException("Table '" + tableName + "' does not match the eventproperties");
        }
    }

    private boolean checkTableSchemaIdenticalWithInput(Connection conn, final Map<String, Object> event){

        Statement stmt = null;
        ResultSet rs = null;
        boolean isIdentical = false;

        String name;
        String type;

        ArrayList<String> table_columnName = new ArrayList<>();
        ArrayList<String> table_columnType = new ArrayList<>();

        String 	query = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '" + tableName + "';";

        try {
            stmt = conn.createStatement();
            rs = stmt.executeQuery(query);

            //runs through query result
            while (rs.next()) {
                name = rs.getString("column_name");
                type = rs.getString("data_type");
                table_columnName.add(name);
                table_columnType.add(type);
            }
            stmt.close();
            rs.close();

            //if size is different result is false
            if (event.size() == table_columnName.size()){

                //todo
                //identical position of the statements is important
                //identical type is important
                //identical name is important
                //whats with nested?

                isIdentical = true;
            }


        } catch (SQLException e) {
            throw new SpRuntimeException("Table readout went wrong: " + e.getSQLState()  + "\n" + e.getMessage());
            //e.printStackTrace();
        } finally {

            return isIdentical;
        }

    }





    private int getSizeOfTable(Connection conn) throws SpRuntimeException {

        int size = 0;
        String query = "SELECT COUNT(column_name), data_type FROM information_schema.columns WHERE table_name = '" + tableName + "';";

        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)){

            if (!rs.first()) {
                size = 0;
            } else while (rs.next()){
                size = size + 1;
            }
        } catch (SQLException e) {
            throw new SpRuntimeException("Measuring size of table went wrong: " + e.getSQLState() +"\n" + e.getMessage());
        } finally {
            return size;
        }
    }


}
