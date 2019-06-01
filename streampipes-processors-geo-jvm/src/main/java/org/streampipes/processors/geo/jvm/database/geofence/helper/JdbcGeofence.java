package org.streampipes.processors.geo.jvm.database.geofence.helper;


import org.postgresql.util.PSQLException;
import org.streampipes.commons.exceptions.SpRuntimeException;
import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.model.schema.EventProperty;
import org.streampipes.model.schema.EventPropertyNested;
import org.streampipes.model.schema.EventPropertyPrimitive;
import org.streampipes.vocabulary.XSD;

import java.sql.*;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class JdbcGeofence {
    private String allowedRegEx;
    private String urlName;

    private Integer port;
    private String host;
    private String databaseName;
    private String user;
    private String password;
    private String geofenceName;



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




    //================= Constructor for all
    public JdbcGeofence(List<EventProperty> eventProperties,
                      String host,
                      Integer post,
                      String databaseName,
                      String user,
                      String password,
                      String geofenceName,
                      String allowedRegEx,
                      String driver,
                      String urlName,
                      Logger logger) throws SpRuntimeException {
        this.host = host;
        this.port = post;
        this.databaseName = databaseName;
        this.user = user;
        this.password = password;
        this.allowedRegEx = allowedRegEx;
        this.urlName = urlName;
        this.geofenceName = geofenceName;
        this.logger = logger;
        this.eventProperties = eventProperties;





        try {
            Class.forName(driver);
        } catch (ClassNotFoundException e) {
            throw new SpRuntimeException("Driver '" + driver + "' not found.");
        }

        validate();
        connectForGeofence();
    }

    public void setGeofenceName(String geofenceName) {
        this.geofenceName = geofenceName;
    }



    // ============= database methods

    /**
     * Checks whether the given {@link } and the {@link #databaseName}
     * are allowed
     *
     * @throws SpRuntimeException If either the hostname or the databasename is not allowed
     */
    private void validate() {
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
        try {
            checkRegEx(geofenceName, "geofencename");
        } catch (SpRuntimeException e) {
            e.printStackTrace();
        }
    }




    private Connection buildConnection(String url){
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(url, user, password);
            // if successful connection is


        } catch (SQLException e) {
            e.getStackTrace();

        } finally {
            //returns the conn class, if exceptions connections is null! Thats why it has to be tested later on
            return conn;
        }

    }
    

    
    private boolean isConnectionOpen(Connection conn){
        return conn != null;
    }

    
    private void connectForGeofence(){

        // first try to use the plain input from the user. to go the short way
        String u = "jdbc:" + urlName + "://" + host + ":" + port + "/" + databaseName;


        try {
            c= DriverManager.getConnection(u, user, password);

        } catch (SQLException e) {
            logger.error("Check if Docker container is running ");
        }

        if (isConnectionOpen(c)) {
            logger.info("Connection to database successfully established ");
            // statement will be created
            try {
                st = c.createStatement();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }

    private void createMain(int count){

        if (count > 0){
            setGeofenceName(geofenceName += count);
        }

        try (Statement stmt = c.createStatement()){


            LocalDateTime settime = LocalDateTime.now();


            String createMain = "INSERT INTO geofence.main (time, name)" +
                    " VALUES ('" + settime  + "' , '" + geofenceName + "');";
            stmt.executeUpdate(createMain);
            logger.info("geofence entry into main table successful");

        } catch (PSQLException e1){
            // Unique-Constraint validation try with recursive solution
            System.out.println(e1.getServerErrorMessage());
            count +=1;
            createMain(count);
        } catch ( SQLException e) {
            e.printStackTrace();

        }


    }


    private void alterTable() {

        try (Statement stmt = c.createStatement()) {

            String altertable = "ALTER TABLE geofence." + geofenceName +
                    " ADD COLUMN name text";
            stmt.executeUpdate(altertable);
            logger.info("alter geofence table successful");
        } catch (SQLException e) {
            logger.warn("error in alter table");
            e.printStackTrace();
        }
    }


    private void intertInto() {

        try (Statement stmt = c.createStatement()) {

            String insertQuery = "INSERT INTO geofence." + geofenceName +
                    " (name) VALUES ('" + geofenceName + "')";

            stmt.executeUpdate(insertQuery);
            logger.info("insert geofence name into table successful");
        } catch (SQLException e) {
            logger.warn("error in insert table");
            e.printStackTrace();
        }
    }


    public void createGeofenceTableEntries( ) throws SpRuntimeException {


        checkRegEx(geofenceName, "GeofenceName");
        //todo umlaute sind nicht erlaubt, führen somit zu fehlern

        int count = 0;
        createMain(count);

        createTable();

        alterTable();

        intertInto();

    }



    private void createTable() throws SpRuntimeException {

        StringBuilder statement = new StringBuilder("CREATE TABLE geofence.");
        statement.append(geofenceName).append(" ( ");

        statement.append(extractEventProperties(eventProperties)).append(" );");

        try (Statement stmt = c.createStatement()){
            stmt.executeUpdate(statement.toString());
            logger.info("create geofence table successful");
        } catch (SQLException e) {
            e.getMessage();
        }
    }


    private String  createAlterStatement(String geofenceName)  {

        //todo debug
        try {
            checkRegEx(geofenceName, "geofencename");
        } catch (SpRuntimeException e) {
            e.printStackTrace();
        }


        StringBuilder statement = new StringBuilder("AlTER TABLE geofence.");
        statement.append(this.geofenceName).append(" ");
        statement.append(" ADD ( ");
        try {
            statement.append(extractEventProperties(eventProperties)).append(" );");
        } catch (SpRuntimeException e) {
            e.printStackTrace();
        }

        return statement.toString();

    }


    public void update(final Event event) {
        Map<String, Object> eventMap = event.getRaw();
        if (event == null) {
            logger.warn("event is empty and nothing can be put into database");
        }

        //todo write update here
        //todo flatten event if nested

        for (Map.Entry<String, Object> entry : eventMap.entrySet()) {
//            String key = entry.getKey();
//            System.out.println(key);
//            System.out.println("===============");
//            Object value = entry.getValue();
//            System.out.println(value);


            String updateQuery = "UPDATE geofence." + geofenceName +
                    " SET  " + entry.getKey() + " = '" + entry.getValue().toString() + "'"
                    + "  WHERE name = '" + geofenceName + "';";


            try (Statement stmt = c.createStatement()) {
                stmt.executeUpdate(updateQuery);
                logger.info(("update geofence table update with key + " + entry.getKey() + "  successful"));
            } catch (SQLException e) {
                logger.error("\n ============================ \n update query went wrong \n ========================================");
                e.getMessage();
            }
        }

        //after update, update also main taible timestamp

        LocalDateTime settime = LocalDateTime.now();

        try (Statement stmt = c.createStatement()) {
            // update time in main
            String updateTime = "UPDATE geofence.main " +
                    "SET time = '" + settime + "' WHERE name = '" + geofenceName + "';";


            stmt.executeUpdate(updateTime);
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


        public boolean deleteTableEntry(){
        boolean result = false;

        try (Statement stmt = c.createStatement()){


            String query = "DELETE FROM geofence.main WHERE name = '" +  geofenceName + "';";
            stmt.executeUpdate(query);


            String dropper = "DROP TABLE geofence." + geofenceName;
            stmt.executeUpdate(dropper);

            result = true;


        } catch (SQLException e) {
            logger.warn("Deleting went wrong");
            e.printStackTrace();

        }

        return result;
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
     * Initializes the variables {@link #parameters} and {@link #ps}
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
            StringBuilder statement1 = new StringBuilder("ALTER TABLE geofence.");
            statement1.append(geofenceName);


            StringBuilder statement2 = new StringBuilder("ADD ");
            statement2.append(";");


            // Starts index at 1, since the parameterIndex in the PreparedStatement starts at 1 as well
            extendPreparedStatement(event, statement1, statement2, 1);


            String finalStatement = statement1.append(statement2).toString();
            ps = c.prepareStatement(finalStatement);
    }







    /**
     * Clears, fills and executes the saved prepared statement {@code ps} with the data found in
     * event. To fill in the values it calls {@link #fillPreparedStatement(Map)}.
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
     * Fills a prepared statement with the actual values base on {@link #parameters}. If
     * {@link #parameters} is empty or not complete (which should only happen once in the
     * begining), it calls {@link #generatePreparedStatement(Map)} to generate a new one.
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
     * {@link #extractEventProperties(List, String)} with an empty string
     *
     * @param properties The list of properties which should be included in the query
     * @return A StringBuilder with the query which needs to be executed in order to create the table
     * @throws SpRuntimeException See {@link #extractEventProperties(List, String)} for details
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
     * @throws SpRuntimeException If {@code input} does not match with {@link #allowedRegEx}
     * or if the length of {@code input} is 0
     */
    private void checkRegEx(String input, String as) throws SpRuntimeException {
        if (!input.matches(allowedRegEx) || input.length() == 0) {
            throw new SpRuntimeException(as + " '" + input
                    + "' not allowed (allowed: '" + allowedRegEx + "') with a min length of 1");
        }
    }


}