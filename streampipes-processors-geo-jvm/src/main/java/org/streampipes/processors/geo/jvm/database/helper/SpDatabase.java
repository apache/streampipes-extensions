package org.streampipes.processors.geo.jvm.database.helper;


import org.locationtech.jts.geom.*;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public class SpDatabase {

    private String host;
    private int port;
    private String dbName;
    private String user;
    private String password;
    private String login;


    public SpDatabase (String host, int port, String dbName, String user, String password){
        this.host = host;
        this.port = port;
        this.dbName = dbName;
        this.user = user;
        this.password = password;
        this.login = "jdbc:postgresql://" + this.host + ":" + this.port + "/" + this.dbName;
    }


    protected String getLogin() {
        return login;
    }

    protected String getUser() {
        return user;
    }

    protected String getPassword() {
        return password;
    }




    public  Connection connect() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(getLogin(), getUser(), getPassword());
            System.out.println("Connected to the PostgreSQL server successfully.");
        } catch (SQLException e) {
            System.out.println(e.getMessage());
        }
        return conn;
    }

    public void closeConnection(Connection conn){
        try {
            conn.close();
        } catch (SQLException e) {
            e.printStackTrace();
        }
    }



    public enum IsochroneUnits {
        seconds (1), meters (2);

        private int number;

        IsochroneUnits(int number) {
            this.number = number;
        }

        public int getNumber() {
            return number;
        }
    }


    /**
     * Creates an sql routing query query.
     *
     * @param source start point of routing
     * @param target destination point of routing
     * @return A string with the complete sql query
     */
    public String prepareRoutingQuery(Point source, Point target) {

        //pgrouting works in WGS84, so if the points are in another epsg code, coords
        //will be reprojected
        if (!isWGS84(target)){
            target = (Point) transformSpGeom(target, 4326);
        }
        if (!isWGS84(source)){
            source = (Point) transformSpGeom(source, 4326);
        }

        //todo adjust name to new location --> ka.ways_vertices_pgr
        String query =
                "SELECT dijkstra.*, ST_X(w.the_geom) as x, ST_Y(w.the_geom) as y "
                        +" FROM pgr_dijkstra( "
                        +"'SELECT gid AS id, "
                        + "source, "
                        + "target, "
                        + "cost_s as cost "
                        + "FROM ka.ways', "
                        + "(SELECT id "
                        + "FROM ka.ways_vertices_pgr "
                        + "Order BY the_geom <-> ST_SetSRID(ST_MakePoint(" + source.getX() + "," + source.getY() + "), 4326)"
                        + "LIMIT 1), "
                        + "(SELECT id "
                        +"FROM ka.ways_vertices_pgr "
                        +"Order BY the_geom <-> ST_SetSRID(ST_MakePoint(" + target.getX() + "," + target.getY() + "), 4326) "
                        +"LIMIT 1), "
                        +"directed := false) as dijkstra "
                        +"LEFT JOIN ka.ways_vertices_pgr w "
                        +"ON (dijkstra.node = w.id) ORDER BY seq; ";

        return query;
    }


    /**
     *
     * @param point Point geometry were isochrone starts with
     * @param value  value to calculate the isochrone
     * @param unitSecond true if value are seconds, if false meter values are expected
     * @return
     */
    public String prepareIsochroneQuery(Point point, int value, boolean unitSecond) {

        // test if point is in wgs84
        if (!isWGS84(point)) {
            point = (Point) transformSpGeom(point, 4326);
        }

        String cost_field = "cost_s";
        if (!unitSecond) {
            cost_field = "length_m";
        }

        String query = "" +
                "SELECT row_number() OVER() AS id, x, y  "
                + "FROM pgr_alphashape('SELECT id::int4, "
                + " lat::float8 AS y, "
                + "lon::float8 AS x "
                + "FROM ka.ways_vertices_pgr w "
                + "JOIN (SELECT * FROM pgr_drivingdistance "
                + "('' SELECT gid AS id, "
                + "source::int8 AS source, "
                + "target::int8  AS target, "
                + cost_field + "::float8 AS cost, "
                + "reverse_cost_s::float8 as reverse_cost "
                + "FROM ka.ways'', "
                + "(SELECT id "
                + "FROM ka.ways_vertices_pgr "
                + "Order BY the_geom <-> ST_SetSRID(ST_MakePoint(" + point.getY() + "," + point.getX() + "), 4326) "
                + " LIMIT 1), "
                + value + ","
                + "true) "
                + ") AS dd "
                + "ON w.id = dd.node'::text "
                + "); ";

        return query;

    }



    /**
     *
     * @param query query string, send to the database
     * @param conn connection Class
     * @param isRoutingQuery true if request is a routing query (Linestring),
     *                false if request is a isochrone query (Polygon)
     * @return returns a coordinate list
     */
    public CoordinateList getResultFromDB(String query, Connection conn, boolean isRoutingQuery) {

        Double x = null;
        Double y = null;
        CoordinateList readout = new CoordinateList();


        // try with resources (no finally block and double try catch necessary
        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)){

            //reads result set until next is false, rs is kind of table
            while (rs.next()) {
                x = rs.getDouble("x");
                y = rs.getDouble("y");

                //creates a coordinate
                Coordinate coord = new Coordinate(x,y);
                //and adding the coordinate to the List
                readout.add(coord);
            }

            //if result is polygon, first and last point are the same
            if(!isRoutingQuery){
                readout.add(readout.getCoordinate(0));
            }

            // internal validator
            //todo more checks probably distance checker?
            readout.removeIf(coordinate -> ((coordinate.x ==0)) | (coordinate.y ==0));


        } catch (SQLException e) {
            //todo logger
            e.printStackTrace();
        }

        return readout;
    }


    /**
     *
     * @param input CoordinateList
     * @param isRoutingQuery true if request is a routing, false if request is an isochrone
     * @return a class Geometry in EPSG 4326
     */
    public Geometry createSPGeometryFromDB(CoordinateList input, boolean isRoutingQuery){

        PrecisionModel prec = getPrecisionModel(4326);
        GeometryFactory geomFactory = new GeometryFactory(prec, 4326);

        Geometry geom = null;

        if(isRoutingQuery){
            geom = geomFactory.createLineString(input.toCoordinateArray());
            geom.getClass().getName();
        } else {
            geom = geomFactory.createPolygon(input.toCoordinateArray());
            geom.getClass().getName();
        }

        //cast to main Geometry instead to get polygon or line class
        return geom;
    }



    /**
     *
     * @param geom
     * @return
     */
    public String prepareElevationQuery(Geometry geom){

        Geometry internal = geom;

        if (!isWGS84(geom)){
            internal = transformSpGeom(geom,4326);
        }

        String wkt = internal.toText();

        String query =
                "SELECT b.*, ST_Value(a.rast, b.geom) as pitch "
                        + "FROM srtm.bw a, "
                        +"( SELECT (dp).path[1] AS INDEX, (dp).geom AS geom FROM "
                        +"( SELECT st_dumppoints(geom) AS dp FROM "
                        +"(SELECT ST_GeomFromText('" + wkt + "', 4326) AS geom) AS foo ) "
                        +"AS boo ) "
                        +"AS b "
                        +"WHERE  ST_Intersects(b.geom, a.rast);";

        return query;
    }


    /**
     *
     * @param query
     * @param conn
     * @param geom
     * @return
     */
    public CoordinateList getElevationFromDB(String query, Connection conn, Geometry geom){
        Double z = null;

        List<Double> elev = new ArrayList<>();
        //Coordinate coord = null;

        CoordinateList coords= new CoordinateList();


        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)){

            //reads out the result set add adds pitch to a List
            while (rs.next()) {
                z = rs.getDouble("pitch");
                elev.add(z);
            }
        } catch (SQLException e) {
            e.printStackTrace();

        }

        //writes pitch and length in coordinate list
        if (elev.size() == geom.getCoordinates().length){
            int i = 0;
            for (Coordinate c : geom.getCoordinates()){
                c.setZ(elev.get(i));
                coords.add(c);
                i++;
            }
        } else {
            //if size of pitch differs from geometry, then a single 0 0 0 value is set
            Coordinate coord;
            coords.add(coord = new Coordinate(0, 0, Double.valueOf(null)));
        }

        return coords;
    }


    /**
     * enum list with valid result options for
     */
    public enum calcPitchResultOption {
        PERCENT (1), ALPHA (2);

        private int number;


        calcPitchResultOption(int number) {
            this.number = number;
        }

        public int getNumber() {
            return number;
        }
    }




    /**
     *
     * @param coordList Coordinate list
     * @param val
     * @return
     */
    public ArrayList<Double> calcPitchFromDB(CoordinateList coordList, int val){


        coordList.getCoordinate(0).getZ();

        ArrayList<Double> result_alpha = new ArrayList<>();
        ArrayList<Double> result_pitch = new ArrayList<>();

        PrecisionModel wgs_prec = new PrecisionModel(10000000);
        GeometryFactory coordfactory = new GeometryFactory(wgs_prec, 4326);

        Point geom1 = null;
        Point geom2 = null;


        //todo check if it in enough to check first coordinate to be empty or check all

        // if a z value is in the first coordinate, it can be assumed that in the rest is also
        // a z value and calculation can begin
        if (!(Double.valueOf(coordList.getCoordinate(0).getZ()) == null)) {
            // start at i +1 to skip start points
            for (int i = 1; i < coordList.size(); i++) {


                // calculates necessary parameters distance (hypotenuse), deltaElev (opposite side)
                // and adjacent side
                geom1 = coordfactory.createPoint(coordList.getCoordinate(i - 1));
                geom2 = coordfactory.createPoint(coordList.getCoordinate(i));

                // find utm zone for distance calculation in meter
                geom1 = (Point) transformSpGeom(geom1, findWgsUtm_EPSG(extractPoint(geom1)));
                //for second geom tages utm zone from geom1
                geom2 = (Point) transformSpGeom(geom2, geom1.getSRID());
                //geom2 = (Point) transformSpGeom(geom2, findWgsUtm_EPSG(extractPoint(geom1)));

                Double distance;
                Double deltaElev;
                Double adjacent;
                Double alpha;
                Double pitch;


                distance = geom1.distance(geom2);
                deltaElev = coordList.getCoordinate(i - 1).getZ() - coordList.getCoordinate(i).getZ();

                int multiplier = 1;
                if (deltaElev < 0){
                    multiplier = -1;
                    deltaElev *= multiplier;
                }

                adjacent = Math.sqrt(Math.pow(distance, 2) - Math.pow(deltaElev, 2));

//                if (adjacent ==0 ){
//                    alpha = -9999d;
//                    pitch = -9999d;
//                } else {
                    alpha = ((100 *(Math.atan(distance))) / distance) * multiplier;
                    pitch =  ((100 * deltaElev) / adjacent) * multiplier;
//                }

                // if something went wrong. e.g. dividing through 0 because distance or adjacent is 0, normally NaN values are
                if (pitch.isNaN() || pitch == -9999 || alpha.isNaN() || alpha == -9999) {
                    //todo logger
                    System.out.println("Something went wrong. check values \n" +
                            "distance: " + distance + "\n" +
                            "adjacent: " + adjacent);
                } else {
                    result_alpha.add(alpha);
                    result_pitch.add(pitch);
                }
            }
        }

        // alpha
        if (val == 2){
            return result_alpha;
        } else {
            return result_pitch;
        }
    }


    public enum AggregationType {
        avg(1), sum(2), min(3), max(4);

        private int number;

        AggregationType(int number) {
            this.number = number;
        }

        public int getNumber() {
            return number;
        }
    }


    /**
     * prepares the sql query string
     * @param geom input geometry (polygon required)
     * @param year select the year that wants to be accessed
     * @return
     */
    public String preparePrecipitationQuery(Geometry geom, Integer year, String aggregationType ){


        Geometry internal = geom;


        if (!isWGS84(geom)){
            internal = transformSpGeom(geom,4326);
        }

        String wkt = internal.toText();

        String query =
                "WITH " +
                        "sub AS(" +
                        "SELECT (st_intersection(prec.rast, 1, poly.geom)).*  " +
                        "FROM " +
                        "(select ST_GeomFromText('"+ wkt + "',4326) AS geom) poly " +
                        " , niederschlag.\""+ year + "\" prec " +
                        "WHERE st_intersects(prec.rast, 1, poly.geom) " +
                        ") " +
                        "SELECT ("+  aggregationType +"(val)) AS result FROM sub;";

        return query;
    }


    /**
     * returns a single value of raster or.org.streampipes.processors.geo.jvm.geofence.storing function
     * @param query
     * @param conn
     * @return
     */
    public Double getValuesFromRaster(String query, Connection conn){

        Double result = -9999d;


        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)){

            //reads result set until next is false, rs is kind of table
            while (rs.next()) {
                result = rs.getDouble("result");

                // result can be empty if polygon is out of range. null is returned, dedicated to -999
                if (rs.wasNull()){
                    result = -9999d;
                }

            }

        } catch (SQLException e) {
            //todo logger
            e.printStackTrace();
        }

        return result;
    }



    public List<Integer> createPrecipitationRange(int from, int limit) {
        return IntStream.range(from, limit)
                .boxed()
                .collect(Collectors.toList());
    }


    public Double getTotalResult(Integer startYear, Integer endYear, Connection conn, Geometry geom, String aggregationType){


        // hard coded range for year
        if (startYear < 1900){
            startYear = 1990;
        }

        if (endYear > 2018){
            endYear = 2018;
        }


        // init variables
        List<Double> resultList = new ArrayList<>();

        // create range
        List<Integer> range = createPrecipitationRange(startYear, endYear);
        Double totalResult;



        // calculate for each year
        for (Integer year : range){
            String query = preparePrecipitationQuery(geom, year, aggregationType);
            resultList.add(getValuesFromRaster(query, conn));
        }


        // calculates end result
        totalResult = resultList.stream().mapToDouble(val -> val).average().orElse(0.0);

        return totalResult;

    }


    public boolean isInsideRoutingArea(Connection conn, Point point){
        boolean result = false;


        Point internal = point;

        if (!isWGS84(internal)){
            internal = (Point) transformSpGeom(point,4326);
        }


        String query = "SELECT ST_Contains(geom, st_geomfromtext('" + internal.toText() + "', 4326))  FROM routing.validarea;";


        try (Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(query)){

            while (rs.next()) {
                result = rs.getBoolean(1);
            }

        } catch (SQLException e) {
            //todo logger
            e.printStackTrace();
        }


        return result;
    }

}


