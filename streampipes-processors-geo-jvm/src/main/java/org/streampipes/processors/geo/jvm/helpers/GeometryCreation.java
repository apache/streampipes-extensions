package org.streampipes.processors.geo.jvm.helpers;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.operation.buffer.BufferOp;
import org.locationtech.jts.operation.buffer.BufferParameters;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.opengis.referencing.FactoryException;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.MathTransform;
import org.opengis.referencing.operation.TransformException;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.util.ArrayList;
import java.util.Collection;


public class GeometryCreation {



    public enum GeometryTypes {
        POINT (1), LINESTRING (2), POLYGON(3), MULTIPOINT(4), MULTILINESTRING(5), MULTIPOLYGON (6), GEOMETRYCOLLECTION (7);

        private int number;

        GeometryTypes(int number) {
            this.number = number;
        }

        public int getNumber() {
            return number;
        }
    }



    //======================== main methods for creation

    /**
     * creates a Geometry from a wkt_string. string has to be valid and is not be checked. If invalid, an empty point
     * geom is returned. method calls getPrecision method and creates a jts geometry factory and a WKT-parser object.
     * from the wktString the
     *
     * @param wktString
     * @param epsgCode
     * @return returnedGeom: a geometry object with spec srid and prec
     */
    public static Geometry createSPGeom(String wktString, Integer epsgCode) {

        Geometry returnedGeom = null;
        PrecisionModel prec = getPrecisionModel(epsgCode);

        GeometryFactory geomFactory = new GeometryFactory(prec, epsgCode);
        WKTReader wktReader = new WKTReader(geomFactory);

        try {
            returnedGeom = wktReader.read(wktString);
        } catch (ParseException e) {
            // if wktString is invalid, an empty point geometry will be created as returnedGeom
            returnedGeom = geomFactory.createPoint();
        }

        return returnedGeom;
    }


    /**
     * creates a new geometry object instance from the input geom with own srid and prec model, depending on the epsgCode.
     * This method should be used if a new geometry is created from another geometry. e.g. create centroid point from geometry.
     * Problem is, if return value of a method is a geometry, it comes without prec and srid info. To get sure all necessary
     * values are set, call this method
     *
     * @param geom plain geometry object without srid and prec info
     * @return geometry object with srid and prec info
     * @author GIFlo
     * @version 1.0
     */
    public static Geometry createSPGeom(Geometry geom, Integer epsgCode) {

        //init geom
        Geometry returnedGeom = null;

        //gets precision model from getPrecisionModel method
        PrecisionModel prec = getPrecisionModel(epsgCode);

        //creates the factory object
        GeometryFactory geomFactory = new GeometryFactory(prec, epsgCode);

        // creates the new geom from the input geom. precision and srid will be calculated above and will be set in the new geom
        returnedGeom = geomFactory.createGeometry(geom);

        return returnedGeom;
    }


    /**
     * creates a point geometry from lat lng values
     *
     * @param lat double values, representing the Latitude value
     * @param lng double values, representing the Longitude value
     * @param epsgCode Defines the EPSG code
     * @return a point geometry in the main class geometry
     */
    public static Geometry createSPGeom(Double lng, Double lat, Integer epsgCode) {

        Geometry point;
        PrecisionModel prec = getPrecisionModel(epsgCode);

        GeometryFactory geomFactory = new GeometryFactory(prec, epsgCode);


        Coordinate coords = new Coordinate(lng, lat);
        point = geomFactory.createPoint(coords);


        return point;
    }

    /**
     * returns, depending on the geometry type, an empty geometry. This method should be used inside exceptions handling
     *
     * @param geom
     * @return
     * @author GIFlo
     */
    public static Geometry createEmptyGeometry(Geometry geom) {

        //calling the factory from the input geom, it is not necessary to add epsg and prec because this is automatically
        // used from geom
        Geometry outputGeom = null;

        if (geom instanceof Point) {
            outputGeom = geom.getFactory().createPoint();
        } else if (geom instanceof LineString) {
            outputGeom = geom.getFactory().createLineString();
        } else if (geom instanceof Polygon) {
            outputGeom = geom.getFactory().createPolygon();
        } else if (geom instanceof MultiPoint) {
            outputGeom = geom.getFactory().createMultiPoint();
        } else if (geom instanceof MultiLineString) {
            outputGeom = geom.getFactory().createMultiLineString();
        } else if (geom instanceof MultiPolygon) {
            outputGeom = geom.getFactory().createMultiPolygon();
        } else {
            outputGeom = geom.getFactory().createGeometryCollection();
        }
        return outputGeom;
    }




    // ======================= Projections and helpers
    /**
     * Coordinate transformation to targetEPSG code
     *
     * @param geom       input geom that should be transformed
     * @param targetEPSG target epsg code
     * @return returns the reprojected geometry object with srid and correct prec model. If transformation
     * goes wrong, an empty geometry object will be returned
     * @author GIFlo
     * @version 1.0
     */
    public static Geometry transformSpGeom(Geometry geom, Integer targetEPSG) {

        Geometry reproj_geom = null;

        CoordinateReferenceSystem targetCRS = getCRS(targetEPSG);
        CoordinateReferenceSystem sourceCRS = getCRS(geom.getSRID());


        try {
            // calculates the math for reprojection
            MathTransform transform = CRS.findMathTransform(sourceCRS, targetCRS);

            //creates transformed geom. calls createSpGeom method -> input geom is the casted JTS geom.
            // transformation is done with the transform method
            reproj_geom = createSPGeom(JTS.transform(geom, transform), targetEPSG);
//            reproj_geom = JTS.transform(geom, transform);


        } catch (FactoryException | TransformException e) {
            // returns an empty geometry
            //todo logger reprojection went wrong and empty geometry is build
            reproj_geom = createEmptyGeometry(geom);
            e.printStackTrace();
        }

        return reproj_geom;
    }


    /**
     *
     * @param geomA
     * @param geomB
     * @param getEpsgFromA_ReturnsB
     * @return
     */
    public static Geometry unifyEPSG(Geometry geomA, Geometry geomB, boolean getEpsgFromA_ReturnsB){

        Geometry tempGeomA =  geomA;
        Geometry tempGeomB = geomB;


        if (geomA.getSRID() != geomB.getSRID()){
            if (getEpsgFromA_ReturnsB){
                tempGeomB = transformSpGeom(geomB, geomA.getSRID());
            } else {
                tempGeomA = transformSpGeom(geomA, geomB.getSRID());
            }

        }

        // output
        if (getEpsgFromA_ReturnsB){
            return tempGeomA;
        } else {
            return tempGeomB;
        }
    }



    /**
     * checks if the input epsgCode is in Meter Unit
     *
     * @param epsgCode
     * @return true if crs uses meter as unit
     */
    public static boolean isMeterCRS(int epsgCode) {
        boolean result = false;

        if (getCrsUnit(epsgCode).equals("m")) {
            result = true;
        }

        return result;
    }

    /**
     * WGS84 (4326) checker
     * @param geom input geometry to check with
     * @return true if geom has epsg 4632, false if other
     */
    public  static boolean isWGS84(Geometry geom) {
        return geom.getSRID() == 4326;
    }



    /**
     * creates internal an CoordinateReferenceSystem object and reads out the unit
     *
     * @param epsgCode integer unit epsgCode
     * @return String value of the unit (e.g. m vor meter or degree for Degree
     * @author GIFlo
     */
    public static String getCrsUnit(int epsgCode) {

        String output = null;

        CoordinateReferenceSystem crs = getCRS(epsgCode);
        output = crs.getCoordinateSystem().getAxis(0).getUnit().toString();

        return output;
    }


    /**
     * @param epsgCode
     * @return
     */
    public static PrecisionModel getPrecisionModel(Integer epsgCode) {

        PrecisionModel prec = null;

        //creates geometry factory for result
        if (getCrsUnit(epsgCode).equals("m")) {
            prec = new PrecisionModel(1);
        } else {
            prec = new PrecisionModel(1000000);
        }

        return prec;
    }


    /**
     * @param epsgCode
     * @return
     * @throws FactoryException
     */
    public static boolean checkLongitudeFirst(int epsgCode) throws FactoryException {


        CRS.AxisOrder axisCheck = CRS.getAxisOrder(CRS.decode("EPSG:" + epsgCode));

        return axisCheck.toString().equals("NORTH_EAST");

    }



    /**
     * @param point
     * @return epsg code from the WGS84 utmZone
     */
    public static int findWgsUtm_EPSG(Point point) {
        double lon = point.getX();
        double lat = point.getY();

        Integer zone;
        Integer epsg;
        Integer hemisphere;

        zone = (int) Math.floor(lon / 6 + 31);

        if ((lat > 55) && (zone == 31) && (lat < 64) && (lon > 2)) {
            zone = 32;
        } else if ((lat > 71) && (zone == 32) && (lon < 9)) {
            zone = 31;
        } else if ((lat > 71) && (zone == 32) && (lon > 8)) {
            zone = 33;
        } else if ((lat > 71) && (zone == 34) && (lon < 21)) {
            zone = 33;
        } else if ((lat > 71) && (zone == 34) && (lon > 20)) {
            zone = 35;
        } else if ((lat > 71) && (zone == 36) && (lon < 33)) {
            zone = 35;
        } else if ((lat > 71) && (zone == 36) && (lon > 32)) {
            zone = 37;
        }


        if (lat < 0) {
            hemisphere = 7;
        } else {
            hemisphere = 6;
        }

        //concatenate integer values
        epsg = Integer.valueOf(String.valueOf(32) + hemisphere + zone);
        return epsg;
    }



    /**
     * Creates an CoordinateReferenceSystem (CRS) from the @param epsgGode. If the
     * code is invalid and throws a FactoryException, warning appears and standard CRS WGS84 4326 is returned.
     * If still an exception occurs check the maven repro and add 'gt-epsg-wkt' and 'gt-epsg-hsql'
     *
     * @param epsgCode
     * @return output is a valid CRS Object or the Streampipes standard EPSG 4326. Attention: Using the wrong EPSG Code
     * can lead to weirds and nonsens geometry results!
     * @author GIFlo
     */
    public static CoordinateReferenceSystem getCRS(int epsgCode) {
        CoordinateReferenceSystem output = null;

        try {
            output = CRS.decode("EPSG:" + epsgCode, checkLongitudeFirst(epsgCode));

        } catch (FactoryException e) {
            //todo create log warning
            try {
                output = CRS.decode("EPSG:4326", true);
            } catch (FactoryException e1) {
                // todo log wrong setting. maven repro is missing
                e1.printStackTrace();
            }
        }

        return output;
    }




    /**
     * Extract single geometries from an aggregated geometry class
     * @param geom Multipoint geometry to extract from
     * @return Collection with Point geometries
     */
    public static Collection<Point> multiToSingle(MultiPoint geom) {
        int size = geom.getNumGeometries();

        Collection<Point> singlePoints = new ArrayList<>();


        for (int i = 0; i <= size - 1; i++) {
            Point single = (Point) createSPGeom(geom.getGeometryN(i), geom.getSRID());
            singlePoints.add(single);
        }

        return singlePoints;
    }


    /**
     * Extract single geometries from an aggregated geometry class
     * @param geom MultiLineString geometry to extract from
     * @return Collection with LineString geometries
     */
    public static Collection<LineString> multiToSingle(MultiLineString geom) {
        int size = geom.getNumGeometries();

        Collection<LineString> singleLine = new ArrayList<>();

        for (int i = 0; i <= size - 1; i++) {
            LineString single = (LineString) createSPGeom(geom.getGeometryN(i), geom.getSRID());
            singleLine.add(single);
        }

        return singleLine;
    }


    /**
     * Extract single geometries from an aggregated geometry class
     * @param geom MultiPolygon geometry to extract from
     * @return Collection with Polygons geometries
     */
    public static Collection<Polygon> multiToSingle(MultiPolygon geom) {
        int size = geom.getNumGeometries();

        Collection<Polygon> singlePoly = new ArrayList<>();

        for (int i = 0; i <= size - 1; i++) {
            Polygon single = (Polygon) createSPGeom(geom.getGeometryN(i), geom.getSRID());
            singlePoly.add(single);
        }

        return singlePoly;
    }


    /**
     * Extract single geometries from an aggregated geometry class
     * @param geom GeometryCollection geometry to extract from
     * @return Collection with Geometry class
     *
     * The Collection can extract from "nested level 1" aggregated Geometries.
     * This means a Collection can have a MultiLineString Geometry, which will be extracted to single
     *
     *
     * The Collection with a "nested level 2" or higher can't be extracted.
     * It will not tested if this is the case and a correct result can't be guaranteed
     *
     */
    public static Collection<Geometry> multiToSingle(GeometryCollection geom) {
        int size = geom.getNumGeometries();

        Collection<Geometry> singleGeom = new ArrayList<>();

        for (int i = 0; i <= size - 1; i++) {

            if (geom instanceof MultiPoint) {
                singleGeom.addAll(multiToSingle((MultiPoint) geom));
            } else if (geom instanceof MultiLineString) {
                singleGeom.addAll(multiToSingle((MultiLineString) geom));
            } else if (geom instanceof MultiPolygon) {
                singleGeom.addAll(multiToSingle((MultiPolygon) geom));
            } else {
                singleGeom.add(createSPGeom(geom.getGeometryN(i), geom.getSRID()));

            }
        }

        return singleGeom;
    }


    /**
     * creates a MultiPoint geometry from a Collection with point geometries
     *
     * @param points Collection of Point geometries. It will be assumed that all points have the same epsg code
     *               and the epsg code from the first geometry will be used as base epsg code.
     *               An empty collection leeds to an empty multipoint output. A collection size greater than zero will be
     *               stored in the MultiPoint geometry.
     *
     * @return a Multipoint geometry.
     */
    public static MultiPoint createSPMultiPoint(Collection<Point> points) {


        if (points.size() == 0) {
            //empty collection returns empty MultiPoint

            GeometryFactory geomFactory = new GeometryFactory();
            MultiPoint fail = geomFactory.createMultiPoint();

            return fail;
        }

        // it will be assumed that every line has a valid epsg and that all points have the same epsg
        int epsgCode = points.iterator().next().getSRID();
        PrecisionModel prec = getPrecisionModel(epsgCode);
        GeometryFactory geomFactory = new GeometryFactory(prec, epsgCode);


        MultiPoint result = null;

        if (points.size() == 1) {
            //only one point inside collection
            result = convertToMultiFromSingle(points.iterator().next());

        } else {

            result = (MultiPoint) geomFactory.buildGeometry(points);


        }

        return result;
    }


    /**
     * creates a MultiLine geometry from a Collection with LineString geometries
     *
     * @param lines Collection of LineString geometries. It will be assumed that all LineStrings have the same epsg code
     *              and the epsg code from the first geometry will be used as base epsg code.
     *              An empty collection leeds to an empty multipoint output. A collection size greater than zero will be
     *              stored in the MultiPoint geometry.
     * @param mergeAtEndpoints Option to "merged" or join LineStrings to one LineString if start and endpoint of
     *                         the two LineString are the same. This additional operation take only effect
     *                         topological graph nodes with the dimension of 2.
     *                         Lines containing other lines will be not nested so far.
     * @return a MultiLineString geometry
     */
    public static MultiLineString createSPMultiLineString(Collection<LineString> lines, boolean mergeAtEndpoints) {


        // if empty collection is returned
        if (lines.size() == 0) {
            //empty collection returns empty MultiLineString
            GeometryFactory geomFactory = new GeometryFactory();
            MultiLineString fail = geomFactory.createMultiLineString();

            return fail;
        }

        //it will be assumed that all lines have the same epsg code

        int epsgCode = lines.iterator().next().getSRID();
        PrecisionModel prec = getPrecisionModel(epsgCode);
        //creates the factory object
        GeometryFactory geomFactory = new GeometryFactory(prec, epsgCode);

        MultiLineString result = null;


        if (lines.size() == 1) {
            //only one line inside collection
            result = convertToMultiFromSingle(lines.iterator().next());

        } else {
            // main task

            result = (MultiLineString) geomFactory.buildGeometry(lines);


            //mergedOptions
            if (mergeAtEndpoints) {

                //creates object from LineMerger
                LineMerger merger = new LineMerger();
                merger.add(result);
                //returns a collection
                Collection<LineString> temp = merger.getMergedLineStrings();

                //if due merging only one linestring is inside collection then convert single to multi
                if (temp.size() == 1) {
                    result = convertToMultiFromSingle(temp.iterator().next());
                } else {
                    //mergedResult is passed into result
                    result = (MultiLineString) geomFactory.buildGeometry(temp);

                }

            }
        }

        return result;
    }


    /**
     *
     * @param polygons
     * @param dissolveResult
     * @return
     */
    public static MultiPolygon createSPMultiPolygon(Collection<Polygon> polygons, boolean dissolveResult) {


        // if empty collection is returned
        if (polygons.size() == 0) {
            //empty collection returns empty MultiPolygon

            GeometryFactory geomFactory = new GeometryFactory();
            MultiPolygon fail = geomFactory.createMultiPolygon();

            return fail;
        }

        //it will be assumed that all polygons have the same epsg code

        int epsgCode = polygons.iterator().next().getSRID();
        PrecisionModel prec = getPrecisionModel(epsgCode);
        //creates the factory object
        GeometryFactory geomFactory = new GeometryFactory(prec, epsgCode);

        MultiPolygon result = null;

        if (polygons.size() == 1) {
            //only one line inside collection
            result = convertToMultiFromSingle(polygons.iterator().next());

        } else {
            // main task

            result = (MultiPolygon) geomFactory.buildGeometry(polygons);


            //mergedOptions
            if (dissolveResult) {

                try {
                    //merge via zero bufferOperator
                    //result = (MultiPolygon) result.bufferOperator(0);
                    result = (MultiPolygon) result.union();
                    // there will be an exception if result or merge is a single polygon
                } catch (ClassCastException e) {

                    // then result of bufferOperator will be a polygon and then converted
                    //Polygon single = (Polygon) result.bufferOperator(0);
                    Polygon single = (Polygon) result.union();
                    result = convertToMultiFromSingle(single);
                }

            }


        }


        return result;
    }


    /**
     * Converts a single point into a multipoint with only one value.
     * This is not  possible via basic cast. So "dirty" string manipulation.
     * Another method would be to read out the coordinate sequenz and create a
     * geometry via factoryBuilder.
     *
     * This method should be deprecated soon
     *
     *
     * @param geom Point geometry
     * @return a MultiPoint with same prec and epsg as input
     */
    public static MultiPoint convertToMultiFromSingle(Point geom) {

        String wkt = geom.toText();

        wkt = wkt.replaceFirst("\\bPOINT\\b", "MULTIPOINT");
        wkt = wkt.replaceFirst("\\(", "((");
        wkt = wkt.replaceFirst("\\)", "))");


        MultiPoint result = (MultiPoint) createSPGeom(wkt, geom.getSRID());

        return result;
    }



    /**
     * Converts a single linestring into a MultiLineString with only one value.
     * This is not  possible via basic cast. So "dirty" string manipulation.
     * Another method would be to read out the coordinate sequenz and create a
     * geometry via factoryBuilder.
     *
     * This method should be deprecated soon
     *
     *
     * @param geom LineString geometry
     * @return a MultiLineString with same prec and epsg as input
     */
    public static MultiLineString convertToMultiFromSingle(LineString geom) {

        String wkt = geom.toText();

        wkt = wkt.replaceFirst("\\bLINESTRING\\b", "MULTILINESTRING");
        wkt = wkt.replaceFirst("\\(", "((");
        wkt = wkt.replaceFirst("\\)", "))");


        MultiLineString result = (MultiLineString) createSPGeom(wkt, geom.getSRID());

        return result;
    }


    /**
     * Converts a single polygon into a MultiPolygon with only one value.
     * This is not  possible via basic cast. So "dirty" string manipulation.
     * Another method would be to read out the coordinate sequenz and create a
     * geometry via factoryBuilder.
     *
     * This method should be deprecated soon
     *
     *
     * @param geom LineString geometry
     * @return a MultiLineString with same prec and epsg as input
     */
    public static MultiPolygon convertToMultiFromSingle(Polygon geom) {

        String wkt = geom.toText();

        wkt = wkt.replaceFirst("\\bPOLYGON\\b", "MULTIPOLYGON");
        wkt = wkt.replaceFirst("\\(", "((");
        wkt = wkt.replaceFirst("\\)", "))");


        MultiPolygon result = (MultiPolygon) createSPGeom(wkt, geom.getSRID());

        return result;
    }


    /**
     * gives output how many single geometries are inside a collection
     * @param geom input geometry collection
     * @param dimOutput int values allowed 0 (Point), 1 (Line), 2 (Polygon)
     * @return amount of geometries depending on dimOutput parameter
     */
    public static Integer countSingleGeometriesInsideCollection(GeometryCollection geom, int dimOutput) {

        // geometry for iterator
        Geometry internal = null;

        int countPoint = 0;
        int countLine = 0;
        int countPoly = 0;



        //valid options are 0, 1 and 2, if outside it will be set to nearest valid dim
        if (dimOutput < 0) {
            dimOutput = 0;
        } else if (dimOutput > 2) {
            dimOutput = 2;
        }


        //get size of geometries inside collection
        int size = geom.getNumGeometries();


        for (int i = 0; i <= size - 1; i++) {

            internal = geom.getGeometryN(i);

            if (internal.getDimension() == 0) {
                if (internal instanceof MultiPoint) {
                    countPoint = countPoint + multiToSingle((MultiPoint) internal).size();
                } else {
                    countPoint = countPoint + 1;
                }
            } else if (internal.getDimension() == 1) {
                if (internal instanceof MultiLineString) {
                    countLine = countLine + multiToSingle((MultiLineString) internal).size();
                } else {
                    countLine = countLine + 1;
                }
            } else {
                if (internal instanceof MultiPolygon) {
                    countPoly = countPoly + multiToSingle((MultiPolygon) internal).size();
                } else {
                    countPoly = countPoly + 1;
                }
            }
        }


        // controls output dependent of chosen output operation
        if (dimOutput == 0) {
            return countPoint;
        } else if (dimOutput == 1) {
            return countLine;
        } else {
            return countPoly;
        }

    }




    // ================= other helpers


    /**
     * extract a single point from input geometry. If point, a referenced point geometry i8s returned.
     * If Linestring the interior point of the linestring is returned. If polygon or any kind of MultiGeometry the
     * centroid point of the object will be returned.
     *
     * This method is mainly to find correct utm epsg from extracting single point
     *
     * @param geom
     * @return a geometry object from type point
     */
    public static Point extractPoint(Geometry geom) {
        Point returnedPoint = null;

        Integer epsgCode = geom.getSRID();

        if (geom instanceof Point) {
            // get y lng and x lat coords from point. has to be casted because input is basic geometry
            returnedPoint = (Point) geom;

        } else if (geom instanceof LineString) {
            // cast geometry to line and calculates the centroid to get point
            returnedPoint = (Point) createSPGeom((geom).getInteriorPoint(), epsgCode);
        } else {
            returnedPoint = (Point) createSPGeom(geom.getCentroid(), epsgCode);
        }


        return returnedPoint;
    }


    /**
     * Checks if the input geometry is from type Point
     * @param geom
     * @return true if geometry is a point, false if other geometry
     */
    public  static boolean isPoint(Geometry geom){
        boolean isPoint = false;

        if (geom instanceof Point){
            isPoint = true;
        }
        return isPoint;
    }




    /**
     * creates a bufferOperator around the input geometry. @see BufferParameters has to be set outside the
     * method first and then used as @param bufferParameter. valid options are:
     * BufferParameters params = new BufferParameters();
     * params.setQuadrantSegments(Default 8);
     * params.setSimplifyFactor(Default 0.01);
     * params.setMitreLimit(Default 5);
     * params.setJoinStyle(1 (Round) ,2 (Mitre), 3 (Bevel));
     * params.setEndCapStyle(1 (Round), 2 (Flat), 3 (Square));
     * params.setSingleSided(true, false)
     * if true you can have left and right side. for left use negative distance, for right normal distance
     * capStyle will be ignored and forced to flat
     * <p>
     * return value should be a valid polygon.
     * It can be empty if point and capStyle flat is used --> console info output
     * It can also be empty if polygon with hole is used and singleSided left with to high distance is used.
     * (distance is higher than space between outside and inside POLYGON boundary) --> console info output
     * If Points are inside GeometryCollection and options above is used, points will not
     * be part of the output --> console info output
     *
     * @param geom            input geometry
     * @param distance        distance in meter to calculate bufferOperator
     * @param bufferParameter BufferParameters Class with set params
     * @return a Polygon of class Geometry
     */
    public static Geometry createSpBuffer(Geometry geom, Double distance, BufferParameters bufferParameter) {

        // internal is necessary otherwise global geometry will be transformed
        Geometry internal = geom;
        Geometry result = null;


        //if epsg is non metric, it will be transformed to corresponding utm zone
        if (!isMeterCRS(geom.getSRID())) {
            internal = transformSpGeom(internal, findWgsUtm_EPSG(extractPoint(internal)));
        }

        //using bufferOP with input geometry (internal) distance and Parameter)
        result = createSPGeom(BufferOp.bufferOp(internal, distance, bufferParameter), internal.getSRID());


        // if geometry wasn't metric, transformation took place. to get the result in the same coordinate system like input, it will
        // be back transformed
        if (result.getSRID() != geom.getSRID()) {
            result = transformSpGeom(result, geom.getSRID());
        }

        //========VALIDATE RESULT
        if (result.isEmpty()) {
            //todo logger
            System.out.println("Result from bufferOperator is empty. reasons e.g polygon with hole and wrong and too " +
                    "high side distance or ");
        }

        if (geom.getGeometryType()== "GeometryCollection") {

            if ((countSingleGeometriesInsideCollection((GeometryCollection) geom, 0) > 0) & bufferParameter.getEndCapStyle() == 2 | bufferParameter.isSingleSided()) {
                //todo logger
                System.out.println("points inside collections got lost during bufferCreating because of chosen Option capStyle 'flat' or singleSided 'true'");
            }
        }

        return result;
    }


    /**
     * Creates buffer from input geometry
     *
     *
     * @param geom geometry input
     * @param distance distance parameter for buffer building
     * @param endCapStyle defines end cap style
     * @param joinStyle defines join  style value
     * @param mitreLimit defines mitre limit value, takes effect when mitre join style is selected
     * @param segment defines segmentQuadrant value
     * @param simplifyFactor defines simplify factor value
     * @param singleSided boolean true is single sided, false is both sided
     * @param side int value for side choice @see BufferSide enum. the distance has to be multiplied my this value but exclude 0 value (both side)
     * @return
     */
    public static Geometry createSpBuffer(Geometry geom, Double distance, int endCapStyle, int joinStyle, double mitreLimit, int segment, double simplifyFactor, boolean singleSided, int side ) {

        // internal is necessary otherwise global geometry will be transformed
        Geometry internal = geom;
        Geometry result = null;


        BufferParameters bufferParameter = new BufferParameters(); //init
        bufferParameter.setEndCapStyle(endCapStyle);
        bufferParameter.setJoinStyle(joinStyle);
        bufferParameter.setMitreLimit(mitreLimit);
        bufferParameter.setQuadrantSegments(segment);
        bufferParameter.setSimplifyFactor(simplifyFactor);
        bufferParameter.setSingleSided(singleSided);


        //
        if(side != 0){
            distance = distance * side;
        }


        //if epsg is non metric, it will be transformed to corresponding utm zone
        if (!isMeterCRS(geom.getSRID())) {
            internal = transformSpGeom(internal, findWgsUtm_EPSG(extractPoint(internal)));
        }

        //using bufferOP with input geometry (internal) distance and Parameter)
        result = createSPGeom(BufferOp.bufferOp(internal, distance, bufferParameter), internal.getSRID());


        // if geometry wasn't metric, transformation took place. to get the result in the same coordinate system like input, it will
        // be back transformed
        if (result.getSRID() != geom.getSRID()) {
            result = transformSpGeom(result, geom.getSRID());
        }

        //========VALIDATE RESULT
        if (result.isEmpty()) {
            //todo logger
            System.out.println("Result from bufferOperator is empty. reasons e.g polygon with hole and wrong and too " +
                    "high side distance or ");
        }

        if (geom.getGeometryType()== "GeometryCollection") {

            if ((countSingleGeometriesInsideCollection((GeometryCollection) geom, 0) > 0) & bufferParameter.getEndCapStyle() == 2 | bufferParameter.isSingleSided()) {
                //todo logger
                System.out.println("points inside collections got lost during bufferCreating because of chosen Option capStyle 'flat' or singleSided 'true'");
            }
        }

        return result;
    }



    /**
     *
     *creates a bufferOperator around the input geometry. @see BufferParameters has to be set outside the
     *method first and then used as @param bufferParameter. valid options are:
     *BufferParameters params = new BufferParameters();
     *params.setQuadrantSegments(Default 8);
     *params.setSimplifyFactor(Default 0.01);
     * params.setEndCapStyle(1 (Round), 2 (Flat), 3 (Square));
     * Internal the geometry is reprojected to corresponding utm zone to calculate the distance in a metric system
     *
     * @param geom Point geometry
     * @param distance distance of the created buffer area
     * @param capStyle set capStyle of the buffer geometry
     * @param segments information about the amount of the segments
     * @param simplifyFactor simplify factor
     * @return A Polygon in the CRS of the input geometry.
     */
    public static Polygon createSpBuffer(Point geom, double distance, int capStyle, int segments, double simplifyFactor ){

        Point geomInternal = geom;
        Polygon buffer_geom = null;


        //if capStyle is flat it will be forced to be round
        if (capStyle == 2){
            capStyle = 1;
        }


        // transform to metric coordinate system
        if (!isMeterCRS(geom.getSRID())) {
            geomInternal = (Point) transformSpGeom(geom, findWgsUtm_EPSG(geom));
        }


        //creates buffer params
        BufferParameters bufferParam = new BufferParameters(); //init
        bufferParam.setEndCapStyle(capStyle);
        bufferParam.setQuadrantSegments(segments);
        bufferParam.setSimplifyFactor(simplifyFactor);


        buffer_geom = (Polygon) createSPGeom(BufferOp.bufferOp(geomInternal, distance, bufferParam), geomInternal.getSRID());


        if (buffer_geom.getSRID() != geom.getSRID()) {
            buffer_geom = (Polygon) transformSpGeom(buffer_geom, geom.getSRID());
        }


        return buffer_geom;
    }


    /**
     *
     * Creates a simplicity checker using the default SFS Mod-2 Boundary Node Rule
     * @param geom
     * @return
     */
    public static boolean isSimplePolygon (Polygon geom){
        boolean isSimple = false;

        if (geom.isSimple()){
            isSimple = true;
        }

        return isSimple;
    }


    /**
     * transform the double value to a String depending on the {@link DecimalFormat} pattern.
     * Max 10 decimal positions are defined. negative values are interpreted as 4 digits.
     *
     * @param value
     * @param decimalPositions
     * @return
     */
    public static  String doubleToString(Double value, int decimalPositions){


        //negative values should not be possible
        if (decimalPositions <0){
            decimalPositions = 3;
        }
        // setup the decimal format to prevent scientific format style
        DecimalFormat df = new DecimalFormat();

        // transform the decimal
        DecimalFormatSymbols dfs = new DecimalFormatSymbols();
        // uses always the . instead of , for decimal separator
        dfs.setDecimalSeparator('.');
        df.setDecimalFormatSymbols(dfs);
        df.setGroupingUsed(false);

        // if decimal position higher than 0 is chosen, decimal Pos will be set. if negative value all
        // calc decimal position will be used
        if (decimalPositions >=0 ){
            df.setMaximumFractionDigits(decimalPositions);
        }

        //writes the result into a String to parse this into the stream. Cannot be parsed as a Double Otherwise scientific style comes back
        String result = df.format(value);

        return result;
    }


    /**
     * The centroid is equal to the centroid of the set of component Geometries of highest dimension
     * (since the lower-dimension geometries contribute zero "weight" to the centroid).
     *
     * @param geom
     * @return
     */
    public static Point getCentroidSp(Polygon geom){

        Point centroid = (Point) createSPGeom(geom.getCentroid(), geom.getSRID());

        return centroid;

    }


    public static Point getCentroidSp(MultiPolygon geom){

        Point centroid = (Point) createSPGeom(geom.getCentroid(), geom.getSRID());
        return centroid;

    }

    /**
     * An interior point is guaranteed to lie in the interior of the Geometry, if it possible to calculate such a
     * point exactly. Otherwise, the point may lie on the boundary of the geometry.
     *
     * @param geom LineString geom
     * @return
     */
    public static Point getInteriorSp(LineString geom){

        Point interior = (Point) createSPGeom(geom.getInteriorPoint(), geom.getSRID());

        return interior;
    }




}
