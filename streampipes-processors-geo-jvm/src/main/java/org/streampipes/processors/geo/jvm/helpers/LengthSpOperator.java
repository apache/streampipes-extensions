package org.streampipes.processors.geo.jvm.helpers;


import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.jts.algorithm.distance.DiscreteHausdorffDistance;
import org.locationtech.jts.geom.*;
import org.locationtech.jts.operation.distance.DistanceOp;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import si.uom.SI;
import tec.uom.se.quantity.Quantities;
import tec.uom.se.unit.MetricPrefix;

import javax.measure.Quantity;
import javax.measure.Unit;
import javax.measure.quantity.Length;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public class LengthSpOperator {


    // Units for Length

    private final Unit<Length> M = SI.METRE;
    private final Unit<Length> KM = MetricPrefix.KILO(SI.METRE);
    private final Unit<Length> MILE = SI.METRE.multiply(1609344).divide(1000);
    private final Unit<Length> FOOT= SI.METRE.multiply(3048).divide(10000);


    /**
     * enum list with valid length units. extraction can be due name or int number (getNumber)
     */
    public enum ValidLengthUnits {
        meter (1), km (2), mile (3), foot (4);


        private final int number;


        ValidLengthUnits(int number) {
            this.number = number;

        }

        public int getNumber() {
            return number;
        }
    }



//    private double length;
    private Integer decimalPosition;
    private Quantity<Length> length;



    // ========================= constructor
    public LengthSpOperator(Quantity<Length> length , int decimalPosition) {
        this.length = length;
        this.decimalPosition = decimalPosition;
    }


    public LengthSpOperator(int decimalPosition){
        this.length = Quantities.getQuantity(-9999d, M);
        this.decimalPosition = decimalPosition;
    }



    // ========================== getter

    /**
     * getter method for variable decimalPosition
     * @return decimalPosition
     */
    public int getDecimalPosition() {
        return decimalPosition;
    }



    public String getLengthAsString(){
        String result = doubleToString( getLengthValue(), getDecimalPosition());
        return result;
    }


    /**
     * getter method for variable length
     * @return Double length value
     */
    public Double getLengthValue() {
        return (length.getValue()).doubleValue();
    }

    /**
     * getter method for unit as a String
     * @return String unit string
     */
    public String getLengthUnit() {
        return length.getUnit().toString();
    }

    /**
     * getter method for unit
     * @return Unit<Length> unit
     */
    public Unit<Length> getUnit(){
        return length.getUnit();

    }


    // ========================== setter


    /**
     * setter method to set Quantity<Length>
     * @param length  Quantity<Length> value
     */
    private void setLength(Quantity<Length> length) {
        this.length = length;
    }



    /**
     * setter method to set decimalPosition variable
     * @param decimalPosition set the value for decimal position
     */
    public void setDecimalPosition(int decimalPosition) {
        this.decimalPosition = decimalPosition;
    }


    /**
     * setter method to set Quantity<Length> value
     * @param value double length value
     * @param unit Unit<length> value //todo use only final unit values
     */
    private void setLength(double value, Unit<Length> unit) {
        this.length = Quantities.getQuantity(value, unit);
    }




    // Methoden unit transformation


    /**
     * convert  Quantity<Length>  value to Unit<Length>
     * @param value choose target Unit<Length> from enum @see ValidLengthUnits
     */
    public void convertUnit (ValidLengthUnits value) {
        switch (value.getNumber()) {
            case 1:
                setLength(this.length.to(M));
                break;
            case 2:
                setLength(this.length.to(KM));
                break;
            case 3:
                setLength(this.length.to(MILE));
                break;
            case 4:
                setLength(this.length.to(FOOT));
                break;
        }
    }

    /**
     * convert  Quantity<Length>  value to Unit<Length>
     * @param value choose int value corresponding to ValidLengthUnits
     */
    public void convertUnit (int value) {
        switch (value) {
            case 1:
                setLength(this.length.to(M));
                break;
            case 2:
                setLength(this.length.to(KM));
                break;
            case 3:
                setLength(this.length.to(MILE));
                break;
            case 4:
                setLength(this.length.to(FOOT));
                break;
        }
    }


    /**
     * calculates the length of a line.
     * If non metric CRS, line will be reprojected to corresponding utm zone, due using the interior point of the line.
     * @param geom LineString geom
     */
    public void calcLength(LineString geom){

        Double calculatedLength = null;
        LineString geomInternal = geom;


        if (!isMeterCRS(geom.getSRID())){
            geomInternal = (LineString) transformSpGeom(geomInternal, findWgsUtm_EPSG(extractPoint(geom)));
        }

        calculatedLength = geomInternal.getLength();

        setLength(calculatedLength, M);
    }


    /**
     * calculates the length of a line.
     * If non metric CRS, line will be reprojected to corresponding utm zone, due using the interior point of the line.
     * @param geom MultiLineString. The total length of all LineStrings will be calculated.
     */
    public void calcLength(MultiLineString geom){

        Double calculatedLength = null;
        MultiLineString geomInternal = geom;

        if (!isMeterCRS(geom.getSRID())){
            geomInternal = (MultiLineString) transformSpGeom(geomInternal, findWgsUtm_EPSG(extractPoint(geom)));
        }

        calculatedLength = geomInternal.getLength();

        setLength(calculatedLength, M);
    }



    /**
     * calculates the polygonPerimeterCalc of a Polygon
     * @param geom Polygon geom
     */
    public void calcPerimeter(Polygon geom){
        Double perimeter = null;
        Polygon geomInternal = geom;

        if (!isMeterCRS(geom.getSRID())){
            geomInternal = (Polygon) transformSpGeom(geomInternal, findWgsUtm_EPSG(extractPoint(geom)));
        }

        perimeter = geomInternal.getLength();

        setLength(perimeter, M);

    }

    /**
     * calculates the polygonPerimeterCalc of a MultiPolygon
     * @param geom MultiPolygon geom. Total length of all polygons will be calculated
     */
    public void calcPerimeter(MultiPolygon geom){
        Double perimeter = null;
        MultiPolygon geomInternal = geom;

        if (!isMeterCRS(geom.getSRID())){
            geomInternal = (MultiPolygon) transformSpGeom(geomInternal, findWgsUtm_EPSG(extractPoint(geom)));
        }

        perimeter = geomInternal.getLength();

        setLength(perimeter, M);

    }


    /**
     * Calculates the geodesic distance between 2 geometries and uses the Wgs84 ellipsoid.
     * If geometries have different types, the nearest point compared to the other geometry will be calculated
     * and will be used for distance measure.
     *
     *
     * @param startGeom first geometry
     * @param destinationGeom second geometry
     * @return Double length value with Unit<Length> Meter.
     */
    public void calGeodeticDistance(Geometry startGeom, Geometry destinationGeom) {
        Double distance = null;

        /** creates an GeodesicCalculator with standard ellipsoid wgs84 */
        GeodeticCalculator gc = new GeodeticCalculator();

        /** read out the used CRS coded from the input geometries*/

        CoordinateReferenceSystem start_CRS = getCRS(startGeom.getSRID());
        CoordinateReferenceSystem destination_CRS =  getCRS(destinationGeom.getSRID());


        try {
            // calculation for start point, if not a single point the nearest point of between this 2 points are searched
            if (!isPoint(startGeom)) {
                // sets the closest destination point by calculating the nearest point compared to the  start geometry.
                // the interim result is a coordinate pair calculated with the DistanceOP method. --> nearest point (in array on position 0)
                // is a coordinate pair (coords are required for this interim method)
                gc.setStartingPosition(JTS.toDirectPosition(DistanceOp.nearestPoints(startGeom, destinationGeom)[0], start_CRS));
            } else {

                // point is set as
                gc.setStartingPosition(JTS.toDirectPosition(startGeom.getCoordinate(), start_CRS));
            }


            // calculates for destination point
            if (!isPoint(destinationGeom)) {
                gc.setDestinationPosition(JTS.toDirectPosition(DistanceOp.nearestPoints(destinationGeom, startGeom)[0], destination_CRS));

            } else {
                gc.setDestinationPosition(JTS.toDirectPosition(destinationGeom.getCoordinate(), destination_CRS));
            }


            distance = gc.getOrthodromicDistance();

        } catch (TransformException e) {
            //todo log that something went wrong
            distance = 0d;
            e.printStackTrace();
        }

        setLength(distance, M);
    }


    /**
     * basic problems solved in this method are:
     * 1. calculates the distance requires a cartesian projection. So if the epsgCode is not in meter units, it will
     * be reprojected to corresponding WGS84 UTM Zone (epsg 32601 - 32660 N or 32701 - 32760 S)
     * 2. JTS knows nothing about CRS systems so if calculating the distance it has to be checked if start and destination
     * point using the same crs system. otherwise nonsens distance results occurs: e.g. in UTM32N with UTM33N
     * if a geometry >dim0 is used, the closest point to goal geom is automatically used in this process.
     * @param startGeom
     * @param destinationGeom
     * @return
     */
    public void calcPlanarDistance(Geometry startGeom, Geometry destinationGeom){

        Double distance = null;
        Integer utm_epsg = null;

        // different between =, copy and clone not clear
        Geometry start_geom_internal = startGeom;
        Geometry dest_geom_internal = destinationGeom;


        // referenced to the internal geom. if the internal geom is transformed, the epsg_code is always the new one"
        // so kind of dynamical check
        //int start_epsg = start_geom_internal.getSRID();


        // this one is only checked ones and that's why is is kind of static and references to the input destination geom
        int destination_epsg = destinationGeom.getSRID();


        //if used crs in not metric
        if (!isMeterCRS(start_geom_internal.getSRID())){
            // if epsg code is not wgs84 (and not metric  as checked before) the geom is reprojected into wgs84
            if (start_geom_internal.getSRID() != 4326){
                //transformed geom is referenced to internal geometry
                start_geom_internal = transformSpGeom(startGeom, 4326);
            }

            //if epsg is wgs84 then find corresponding utm epsg (could be also as else)
            if (start_geom_internal.getSRID() == 4326) {
                // extract utm zone epsg of the input geometry
                // for line and polygon the centroid will be used
                utm_epsg = findWgsUtm_EPSG(extractPoint(start_geom_internal));
                // transform to given utm zone
                start_geom_internal = transformSpGeom(start_geom_internal, utm_epsg);
            }
        }

        // so if destination_epsg is not the same as start_epsg ... then they are not in the same metric zone
        // and has to be reprojected
        if (destination_epsg != start_geom_internal.getSRID()) {
            dest_geom_internal = transformSpGeom(destinationGeom, start_geom_internal.getSRID());
        }


        distance = start_geom_internal.distance(dest_geom_internal);

        setLength(distance, M);
    }


    public void calcHausdorffDistance(Geometry mainGeom, Geometry secondGeom){

        Double hausdorff = null;
        Geometry mainGeom_Internal = mainGeom;
        Geometry secondGeom_Internal = secondGeom;

        if(!isMeterCRS(mainGeom_Internal.getSRID())){
            if (mainGeom_Internal.getSRID() != 4326){
                mainGeom_Internal = transformSpGeom(mainGeom_Internal, 4326);
            }
        }

        if (mainGeom_Internal.getSRID() == 4326){
            mainGeom_Internal = transformSpGeom(mainGeom_Internal, findWgsUtm_EPSG(extractPoint(mainGeom_Internal)));
        }

        secondGeom_Internal = unifyEPSG(mainGeom_Internal, secondGeom_Internal, false);


        hausdorff = DiscreteHausdorffDistance.distance(mainGeom, secondGeom_Internal);

        setLength(hausdorff, M);
    }

}
