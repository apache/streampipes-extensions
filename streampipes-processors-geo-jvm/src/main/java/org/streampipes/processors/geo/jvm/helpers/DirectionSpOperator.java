package org.streampipes.processors.geo.jvm.helpers;

import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.GeodeticCalculator;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.operation.distance.DistanceOp;
import org.opengis.referencing.crs.CoordinateReferenceSystem;
import org.opengis.referencing.operation.TransformException;
import si.uom.NonSI;
import tec.uom.se.quantity.Quantities;

import javax.measure.Quantity;
import javax.measure.Unit;
import javax.measure.quantity.Angle;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public class DirectionSpOperator {


    private Geometry geomA;
    private Geometry geomB;

    private final Unit<Angle> GRAD = NonSI.DEGREE_ANGLE;


    private Quantity<Angle> direction;
    private String directionString;
    private String orientation;
    private Integer decimalPosition;


    public DirectionSpOperator(Geometry geomA, Geometry geomB, Integer decimalPosition) {
        this.geomA = geomA;
        this.geomB = geomB;
        this.decimalPosition = decimalPosition;
        this.direction = Quantities.getQuantity(setDirection(this.geomA, this.geomB), GRAD);
        this.orientation = setOrientation((Double) this.direction.getValue());
        this.directionString = doubleToString((Double) direction.getValue(), this.decimalPosition);
    }

    /**
     * calculates azimuth direction in degree °
     * @param startGeom
     * @param destinationGeom
     * @return azimuth in degree between 0° (N), 180  (S) and  360 ° (N)
     */
    private double setDirection(Geometry startGeom, Geometry destinationGeom) {
        Double direction;

        // creates an GeodesicCalculator with standard ellipsoid wgs84
        GeodeticCalculator gc = new GeodeticCalculator();

        // read out the used CRS coded from the input geometries*/

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


            direction = gc.getAzimuth();

            if (direction < 0){
                direction = direction + 360;
            }

        } catch (TransformException e) {
            //todo log that something went wrong
            System.out.println("wrong");
            direction = 0d;
            e.printStackTrace();
        }

        return direction;
    }


    /**
     * Calculates the orientation from compass direction, divided in 11.25° steps. So north is between 348.75 and <= 11.25
     * @param direction azimuth direction. must be between 0 and 360°
     * @return A String with corresponding direction string e.g. "N"
     */
    private String setOrientation(Double direction){
        String orientation = null;

        if (348.75 > direction && direction <= 11.25)
            orientation = "N"; //north
        else if (11.25 > direction && direction <= 33.75)
            orientation = "NNE"; //NNO
        else if (33.75 > direction && direction <= 56.25)
            orientation = "NE"; //NO
        else if (56.25 > direction && direction <= 78.75)
            orientation = "ENE"; //ONO
        else if (78.75 > direction && direction <= 101.25)
            orientation= "E"; //E
        else if (101.25 > direction && direction <= 123.75)
            orientation= "ESE"; //ESO
        else if (123.75 > direction && direction <= 146.25)
            orientation= "SE"; //SO
        else if (146.25 > direction && direction <= 168.75)
            orientation= "SSE"; //SSO
        else if (168.75 > direction && direction <= 191.25)
            orientation= "S"; //S
        else if (191.25 > direction && direction <= 213.75)
            orientation= "SSW"; //SSW
        else if (213.75 > direction && direction <= 236.25)
            orientation= "SW"; //SW
        else if (236.25 > direction && direction <= 258.75)
            orientation= "WSW"; //WSW
        else if (258.75 > direction && direction <= 281.25)
            orientation= "W"; //W
        else if (281.25 > direction && direction <= 303.75)
            orientation = "WNW"; //WNW
        else if (303.75 > direction && direction <= 326.25)
            orientation= "NW"; //NW
        else if (326.25 > direction && direction <= 348.75)
            orientation= "NNW"; //NNW

        return orientation;

    }


    public String getStringDirection() {
        return this.directionString;
    }

    public String getOrientation() {
        return this.orientation;
    }

    public String getDirectionUnit() {
        return direction.getUnit().toString();
    }





}

