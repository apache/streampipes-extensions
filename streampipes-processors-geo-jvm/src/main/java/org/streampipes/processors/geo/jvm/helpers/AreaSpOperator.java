package org.streampipes.processors.geo.jvm.helpers;

import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import si.uom.NonSI;
import si.uom.SI;
import tec.uom.se.quantity.Quantities;
import tec.uom.se.unit.MetricPrefix;

import javax.measure.Quantity;
import javax.measure.Unit;
import javax.measure.quantity.Area;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class AreaSpOperator {

    private final Unit<Area> SQUARE_M = SI.SQUARE_METRE;
    private final Unit<Area> SQUARE_KM = MetricPrefix.KILO(SQUARE_M);
    private final Unit<Area> HECTAR = NonSI.HECTARE;
    private final Unit<Area> AR = SQUARE_M.divide(100);


    /**
     * enum list with valid area units. extraction can be due name or int number (getNumber)
     */
    public enum ValidAreaUnits {
        squareMeter (1), squareKM (2), hectar (3), ar(4);


        private final int number;


        ValidAreaUnits(int number) {
            this.number = number;

        }

        public int getNumber() {
            return number;
        }
    }

    // unit method

    /**
     * convert between the different Area units from the @see ValidAreaUnits  enum
     * @param value
     */
    public void convertUnit (ValidAreaUnits value) {
        switch (value.getNumber()) {
            case 1:
                setArea(this.area.to(SQUARE_M));
                break;
            case 2:
                setArea(this.area.to(SQUARE_KM));
                break;
            case 3:
                setArea(this.area.to(HECTAR));
                break;
            case 4:
                setArea(this.area.to(AR));
                break;
        }
    }


    /**
     * convert between the different Area units from the @see ValidAreaUnits  enum
     * @param value
     */
    public void convertUnit (int value) {
        switch (value) {
            case 1:
                setArea(this.area.to(SQUARE_M));
                break;
            case 2:
                setArea(this.area.to(SQUARE_KM));
                break;
            case 3:
                setArea(this.area.to(HECTAR));
                break;
            case 4:
                setArea(this.area.to(AR));
                break;
        }
    }


    private Quantity<Area> area;
    private int decimalPosition;


    /**
     * Constructor without control using a valid area unit
     *
     * @param area Quantity<Area> class with value and unit
     * @param decimalPosition
     */
    public AreaSpOperator(Quantity<Area> area, int decimalPosition){
        this.area = area;
        this.decimalPosition = decimalPosition;
    }

    /**
     * Constructor for AreaSpOperator with one input parameter decimalPosition.
     * area will be set by default with -9999 instead of empty.
     * This should be the standard constructor of this method.
     *
     * @param decimalPosition amount of decimal position
     */
    public AreaSpOperator(int decimalPosition){
        this.area = Quantities.getQuantity(-9999d, SQUARE_M);
        this.decimalPosition = decimalPosition;

    }


    // getter

    /**
     * getter method for variable decimalPosition
     * @return  value for decimalPosition
     */
    public int getDecimalPosition() {
        return decimalPosition;
    }



    public String getAreaAsString(){
        String result = doubleToString( getAreaValue(), getDecimalPosition());
        return result;
    }



    /**
     * getter method for variable length
     * @return Double length value
     */
    public Double getAreaValue() {
        return (area.getValue()).doubleValue();
    }

    /**
     * getter method for Unit<Area> as a String
     * @return String value of Unit<Area>
     */
    public String getAreaUnit() {
        return area.getUnit().toString();
    }

    /**
     * returns the Unit<Area>
     * @return
     */
    public Unit<Area> getUnit(){
        return area.getUnit();

    }


    //setter

    public void setDecimalPosition(int decimalPosition) {
        this.decimalPosition = decimalPosition;
    }


    private void setArea(Quantity<Area> area) {
        this.area = area;
    }


    private void setArea(double value, Unit<Area> unit) {
        this.area = Quantities.getQuantity(value, unit);
    }





    // main method

    /**
     * calculates the area of a polygon with the Shoelace formula (Gaußsche Trapezformel) algorithm from the
     * package JTS.
     * A non valid polygon leeds to an result of -9999. standard unit in square meter.
     * if a non metric crs is used, it will be automatically reprojected into the corresponding utm zone. the target
     * epsg code will be calculated from the centroid point of the polygon.
     *
     * @param geom polygon geometry
     */
    public void calcArea (Polygon geom){

        if(!isValidPolygon(geom)){
            setArea(-9999, SQUARE_M);
        } else {

            Polygon geomInternal = geom;

            if (!isMeterCRS(geom.getSRID())){
                geomInternal = (Polygon) transformSpGeom(geomInternal, findWgsUtm_EPSG(extractPoint(geom)));
            }

            Double calculatedArea = geomInternal.getArea();

            setArea(calculatedArea, SQUARE_M);

        }


    }



    /**
     * calculates the area of a MultiPolygon with the Shoelace formula (Gaußsche Trapezformel) algorithm from the
     * package JTS.
     * A non valid MultiPolygon leeds to an result of -9999. Standard unit in square meter.
     * If a non metric crs is used, it will be automatically reprojected into the corresponding utm zone. The target
     * epsg code will be calculated from the centroid point of all polygon inside the MultiPolygon.
     *
     * @param geom polygon geometry
     */
    public void calcArea (MultiPolygon geom){

        if(!isValidPolygon(geom)){
            setArea(-9999, SQUARE_M);
        } else {

            MultiPolygon geomInternal = geom;

            if (!isMeterCRS(geom.getSRID())){
                geomInternal = (MultiPolygon) transformSpGeom(geomInternal, findWgsUtm_EPSG(extractPoint(geom)));
            }

            Double calculatedArea = geomInternal.getArea();

            setArea(calculatedArea, SQUARE_M);

        }

    }


    /**
     * Method to check if Polygon is valid. Mainly no self-intersection or empty.
     * Otherwise calculation with false valid polygons lead to errors.
     * @param geom to tested Polygon
     * @return true or false is valid
     */
    public  boolean isValidPolygon (Polygon geom){
        boolean isValid = false;

        if (geom.isValid()){
            isValid = true;
        }

        return isValid;
    }


    /**
     * Method to check if MultiPolygon is valid. Mainly no self-intersection or empty.
     * Otherwise calculation with false valid polygons lead to errors.
     * @param geom to tested MultiPolygon
     * @return true or false is valid
     */
    public  boolean isValidPolygon (MultiPolygon geom){
        boolean isValid = false;

        if (geom.isValid()){
            isValid = true;
        }

        return isValid;
    }
}
