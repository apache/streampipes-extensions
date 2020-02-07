package org.apache.streampipes.processors.geo.jvm.jts.helpers;

import org.locationtech.jts.geom.*;

public class SpGeometryBuilder {

  final static double LONGITUDE_MIN = -180.00;
  final static double LONGITUDE_MAX = 180.00;
  final static double LATITUDE_MIN = -90;
  final static double LATITIDE_MAX = 90;


  /**Creates a JTS point geometry from Longitude and Latitude values
   *
   * @param lng Longitude value in the range -180 <Longitude > 180
   * @param lat Latitude value in the range -90 <LATITUDE > 90
   * @param epsg EPSG Code for projection onfo
   * @return a JTS Point Geometry Object with lat lng values. An empty point geometry is created if Latitude or Longitude values are out of range
   * or has null values.
   */
  public static Point createSPGeom(Double lng, Double lat, Integer epsg) {
    Point point;
    PrecisionModel precisionModel = getPrecisionModel(epsg);
    GeometryFactory geomFactory = new GeometryFactory(precisionModel, epsg);

    //check if value is not null due missing stream value
    if ((lng != null) && (lat != null)) {
      //check if lat lng is in typical range
      if (isInWGSCoordinateRange(lng, LONGITUDE_MIN, LONGITUDE_MAX) || isInWGSCoordinateRange(lat, LATITUDE_MIN, LATITIDE_MAX)) {

        Coordinate coordinate = new Coordinate(lng, lat);
        point = geomFactory.createPoint(coordinate);
      } else {
        // creates empty point if values are out of Range
        point = geomFactory.createPoint();
      }
    } else {
      // creates empty point if lng lat are null value
      point = geomFactory.createPoint();
    }

    return point;
  }

  /**
   *
   * @param checkedvalue Any Value
   * @param min Min value to check
   * @param max max value to check
   * @return boolean value true or false
   */
  private static boolean isInWGSCoordinateRange(double checkedvalue, double min, double max){
    return checkedvalue > min && checkedvalue < max;
  }


  /**
   * Creates a JTS PrecisionModel with a specific precision.
   * WGS84/WGS84 will be created with 7 decimal positions.
   * Any other epsg code will create a precision with Ffloating type. See JTS PrecisionModel for more information
   * @param epsg EPSG alue
   * @return a JTS PrecisionModel
   */
  private static PrecisionModel getPrecisionModel(Integer epsg) {
    PrecisionModel precisionModel;

    if (epsg == 4326) {
      // use scale precision with 7 decimal positions like default OSM
       precisionModel = new PrecisionModel(1000000);
    } else {
      // use default constructor
      precisionModel = new PrecisionModel();
    }

    return precisionModel;
  }
}
