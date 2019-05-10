package org.streampipes.processors.geo.jvm.helpers.topologyGroup;

import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.Point;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.countSingleGeometriesInsideCollection;



public class OverlapsTopology extends EnrichAndFilter {

    public OverlapsTopology(Geometry geomA, Geometry geomB_real) {
        super(geomA, geomB_real);
    }


    /**
     * The geometries have some points in common; but not all points in common (so if one geometry is
     * inside the other overlaps would be false). The overlapping section must be the same kind of
     * shape as the two geometries; so two polygons that touch on a point are not considered to
     * be overlapping.:
     * <p>
     * <p>
     * Overlapping mit Getrenntheit. Interior Objekt A intersected die boundary und interior von
     * Objekt B, die beiden Boundaries schneiden sich sich aber nicht. Beispiel: Linie
     * beginnt außerhalb einer Fläche und endet im inneren
     * <p>
     * <p>
     * Overlapping mit Überschneidung: Boundaries und Interiors der beiden Objekte intersecten.
     * Es gilt aber nur als overlapping , wenn die Geometrie der intersection einer  kleineren Dimension
     * in der Größeren vorhanden ist. oder einfacher nur Geometrien gleichen Types können sich overlappen
     * <p>
     * Punkte können keine Punkte, Linien oder Flächen intersecten und vise versa
     * <p>
     * Zusammengefasst 3 Möglichkeiten:
     * Overlapping mit Getrenntheit --> Linie Polygon
     * Linie mit Überschneidung --> Linie overlappt Linie
     * Polygon overlappt Polygon --> Schnitt Linie
     * MultiPoints overlaps MultiPoint

     * @return
     */

    public  boolean overlapsSP() {

        if (getGeomA() instanceof Point | getGeomB() instanceof Point) {
            //todo logger
            System.out.println("Geometry contains points and can't be checked with overlapping");
        }

        if (getGeomA().getDimension() != getGeomB().getDimension()){
            //todo logger
            System.out.println("Dimensions from the geometries are not from the same type");

        }

        boolean result =getGeomA().overlaps(getGeomB());

        return result;
    }

    public boolean filterLinesOverlap() {
        boolean result = false;

        if (getGeomA().getDimension() == getGeomB().getDimension() & getGeomA().getDimension() == 1) {
            result = getGeomA().relate(getGeomB(), "1*T***T**");
        } else {
            //todo logger
            System.out.println("non line geometries were checked and result will be automatically false  ");
            if (getGeomA().getGeometryType()== "GeometryCollection")
                if (countSingleGeometriesInsideCollection( (GeometryCollection) getGeomA(), 0) == 0 | countSingleGeometriesInsideCollection(( GeometryCollection) getGeomB(), 0) == 0) {
                    //todo logger
                    System.out.println("Points inside Collections can't be tested with overlapping");
                }
        }

        return result;
    }

    public boolean filterPolygonOverlay() {

        boolean result = false;

        // testing if both dims are same ans polygons
        if ((getGeomA().getDimension() == getGeomB().getDimension()) & getGeomA().getDimension() == 2) {
                result = getGeomA().overlaps(getGeomB());
        } else {
            //todo logger
            System.out.println("non polygon geometries were included in filterPolygonOverlay and result will be set automatically false  ");
            if (getGeomA().getGeometryType()== "GeometryCollection")
                //if geometryA have points or liens in the collection
                if ((countSingleGeometriesInsideCollection((GeometryCollection) getGeomA(), 0) >0)  | (countSingleGeometriesInsideCollection((GeometryCollection) getGeomA(), 1 ) >1)){
                    System.out.println("Collection contains with points and lines and are filtered  out");
                }
        }

        return result;
    }

    public boolean filterMultiPoint(){

        boolean result = false;

        if ((getGeomA().getDimension() == getGeomB().getDimension()) & getGeomA().getDimension() == 0) {
            if (getGeomA() instanceof MultiPoint) {
                result = getGeomA().overlaps(getGeomB());
            } else {
                System.out.println("Single Point geometries can't be tested and result is automatically false ");
            }
        } else {
            //todo logger
            System.out.println("Testing Overlapping MultiPoints contains other geometries than MultiPoints. Result is automatically false");
        }

        return result;

    }
}



    // primitive

//    @Deprecated
//    public static boolean overlapsSP(MultiPoint geomA, MultiPoint geomB) {
//
//        MultiPoint geomB_temp = geomB;
//
//        if (geomA.getSRID() != geomB.getSRID()) {
//            geomB_temp = (MultiPoint) createSPGeom(geomB, geomA.getSRID());
//        }
//
//        boolean result = geomA.overlaps(geomB_temp);
//
//        return result;
//
//    }
//
//    @Deprecated
//    public static boolean overlapsSP(MultiLineString geomA, MultiLineString geomB) {
//
//        MultiLineString geomB_temp = geomB;
//
//        if (geomA.getSRID() != geomB.getSRID()) {
//            geomB_temp = (MultiLineString) createSPGeom(geomB, geomA.getSRID());
//        }
//
//        boolean result = geomA.overlaps(geomB_temp);
//
//        return result;
//
//    }
//
//    @Deprecated
//    public static boolean overlapsSP(MultiPolygon geomA, MultiPolygon geomB) {
//
//        MultiPolygon geomB_temp = geomB;
//
//        if (geomA.getSRID() != geomB.getSRID()) {
//            geomB_temp = (MultiPolygon) createSPGeom(geomB, geomA.getSRID());
//        }
//
//        boolean result = geomA.overlaps(geomB_temp);
//
//        return result;
//    }
//
//    @Deprecated
//    public static boolean overlapsSP(GeometryCollection geomA, GeometryCollection geomB) {
//
//        Geometry geomB_temp = geomB;
//
//        if (geomA.getSRID() != geomB.getSRID()) {
//            geomB_temp = createSPGeom(geomB, geomA.getSRID());
//        }
//
//        if (countSingleGeometriesInsideCollection(geomA, 0) == 0 | countSingleGeometriesInsideCollection(geomB, 0) == 0) {
//            System.out.println("Points inside Collections can't be tested with overlapping");
//        }
//
//        boolean result = geomA.overlaps(geomB_temp);
//
//        return result;
//
//    }
//
//    @Deprecated
//    public static boolean overlapLineString(LineString geomA, LineString geomB) {
//
//        LineString geomB_temp = geomB;
//
//        if (geomA.getSRID() != geomB.getSRID()) {
//            geomB_temp = (LineString) createSPGeom(geomB, geomA.getSRID());
//        }
//
//        boolean result = geomA.overlaps(geomB_temp);
//
//        return result;
//
//    }
//
//    @Deprecated
//    public static boolean overlapsSP(Polygon geomA, Polygon geomB) {
//
//        Polygon geomB_temp = geomB;
//
//        if (geomA.getSRID() != geomB.getSRID()) {
//            geomB_temp = (Polygon) createSPGeom(geomB, geomA.getSRID());
//        }
//
//        boolean result = geomA.overlaps(geomB_temp);
//
//        return result;
//
//    }
