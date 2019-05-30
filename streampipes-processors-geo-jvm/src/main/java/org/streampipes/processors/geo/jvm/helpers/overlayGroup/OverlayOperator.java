package org.streampipes.processors.geo.jvm.helpers.overlayGroup;

import org.locationtech.jts.geom.*;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;
import java.util.ArrayList;
import java.util.Collection;


import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class OverlayOperator extends EnrichAndFilter {


    public OverlayOperator(Geometry geomA, Geometry geomB_real) {
        super(geomA, geomB_real);
    }



    //=================== Difference
    public Polygon geomDifferenceSP(){

        Polygon returnValue = null;

        if (bothGeometriesHaveSameType(GeometryTypes.POLYGON)){
            returnValue = (Polygon) createSPGeom(getGeomA().difference(getGeomB()), getGeomA().getSRID());

        } else {
            returnValue = (Polygon) createEmptyGeometry( returnValue);
            //todo logger
            System.out.println("Geometries are not Polygons. So an empty Polygon will be returned");
        }

        return returnValue;

    }



    //=================== Intersection
    public Geometry geomIntersectionSP(){

        Geometry resultGeom = null;


        // geometrycollection will not be tested

        // istanceof geometryCollection is not working
        if (!(getGeomA().getGeometryType()== "GeometryCollection") || !(getGeomB().getGeometryType()== "GeometryCollection")){

            //second geometry is always a polygon

            if (getGeomB().getDimension() ==2 ){
                if (getGeomA().getDimension() == 0){
                    resultGeom = createSPGeom(getGeomA().intersection(getGeomB()), getGeomA().getSRID());
                } else if (getGeomA().getDimension() == 1){
                    resultGeom = createSPGeom(getGeomA().intersection(getGeomB()), getGeomA().getSRID());
                } else if (getGeomA().getDimension() == 2){
                    resultGeom = createSPGeom(getGeomA().intersection(getGeomB()), getGeomA().getSRID());
                }

            } else {
                //todo logger
                System.out.println("Non supported situation in geomIntersectionSP. Second geometry is not a Polygon");
                resultGeom = getGeomA().getFactory().createPolygon();

            }

        } else {
            //todo logger
            System.out.println("GeometryCollection can't be tested with Intersection");
            resultGeom = getGeomA().getFactory().createPolygon();
        }


        return resultGeom;
    }


    // =================== Union
    public Polygon geomUnionSP(){

        Polygon returnGeom = null;


        if (bothGeometriesHaveSameType(GeometryTypes.POLYGON) | bothGeometriesHaveSameType(GeometryTypes.MULTIPOLYGON)){
            returnGeom = (Polygon) createSPGeom(getGeomA().union(getGeomB()), getGeomA().getSRID());
        } else {
            //todo logger
            System.out.println("Both geometries of geomUnionSp are not polygons, so empty polygon will be returned");
            returnGeom = getGeomA().getFactory().createPolygon();
        }


        return returnGeom;




    }

    // =================== SymDifference
    public Geometry geomSymDifferenceSP(){


        Polygon returnGeom = null;

        if (bothGeometriesHaveSameType(GeometryTypes.POLYGON)){
            returnGeom = (Polygon) createSPGeom(getGeomA().symDifference(getGeomB()), getGeomA().getSRID());
        } else {
            //todo logger
            System.out.println("Both geometries of geomUnionSp are not polygons, so empty polygon will be returned");
            returnGeom = getGeomA().getFactory().createPolygon();
        }


        return returnGeom;
    }


    // UnionLevel
    public MultiPolygon geomUnionLevel() {
        Collection<Polygon> collection = new ArrayList<>();


        MultiPolygon returnGeom = null;

        if (bothGeometriesHaveSameType(GeometryTypes.POLYGON) | bothGeometriesHaveSameType(GeometryTypes.MULTIPOLYGON)) {


            OverlayOperator a_vs_b = new OverlayOperator(getGeomA(), getGeomB());
            collection.add((Polygon) a_vs_b.geomIntersectionSP());
            collection.add(a_vs_b.geomDifferenceSP());

            OverlayOperator b_vs_a = new OverlayOperator(getGeomB(), getGeomA());
            collection.add( b_vs_a.geomDifferenceSP());

            returnGeom = createSPMultiPolygon(collection, false);

        } else {
            //todo logger
            System.out.println("geomUnionLevel operator does not have Polygons as input geometries. Empty Polygon will be returned ");
            returnGeom = getGeomA().getFactory().createMultiPolygon();
        }


        return returnGeom;
    }








}
