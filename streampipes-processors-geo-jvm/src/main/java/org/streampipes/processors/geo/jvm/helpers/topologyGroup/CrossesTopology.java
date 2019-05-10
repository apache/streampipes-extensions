package org.streampipes.processors.geo.jvm.helpers.topologyGroup;

import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;
import org.streampipes.processors.geo.jvm.helpers.GeometryCreation;



public class CrossesTopology extends EnrichAndFilter {
    public CrossesTopology(Geometry geomA, Geometry geomB_real) {
        super(geomA, geomB_real);
    }


    public boolean crossesSP(){

        boolean result = getGeomA().crosses(getGeomB());
        return result;
    }

    public boolean crossesOnlyLineSP(){

        boolean result = getGeomA().relate(getGeomB(),"0********");

        if (!bothGeometriesHaveSameType(GeometryCreation.GeometryTypes.LINESTRING))
            //todo logger
            System.out.println("non LineString geometries where checked with crossesOnlyLineSP and result will be set to false automatically" );

        return result;


    }



}
