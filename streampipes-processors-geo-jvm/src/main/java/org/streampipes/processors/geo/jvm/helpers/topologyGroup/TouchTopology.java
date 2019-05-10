package org.streampipes.processors.geo.jvm.helpers.topologyGroup;

import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;
import org.streampipes.processors.geo.jvm.helpers.GeometryCreation;

public class TouchTopology extends EnrichAndFilter {


    public TouchTopology(Geometry geomA, Geometry geomB_real) {
        super(geomA, geomB_real);
    }

    public boolean touchesSP() {

        boolean result = getGeomA().touches(getGeomB());
        return result;
    }

    public boolean touchesInteriorSp(){
        boolean result = getGeomA().relate(getGeomB(), "F**T*****");

        return result;
    }

    public boolean touchesBoundarySp(){
        boolean result = getGeomA().relate(getGeomB(), "F***T****");
        return result;
    }

    public boolean touchesPolygonsAtCorner(){
        boolean result = false;

        if ((bothGeometriesHaveSameType(GeometryCreation.GeometryTypes.POLYGON) | (bothGeometriesHaveSameType(GeometryCreation.GeometryTypes.MULTIPOLYGON)))) {
            result = getGeomA().relate(getGeomB(), "F***0****");
        }
        return result;
    }

    public boolean touchesPolygonsAtBoundaryOnly(){

        boolean result = false;

        if ((bothGeometriesHaveSameType(GeometryCreation.GeometryTypes.POLYGON) | (bothGeometriesHaveSameType(GeometryCreation.GeometryTypes.MULTIPOLYGON)))){
            result = getGeomA().relate(getGeomB(), "F***1****");
        }
        return result;
    }

}
