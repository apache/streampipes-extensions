package org.streampipes.processors.geo.jvm.helpers.topologyGroup;

import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;

public class IntersectTopology extends EnrichAndFilter {


    public IntersectTopology(Geometry geomA, Geometry geomB) {
        super(geomA, geomB);
    }


    public boolean intersectSp() {

        boolean result = getGeomA().intersects(getGeomB());
        return result;

    }

    public boolean intersectSpOnInteriors() {

        boolean result = getGeomA().relate(getGeomB(), "T*********");

        return result;

    }

    public boolean intersectSpOnInteriorsVSBoundary() {

        boolean result = getGeomA().relate(getGeomB(), "*T********");

        return result;

    }

    public boolean intersectSpOnBoundaryVSInterior() {

        boolean result = getGeomA().relate(getGeomB(), "***T******");
        return result;

    }

    public boolean intersectSpOnBoundary() {

        boolean result = getGeomA().relate(getGeomB(), "****T*****");

        return result;

    }

}

