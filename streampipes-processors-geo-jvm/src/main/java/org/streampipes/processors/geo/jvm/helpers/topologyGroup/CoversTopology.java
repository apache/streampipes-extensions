package org.streampipes.processors.geo.jvm.helpers.topologyGroup;

import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;

public class CoversTopology extends EnrichAndFilter {


    public CoversTopology(Geometry geomA, Geometry geomB_real) {
        super(geomA, geomB_real);
    }


    public boolean coversSP(){

        boolean result = getGeomA().covers(getGeomB());

        return result;
    }


}
