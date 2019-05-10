package org.streampipes.processors.geo.jvm.helpers.topologyGroup;

import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;

public class CoveredByTopology extends EnrichAndFilter {


    public CoveredByTopology(Geometry geomA, Geometry geomB_real) {
        super(geomA, geomB_real);
    }


    public boolean coversBySp(){

        boolean result = getGeomA().coveredBy(getGeomB());

        return result;
    }

}
