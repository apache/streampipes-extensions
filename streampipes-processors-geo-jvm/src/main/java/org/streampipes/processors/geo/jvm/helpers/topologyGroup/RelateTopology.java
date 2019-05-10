package org.streampipes.processors.geo.jvm.helpers.topologyGroup;

import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;

public class RelateTopology extends EnrichAndFilter {


    public RelateTopology(Geometry geomA, Geometry geomB_real) {
        super(geomA, geomB_real);
    }


    public boolean relateSP (String pattern){

        boolean result = getGeomA().relate(getGeomB(), pattern);

        return result;

    }
}
