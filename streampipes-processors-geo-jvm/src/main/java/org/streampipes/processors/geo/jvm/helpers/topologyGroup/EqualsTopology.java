package org.streampipes.processors.geo.jvm.helpers.topologyGroup;

import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;

public class EqualsTopology extends EnrichAndFilter {
    public EqualsTopology(Geometry geomA, Geometry geomB_real) {
        super(geomA, geomB_real);
    }


    public boolean equalsSP() {

        boolean result = getGeomA().equals(getGeomB());

        return result;
    }

    public boolean equalsExactSP() {

        boolean result = getGeomA().equalsExact(getGeomB());

        return result;
    }


}
