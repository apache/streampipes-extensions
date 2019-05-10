package org.streampipes.processors.geo.jvm.helpers.topologyGroup;

import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;

public class WithinTopology extends EnrichAndFilter {
    public WithinTopology(Geometry geomA, Geometry geomB_real) {
        super(geomA, geomB_real);
    }


    public boolean withinSP() {

        boolean result = getGeomA().within(getGeomB());

        if (getGeomA().getDimension() > getGeomB().getDimension()) {
            //todo logger
            System.out.println("Contains testing is automatically false due higher dim of second geometry");
        }

        return result;

    }


    public boolean withinCompleteSP() {

        boolean result = getGeomA().relate(getGeomB(), "TFF*FF***");
        return result;
    }
}
