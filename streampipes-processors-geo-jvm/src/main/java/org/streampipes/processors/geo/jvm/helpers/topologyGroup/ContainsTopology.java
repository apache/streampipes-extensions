package org.streampipes.processors.geo.jvm.helpers.topologyGroup;

import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;

public class ContainsTopology extends EnrichAndFilter {


    public ContainsTopology(Geometry geomA, Geometry geomB_real) {
        super(geomA, geomB_real);
    }

    public boolean containsSp() {

        boolean result = getGeomA().contains(getGeomB());

        if (getGeomA().getDimension() > getGeomB().getDimension()) {
            //todo logger
            System.out.println("Contains testing is automatically false due higher dim of second geometry");
        }

        return result;

    }


    public boolean containsCompleteSp() {

        boolean result = getGeomA().relate(getGeomB(),"T**FF*FF*");

        if (getGeomA().getDimension() > getGeomB().getDimension()) {
            //todo logger
            System.out.println("Contains testing is automatically false due higher dim of second geometry");
        }

        return result;

    }

}
