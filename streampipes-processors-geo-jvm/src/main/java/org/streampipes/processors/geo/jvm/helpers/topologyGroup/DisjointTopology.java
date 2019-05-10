package org.streampipes.processors.geo.jvm.helpers.topologyGroup;

import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.EnrichAndFilter;

public class DisjointTopology extends EnrichAndFilter {
    public DisjointTopology(Geometry geomA, Geometry geomB_real) {
        super(geomA, geomB_real);
    }


    /**
     * Disjoint. Geometrie A intersected weder in der Boundary und Interior mit Geometrie B.
     * Ist das Gegenteil von Intersection
     * Relate: FF*FF****
     *
     * @return boolean true, false
     */
    public boolean disjointSP(){

        boolean result = getGeomA().disjoint(getGeomB());

        return result;
    }


}
