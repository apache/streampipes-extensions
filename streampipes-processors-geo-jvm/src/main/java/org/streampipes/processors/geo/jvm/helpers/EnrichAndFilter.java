package org.streampipes.processors.geo.jvm.helpers;

import org.locationtech.jts.geom.*;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public abstract class EnrichAndFilter {

    private Geometry geomA;
    private Geometry geomB_realInput;
    private Geometry geomB;



    public EnrichAndFilter(Geometry geomA, Geometry geomB_realInput) {
        this.geomA = geomA;
        this.geomB_realInput = geomB_realInput;
        this.geomB = unifyEPSG(geomA, getGeomB_realInput(), true);
    }


    public Geometry getGeomA() {
        return geomA;
    }

    public Geometry getGeomB() {
        return geomB;
    }

    public Geometry getGeomB_realInput() {
        return geomB_realInput;
    }

    public boolean bothGeometriesHaveSameType(GeometryTypes value) {

        boolean result = false;

        if ((bothGeometriesHaveSameType()) & value.getNumber() == 1) {
            result = true;

        } else if (bothGeometriesHaveSameType() & value.getNumber() == 2) {
            result = true;

        } else if (bothGeometriesHaveSameType() & value.getNumber() == 3) {
            result = true;

        }  else if (bothGeometriesHaveSameType() & value.getNumber() == 4) {
            result = true;
        } else if (bothGeometriesHaveSameType() & value.getNumber() == 5) {
            result = true;

        } else if (bothGeometriesHaveSameType() & value.getNumber() == 6) {
            result = true;
        } else if (bothGeometriesHaveSameType() & value.getNumber() == 7) {
            return true;
        }

        return result;
    }


    public boolean bothGeometriesHaveSameType() {

        boolean result = false;

        if (this.geomA instanceof Point & this.geomA instanceof Point) {
            result = true;
        } else if (this.geomA instanceof LineString & this.geomA instanceof LineString) {
            result = true;

        } else if (this.geomA instanceof Polygon & this.geomA instanceof Polygon) {
            result = true;

        }  else if (this.geomA instanceof MultiPoint & this.geomA instanceof MultiPoint) {
            result = true;
        } else if (this.geomA instanceof MultiLineString & this.geomA instanceof MultiLineString) {
            result = true;

        } else if (this.geomA instanceof MultiPolygon & this.geomA instanceof MultiPolygon) {
            result = true;
        } else if (this.geomA instanceof GeometryCollection & this.geomA instanceof GeometryCollection) {
            return true;
        }

        return result;
    }
}

