package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.touchesPolygons;

public enum TouchesPolygonType {
    normal (1), borderOnly (2), cornerOnly(3);


    private int number;


    TouchesPolygonType(int number) {
        this.number = number;
    }

    public int getNumber() {
        return number;
    }
}
