package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.intersects;

public enum IntersectsTypes {
    normal (1), bothInterior (2), bothBoundary (3), interiorVsBoundary(4), boundaryVsInterior(5);

    private int number;

    IntersectsTypes(int number) {
        this.number = number;
    }

    public int getNumber() {
        return number;
    }
}
