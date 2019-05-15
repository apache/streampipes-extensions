package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.touches;

public enum TouchesTypes {
    normal (1), interior (2), boundary (3);

    private int number;

    TouchesTypes(int number) {
        this.number = number;
    }

    public int getNumber() {
        return number;
    }
}
