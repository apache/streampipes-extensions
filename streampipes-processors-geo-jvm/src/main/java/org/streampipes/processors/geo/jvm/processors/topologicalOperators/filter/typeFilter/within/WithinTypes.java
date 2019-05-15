package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.within;

public enum WithinTypes {
    within (1), withinComplete (2);

    private int number;

    WithinTypes(int number) {
        this.number = number;
    }

    public int getNumber() {
        return number;
    }
}
