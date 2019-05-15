package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.equals;

public enum EqualsTypes {
    equals (1), equalsExact (2);

    private int number;

    EqualsTypes(int number) {
        this.number = number;
    }

    public int getNumber() {
        return number;
    }
}
