package org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator;

public enum CapStyle {
    Round (1), Flat (2), Square (3);


    private int number;

    CapStyle(int number) {
        this.number = number;
    }


    public int getNumber() {
        return number;
    }
}
