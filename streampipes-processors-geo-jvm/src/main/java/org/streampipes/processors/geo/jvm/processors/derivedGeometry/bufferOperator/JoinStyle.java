package org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator;

public enum JoinStyle {
    Round (1), Mitre (2), Bevel (3);


    private int number;

    JoinStyle(int number) {
        this.number = number;
    }


    public int getNumber() {
        return number;
    }
}

