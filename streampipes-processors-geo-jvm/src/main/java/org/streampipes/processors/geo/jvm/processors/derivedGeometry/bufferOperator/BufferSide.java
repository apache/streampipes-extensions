package org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator;

public enum BufferSide {
    Left (-1), Both (0), Right (1),
    ;

    private final int number;


    BufferSide(int number) {
        this.number = number;
    }


    public int getNumber() {
        return number;
    }
}
