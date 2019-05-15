package org.streampipes.processors.geo.jvm.processors.derivedGeometry.overlayOperator;

public enum OverlayTypes {

    Intersection (1), Union (2), UnionLevel (3), Difference (4), SymDifference (5);


    private int number;


    OverlayTypes(int number) {
        this.number = number;
    }


    public int getNumber() {
        return number;
    }
}
