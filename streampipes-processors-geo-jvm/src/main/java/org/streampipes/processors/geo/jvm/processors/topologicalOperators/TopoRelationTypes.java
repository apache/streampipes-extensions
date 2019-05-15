package org.streampipes.processors.geo.jvm.processors.topologicalOperators;

public enum TopoRelationTypes {
    Contains (1), CoveredBy (2), Covers (3), Crosses (4), Disjoint (5), Equals (6),  Intersects (7), Overlaps (8), Touches(9),  Within (10);



    private int number;


    TopoRelationTypes(int number) {
        this.number = number;
    }


    public int getNumber() {
        return number;
    }
}
