package org.streampipes.processors.geo.jvm.database.networkAnalyst.routing.twoManual;


import org.locationtech.jts.geom.Point;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public class Routing_input_inputParameter extends EventProcessorBindingParams {


    private Point start;
    private Point dest;



    public Routing_input_inputParameter(DataProcessorInvocation graph, Double start_lat, Double start_lng, Double dest_lat, Double dest_lng) {
        super(graph);
        this.start = (Point) createSPGeom(start_lng, start_lat,  4326);
        this.dest = (Point) createSPGeom(dest_lng, dest_lat,  4326);
    }

    public Point getStart() {
        return start;
    }

    public Point getDest() {
        return dest;
    }


}
