package org.streampipes.processors.geo.jvm.processors.derivedGeometry.convexHull;



import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;



public class ConvexHullParameter extends EventProcessorBindingParams {

    private String wkt_string;
    private String epsg_code;


    public ConvexHullParameter(DataProcessorInvocation graph, String wkt_String, String epsg_code) {
        super(graph);
        this.wkt_string = wkt_String;
        this.epsg_code = epsg_code;
    }

    public String getWkt_string() {
        return wkt_string;
    }


    public String getEpsg_code() {
        return epsg_code;
    }
}
