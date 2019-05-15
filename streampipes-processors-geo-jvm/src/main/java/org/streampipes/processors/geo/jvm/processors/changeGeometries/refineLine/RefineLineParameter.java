package org.streampipes.processors.geo.jvm.processors.changeGeometries.refineLine;


import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;



public class RefineLineParameter extends EventProcessorBindingParams {

    private String wkt_string;
    private String epsg_code;
    private Double distance;


    public RefineLineParameter(DataProcessorInvocation graph, String wkt_String, String epsg_code, Double distance) {
        super(graph);
        this.wkt_string = wkt_String;
        this.epsg_code = epsg_code;
        this.distance = distance;
    }

    public String getWkt_string() {
        return wkt_string;
    }


    public String getEpsg_code() {
        return epsg_code;
    }


    public Double getDistance() {
        return distance;
    }
}
