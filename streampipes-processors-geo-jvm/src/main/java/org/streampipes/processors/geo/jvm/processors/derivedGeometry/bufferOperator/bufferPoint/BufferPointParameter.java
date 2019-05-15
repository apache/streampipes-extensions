package org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.bufferPoint;


import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;



public class BufferPointParameter extends EventProcessorBindingParams {

    private String wkt_string;
    private String epsg_code;


    private Integer capStyle;
    private Integer segments;

    private Double simplifyFactor;
    private Double distance;



    public BufferPointParameter(DataProcessorInvocation graph, String wkt_String, String epsg_code, Double distance, Integer capStyle,  Integer segments, Double simplifyFactor) {
        super(graph);
        this.wkt_string = wkt_String;
        this.epsg_code = epsg_code;
        this.distance = distance;
        this.capStyle = capStyle;
        this.segments = segments;
        this.simplifyFactor = simplifyFactor;

    }

    public String getWkt_string() {
        return wkt_string;
    }

    public String getEpsg_code() {
        return epsg_code;
    }

    public Integer getCapStyle() {
        return capStyle;
    }

    public Integer getSegments() {
        return segments;
    }

    public Double getSimplifyFactor() {
        return simplifyFactor;
    }

    public Double getDistance() {
        return distance;
    }




}
