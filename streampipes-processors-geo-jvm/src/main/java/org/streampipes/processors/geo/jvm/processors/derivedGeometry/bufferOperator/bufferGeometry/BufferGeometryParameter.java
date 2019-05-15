package org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.bufferGeometry;


import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;



public class BufferGeometryParameter extends EventProcessorBindingParams {

    private String wkt_string;
    private String epsg_code;
    private Integer capStyle;  //gleich direkt als integer übertragen
    private Integer joinStyle;
    private Integer segments;
    private Double simplifyFactor;
    private Double distance;
    private Double mitreLimit;
    private Boolean singleSided;
    private Integer side;



    public BufferGeometryParameter(DataProcessorInvocation graph, String wkt_String, String epsg_code, Double distance, Integer capStyle, Integer joinStyle, Double mitreLimit, Integer segments, Double simplifyFactor, Boolean singleSided, Integer side) {
        super(graph);
        this.wkt_string = wkt_String;
        this.epsg_code = epsg_code;
        this.distance = distance;
        this.capStyle = capStyle;
        this.joinStyle = joinStyle;
        this.mitreLimit = mitreLimit;
        this.segments = segments;
        this.simplifyFactor = simplifyFactor;
        this.singleSided = singleSided;
        this.side = side;
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

    public Integer getJoinStyle() {
        return joinStyle;
    }

    public Double getMitreLimit() {
        return mitreLimit;
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

    public Integer getSide() {
        return side;
    }

    public Boolean getSingleSided() {
        return singleSided;
    }
}
