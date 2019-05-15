package org.streampipes.processors.geo.jvm.processors.measureOperator.polygonPerimeterCalc;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;



public class PolygonPerimeterParameter extends EventProcessorBindingParams {

    private String wkt_string;
    private String epsg_code;
    private Integer decimalPos;
    private Integer unit;


    public PolygonPerimeterParameter(DataProcessorInvocation graph, String wkt_String, String epsg_code, Integer unit, Integer decimalPos) {
        super(graph);
        this.wkt_string = wkt_String;
        this.epsg_code = epsg_code;
        this.unit = unit;
        this.decimalPos = decimalPos;

    }

    public String getWkt_string() {
        return wkt_string;
    }

    public String getEpsg_code() {
        return epsg_code;
    }

    public Integer getDecimalPos() {
        return decimalPos;
    }

    public Integer getUnit() {
        return unit;
    }
}
