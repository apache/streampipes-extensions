package org.streampipes.processors.geo.jvm.processors.measureOperator.areaCalc;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;


public class AreaCalcParameter extends EventProcessorBindingParams {
    private String wkt_string;
    private String epsg_code;
    private Integer decimalPos;
    private Integer unit;


    public AreaCalcParameter(DataProcessorInvocation graph, String wkt_String, String epsg_code, Integer decimalPos, Integer unit ) {
        super(graph);
        this.wkt_string = wkt_String;
        this.epsg_code = epsg_code;
        this.decimalPos = decimalPos;
        this.unit = unit;
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


