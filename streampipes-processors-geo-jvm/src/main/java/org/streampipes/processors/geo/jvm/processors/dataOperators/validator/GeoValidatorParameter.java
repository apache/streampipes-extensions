package org.streampipes.processors.geo.jvm.processors.dataOperators.validator;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

import java.util.List;

public class GeoValidatorParameter extends EventProcessorBindingParams {

    private String wkt_string;
    private String epsg_code;
    private String output_choosing;
    List<String>  type;


    public GeoValidatorParameter(DataProcessorInvocation graph, String wkt_String, String epsg_code, String output_choosing, List<String> type) {
        super(graph);
        this.wkt_string = wkt_String;
        this.epsg_code = epsg_code;
        this.output_choosing = output_choosing;
        this.type = type;
    }

    public String getWkt_string() {
        return wkt_string;
    }

    public String getEpsg_code() {
        return epsg_code;
    }

    public String getOutput_choosing() {
        return output_choosing;
    }

    public List<String> getType() {
        return type;
    }

}
