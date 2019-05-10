package org.streampipes.processors.geo.jvm.database.dataFromRaster.pitch;


import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;


public class PitchParameter extends EventProcessorBindingParams {

    private String wkt_string;
    private String epsg_code;
    private Integer type;


    public PitchParameter(DataProcessorInvocation graph, String wkt_String, String epsg_code, Integer type) {
        super(graph);
        this.wkt_string = wkt_String;
        this.epsg_code = epsg_code;
        this.type = type;
    }

    public String getWkt_string() {
        return wkt_string;
    }

    public String getEpsg_code() {
        return epsg_code;
    }

    public Integer getType() {
        return type;
    }
}
