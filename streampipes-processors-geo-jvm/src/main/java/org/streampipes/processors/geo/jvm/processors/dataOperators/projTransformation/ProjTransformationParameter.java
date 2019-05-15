package org.streampipes.processors.geo.jvm.processors.dataOperators.projTransformation;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class ProjTransformationParameter extends EventProcessorBindingParams {

    private String wkt_string;
    private String source_epsg;
    private Integer target_epsg;


    public ProjTransformationParameter(DataProcessorInvocation graph, String wkt_String, String source_epsg, Integer target_epsg) {
        super(graph);
        this.wkt_string = wkt_String;
        this.source_epsg = source_epsg;
        this.target_epsg = target_epsg;
    }

    public String getWkt_string() {
        return wkt_string;
    }

    public String getEpsgCode() {
        return source_epsg;
    }


    public Integer getTarget_epsg() {
        return target_epsg;
    }
}
