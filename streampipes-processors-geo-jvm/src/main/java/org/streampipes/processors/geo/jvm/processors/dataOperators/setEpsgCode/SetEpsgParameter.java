package org.streampipes.processors.geo.jvm.processors.dataOperators.setEpsgCode;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;



public class SetEpsgParameter extends EventProcessorBindingParams {


    private Integer epsg_code;

    public SetEpsgParameter(DataProcessorInvocation graph, Integer epsg_code) {
        super(graph);
        this.epsg_code = epsg_code;
    }


    public Integer getEpsg_code() {
        return epsg_code;
    }
}
