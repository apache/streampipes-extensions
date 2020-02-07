package org.apache.streampipes.processors.geo.jvm.jts.processors.setEPSG;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;



public class SetEpsgParameter extends EventProcessorBindingParams {


    private Integer epsg_value;

    public SetEpsgParameter(DataProcessorInvocation graph, Integer epsg_code) {
        super(graph);
        this.epsg_value = epsg_code;
    }


    public Integer getEpsg_value() {
        return epsg_value;
    }
}
