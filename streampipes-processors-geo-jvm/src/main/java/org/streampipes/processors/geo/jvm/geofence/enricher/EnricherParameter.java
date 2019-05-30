package org.streampipes.processors.geo.jvm.geofence.enricher;



import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;


public class EnricherParameter extends EventProcessorBindingParams {


    private String geofence_name;

    public EnricherParameter(DataProcessorInvocation graph, String geofence_name) {
        super(graph);
        this.geofence_name = geofence_name;
    }


    public String getGeofence_name() {
        return geofence_name;
    }
}
