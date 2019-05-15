package org.streampipes.processors.geo.jvm.processors.dataOperators.latLngToGeo;


import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class LatLngToGeoParameter extends EventProcessorBindingParams {

    private String epsg_code;
    private String lat;
    private String lng;

    public LatLngToGeoParameter(DataProcessorInvocation graph, String epsg_code, String lat, String lng) {
        super(graph);
        this.epsg_code = epsg_code;
        this.lat = lat;
        this.lng = lng;
    }


    public String getEpsg_code() {
        return epsg_code;
    }

    public String getLat() {
        return lat;
    }

    public String getLng() {
        return lng;
    }
}
