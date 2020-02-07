package org.apache.streampipes.processors.geo.jvm.jts.processors.latLngToGeo;


import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class LatLngToGeoParameter extends EventProcessorBindingParams {

    private String epsg_value;
    private String lat;
    private String lng;

    public LatLngToGeoParameter(DataProcessorInvocation graph, String epsg_code, String lat, String lng) {
        super(graph);
        this.epsg_value = epsg_code;
        this.lat = lat;
        this.lng = lng;
    }


    public String getEpsg_value() {
        return epsg_value;
    }

    public String getLat() {
        return lat;
    }

    public String getLng() {
        return lng;
    }
}
