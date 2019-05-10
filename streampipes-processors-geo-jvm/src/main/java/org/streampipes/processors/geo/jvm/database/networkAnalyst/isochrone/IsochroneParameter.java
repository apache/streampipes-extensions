package org.streampipes.processors.geo.jvm.database.networkAnalyst.isochrone;


import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;


public class IsochroneParameter extends EventProcessorBindingParams {

    private String wkt_string;
    private String epsg_code;
    private Integer measure;
    private Boolean unit;


    public IsochroneParameter(DataProcessorInvocation graph, String wkt_String, String epsg_code, Integer measure, Boolean unit) {
        super(graph);
        this.wkt_string = wkt_String;
        this.epsg_code = epsg_code;
        this.measure = measure;
        this.unit = unit;

    }

    public String getWkt_string() {
        return wkt_string;
    }


    public String getEpsg_code() {
        return epsg_code;
    }

    public Integer getMeasure() {
        return measure;
    }

    public Boolean getUnit() {
        return unit;
    }
}
