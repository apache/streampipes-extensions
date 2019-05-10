package org.streampipes.processors.geo.jvm.database.dataFromRaster.precipitation;


import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;


public class PrecipitationParameter extends EventProcessorBindingParams {

    private String wkt_string;
    private String epsg_code;
    private Integer year_start;
    private Integer year_end;
    private String type;


    public PrecipitationParameter(DataProcessorInvocation graph, String wkt_String, String epsg_code, Integer year_start, Integer year_end, String type) {
        super(graph);
        this.wkt_string = wkt_String;
        this.epsg_code = epsg_code;
        this.year_start = year_start;
        this.year_end = year_end;
        this.type = type;
    }

    public String getWkt_string() {
        return wkt_string;
    }


    public String getEpsg_code() {
        return epsg_code;
    }


    public Integer getYear_start() {
        return year_start;
    }

    public Integer getYear_end() {
        return year_end;
    }

    public String getType() {
        return type;
    }



}
