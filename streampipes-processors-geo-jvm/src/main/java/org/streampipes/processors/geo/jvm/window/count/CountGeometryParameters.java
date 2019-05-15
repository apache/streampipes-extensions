package org.streampipes.processors.geo.jvm.window.count;


import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class CountGeometryParameters extends EventProcessorBindingParams {


  private String wkt_string;
  private String epsg_Code;
  private String uid;


  public CountGeometryParameters(DataProcessorInvocation graph, String wkt_String, String epsg_Code, String uid ) {
    super(graph);
    this.wkt_string = wkt_String;
    this.epsg_Code = epsg_Code;
    this.uid = uid;
  }


  public String getWkt_string() {
    return wkt_string;
  }

  public String getEpsg_Code() {
    return epsg_Code;
  }

  public String getUid() {
    return uid;
  }


}
