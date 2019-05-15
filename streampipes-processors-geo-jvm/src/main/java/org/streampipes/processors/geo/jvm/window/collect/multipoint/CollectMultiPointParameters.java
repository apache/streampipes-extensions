package org.streampipes.processors.geo.jvm.window.collect.multipoint;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class CollectMultiPointParameters extends EventProcessorBindingParams {

  private String wkt;
  private String epsg_Code;
  private String uid;

  public CollectMultiPointParameters(DataProcessorInvocation graph, String wkt, String epsg_Code, String uid ) {
    super(graph);
    this.wkt = wkt;
    this.epsg_Code = epsg_Code;
    this.uid = uid;

  }

  public String getWkt() {
    return wkt;
  }

  public String getEpsg_Code() {
    return epsg_Code;
  }

  public String getUid() {
    return uid;
  }
}
