package org.streampipes.processors.geo.jvm.window.collect.multiline;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class CollectMultiLineStringParameters extends EventProcessorBindingParams {

  private String wkt;
  private String epsg_Code;
  private String uid;

  public CollectMultiLineStringParameters(DataProcessorInvocation graph, String wkt, String epsg_Code, String uid ) {
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
