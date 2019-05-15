package org.streampipes.processors.geo.jvm.window.collect.multipolygon;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class CollectMultiPolygonParameters extends EventProcessorBindingParams {

  private String wkt;
  private String epsg_Code;
  private String uid;
  private boolean dissolve;

  public CollectMultiPolygonParameters(DataProcessorInvocation graph, String wkt, String epsg_Code, String uid, boolean dissolve ) {
    super(graph);
    this.wkt = wkt;
    this.epsg_Code = epsg_Code;
    this.uid = uid;
    this.dissolve = dissolve;

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


  public boolean isDissolve() {
    return dissolve;
  }
}
