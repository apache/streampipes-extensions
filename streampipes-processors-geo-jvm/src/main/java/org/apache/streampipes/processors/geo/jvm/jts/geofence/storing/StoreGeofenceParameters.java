package org.apache.streampipes.processors.geo.jvm.jts.geofence.storing;

import org.apache.streampipes.model.graph.DataSinkInvocation;
import org.apache.streampipes.wrapper.params.binding.EventSinkBindingParams;

public class StoreGeofenceParameters extends EventSinkBindingParams {


  private String geofenceName;
  private String wkt;
  private String epsg;

  public StoreGeofenceParameters(DataSinkInvocation graph, String geofenceName, String wkt, String epsg) {
    super(graph);

    this.geofenceName = geofenceName;
    this.wkt = wkt;
    this.epsg = epsg;

  }

  public String getGeofenceName() {
    return geofenceName;
  }

  public String getWkt() {
    return wkt;
  }

  public String getEpsg() {
    return epsg;
  }

}
