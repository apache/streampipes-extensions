package org.streampipes.processors.geo.jvm.geofence;

import org.streampipes.model.graph.DataSinkInvocation;
import org.streampipes.wrapper.params.binding.EventSinkBindingParams;

public class StoreGeofenceParameters extends EventSinkBindingParams {


  private String geofence;



  public StoreGeofenceParameters(DataSinkInvocation graph, String geofence) {
    super(graph);

    this.geofence = geofence;

  }


  public String getGeofence() {
    return geofence;
  }
}
