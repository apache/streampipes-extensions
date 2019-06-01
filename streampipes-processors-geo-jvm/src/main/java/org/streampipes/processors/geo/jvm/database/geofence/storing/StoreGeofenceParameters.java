package org.streampipes.processors.geo.jvm.database.geofence.storing;

import org.streampipes.model.graph.DataSinkInvocation;
import org.streampipes.wrapper.params.binding.EventSinkBindingParams;

public class StoreGeofenceParameters extends EventSinkBindingParams {


  private String geofence;
  private String wkt_string;
  private String epsg_code;
  private Integer unit;
  private String m_value;



  public StoreGeofenceParameters(DataSinkInvocation graph, String geofence, String wkt_string, String epsg_code, Integer unit, String m_value) {
    super(graph);

    this.geofence = geofence;
    this.wkt_string = wkt_string;
    this.epsg_code = epsg_code;
    this.unit = unit;
    this.m_value = m_value;

  }


  public String getGeofence() {
    return geofence;
  }


  public String getWkt_string() {
    return wkt_string;
  }

  public String getEpsg_code() {
    return epsg_code;
  }

  public Integer getUnit() {
    return unit;
  }

  public String getM_value() {
    return m_value;
  }
}
