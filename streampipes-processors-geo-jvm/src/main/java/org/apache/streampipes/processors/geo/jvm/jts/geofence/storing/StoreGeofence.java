package org.apache.streampipes.processors.geo.jvm.jts.geofence.storing;

import org.apache.streampipes.processors.geo.jvm.jts.helper.SpGeometryBuilder;
import org.locationtech.jts.geom.Geometry;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.logging.api.Logger;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.wrapper.context.EventSinkRuntimeContext;
import org.apache.streampipes.wrapper.runtime.EventSink;
import org.apache.streampipes.processors.geo.jvm.jts.geofence.SpGeofencelDatabase;


public class StoreGeofence extends SpGeofencelDatabase implements EventSink<StoreGeofenceParameters> {

  private static Logger LOG;
  public StoreGeofenceParameters storeGeofenceParameters;

  private String geofenceName;
  private String geom_wkt;
  private String epsg_code;


  @Override
  public void onInvocation(StoreGeofenceParameters parameters, EventSinkRuntimeContext runtimeContext) throws SpRuntimeException {

    LOG = parameters.getGraph().getLogger(StoreGeofenceParameters.class);
    this.geom_wkt = parameters.getWkt();
    this.epsg_code = parameters.getEpsg();
    this.geofenceName = parameters.getGeofenceName();

    initializeJdbc("^[a-zA-Z_][a-zA-Z0-9_]*$", geofenceName, LOG);
  }


  @Override
  public void onEvent(Event event) throws SpRuntimeException {

    String wkt = event.getFieldBySelector(geom_wkt).getAsPrimitive().getAsString();
    Integer epsg = event.getFieldBySelector(this.epsg_code).getAsPrimitive().getAsInt();
    Geometry geometry =  SpGeometryBuilder.createSPGeom(wkt, epsg);
    updateGeofenceTable(geometry);
  }

  @Override
  public void onDetach() throws SpRuntimeException {
    deleteTableEntry();
    closeAll();
  }


}
