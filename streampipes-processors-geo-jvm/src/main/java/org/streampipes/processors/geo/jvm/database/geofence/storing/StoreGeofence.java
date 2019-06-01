package org.streampipes.processors.geo.jvm.database.geofence.storing;


import org.locationtech.jts.geom.Geometry;
import org.streampipes.commons.exceptions.SpRuntimeException;
import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.processors.geo.jvm.config.GeoJvmConfig;
import org.streampipes.processors.geo.jvm.database.geofence.helper.SpInternalDatabase;
import org.streampipes.wrapper.context.EventSinkRuntimeContext;
import org.streampipes.wrapper.runtime.EventSink;

import java.sql.Connection;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.createSPGeom;


public class StoreGeofence implements EventSink<StoreGeofenceParameters> {


  private static Logger LOG;
  public StoreGeofenceParameters storeGeofenceParameters;

  private String geofenceName;
  private Connection conn;
  private SpInternalDatabase db;
  public Integer unit;



  @Override
  public void onInvocation(StoreGeofenceParameters parameters, EventSinkRuntimeContext runtimeContext) throws SpRuntimeException {


    this.storeGeofenceParameters = parameters;
    this.geofenceName = storeGeofenceParameters.getGeofence();
    this.unit = storeGeofenceParameters.getUnit();

    LOG = storeGeofenceParameters.getGraph().getLogger(StoreGeofence.class);


    String host = GeoJvmConfig.INSTANCE.getPostgresHost();
    Integer port = Integer.valueOf(GeoJvmConfig.INSTANCE.getPostgresPort());
    String dbName = GeoJvmConfig.INSTANCE.getPostgresDatabase();
    String user = GeoJvmConfig.INSTANCE.getPostgresUser();
    String password = GeoJvmConfig.INSTANCE.getPostgresPassword();



    db = new SpInternalDatabase(host, port, dbName, user, password );
    conn = db.connect();

    //if geofencename is already in use geofencename will be changed with added number
    int count = 0;
    // if name will be changed, it has to be changed global as well
    geofenceName = db.createTableEntries(conn, geofenceName, count);


  }


  @Override
  public void onEvent(Event event) {

    String wkt = event.getFieldBySelector(storeGeofenceParameters.getWkt_string()).getAsPrimitive().getAsString();
    Integer epsgCode = event.getFieldBySelector(storeGeofenceParameters.getEpsg_code()).getAsPrimitive().getAsInt();
    Double m_value = event.getFieldBySelector(storeGeofenceParameters.getM_value()).getAsPrimitive().getAsDouble();
    Geometry geometry =  createSPGeom(wkt, epsgCode);

    db.updateGeofenceTable(conn, geofenceName, geometry, unit, m_value);
  }

  @Override
  public void onDetach() {
    db.deleteTableEntry(conn, geofenceName);
    db.closeConnection(conn);
  }




}
