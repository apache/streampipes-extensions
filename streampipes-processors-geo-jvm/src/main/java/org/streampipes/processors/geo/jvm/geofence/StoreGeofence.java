package org.streampipes.processors.geo.jvm.geofence;


import org.streampipes.commons.exceptions.SpRuntimeException;
import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.processors.geo.jvm.geofence.helper.JdbcGeofence;
import org.streampipes.wrapper.context.EventSinkRuntimeContext;
import org.streampipes.wrapper.runtime.EventSink;


public class StoreGeofence implements EventSink<StoreGeofenceParameters> {


  private static Logger LOG;
  public StoreGeofenceParameters storeGeofenceParameters;

  private String geofenceName;
  private JdbcGeofence jdbcClient;



  @Override
  public void onInvocation(StoreGeofenceParameters parameters, EventSinkRuntimeContext runtimeContext) throws SpRuntimeException {


    this.storeGeofenceParameters = parameters;
    this.geofenceName = storeGeofenceParameters.getGeofence();
    LOG = storeGeofenceParameters.getGraph().getLogger(StoreGeofence.class);


    String host = Config.INSTANCE.getPostgresHost();
    Integer port = Integer.valueOf(Config.INSTANCE.getPostgresPort());
    String dbName = Config.INSTANCE.getPostgresDatabase();
    String user = Config.INSTANCE.getPostgresUser();
    String password = Config.INSTANCE.getPostgresPassword();



    this.jdbcClient = new JdbcGeofence(
            parameters.getGraph().getInputStreams().get(0).getEventSchema().getEventProperties(),
            host,
            port,
            dbName,
            user,
            password,
            geofenceName,
            "^[a-zA-Z_][a-zA-Z0-9_]*$",
            "org.postgresql.Driver",
            "postgresql",
            LOG
    );


    // referenced to geofenceName because if name is already taken, it will be updated in the constructor
    jdbcClient.createGeofenceTableEntries();



  }


  @Override
  public void onEvent(Event event) {

    jdbcClient.update(event);

  }

  @Override
  public void onDetach() {
    jdbcClient.deleteTableEntry();
    jdbcClient.closeAll();
  }




}
