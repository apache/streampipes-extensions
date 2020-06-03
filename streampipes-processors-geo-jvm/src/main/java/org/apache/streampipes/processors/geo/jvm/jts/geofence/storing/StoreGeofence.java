/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

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

  private String host;
  private Integer port;
  private String user;
  private String password;
  private String schema;
  private String driver;
  private String urlName;
  private String allowedRegEx;
  private  String geofenceTable;




  @Override
  public void onInvocation(StoreGeofenceParameters parameters, EventSinkRuntimeContext runtimeContext) throws SpRuntimeException {

    LOG = parameters.getGraph().getLogger(StoreGeofenceParameters.class);
    this.geom_wkt = parameters.getWkt();
    this.epsg_code = parameters.getEpsg();
    this.geofenceName = parameters.getGeofenceName();


    this.host = "localhost";
    this.port = 54321;
    this.databaseName = "geo_streampipes";
    this.user = "geo_streampipes";
    this.password = "bQgu\"FUR_VH6z>~j";
    this.schema = "geofence";
    this.driver = "org.postgresql.Driver";
    this.urlName = "postgresql";
    this.geofenceTable = "geofences";

    initializeJdbc(
        parameters.getGraph().getInputStreams().get(0).getEventSchema().getEventProperties(),
        host,
        port,
        databaseName,
        geofenceTable,
        user,
        password,
        "^[a-zA-Z_][a-zA-Z0-9_]*$",
        driver,
        urlName,
        LOG,
        schema,
        false,
        geofenceName);
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
