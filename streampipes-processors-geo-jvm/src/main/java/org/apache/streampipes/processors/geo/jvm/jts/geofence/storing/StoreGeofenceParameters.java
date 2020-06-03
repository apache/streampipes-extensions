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
