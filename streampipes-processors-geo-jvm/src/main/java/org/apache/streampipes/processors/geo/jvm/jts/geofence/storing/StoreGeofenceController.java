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

import org.apache.streampipes.model.DataSinkType;
import org.apache.streampipes.model.graph.DataSinkDescription;
import org.apache.streampipes.model.graph.DataSinkInvocation;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.DataSinkBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.extractor.DataSinkParameterExtractor;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.wrapper.standalone.ConfiguredEventSink;
import org.apache.streampipes.wrapper.standalone.declarer.StandaloneEventSinkDeclarer;

public class StoreGeofenceController extends StandaloneEventSinkDeclarer<StoreGeofenceParameters> {

  protected final static String GEOMETRY_KEY = "geometry-key";
  protected final static String EPSG_KEY = "epsg-key";
  protected final static String GEOFENCE_KEY = "geofence-key";

  @Override
  public DataSinkDescription declareModel() {
    return DataSinkBuilder.create("org.apache.streampipes.processors.geo.jvm.jts.geofence.storing")
        .withLocales(Locales.EN)
        .withAssets(Assets.DOCUMENTATION, Assets.ICON)
        .category(DataSinkType.STORAGE)
        .requiredStream(StreamRequirementsBuilder
            .create()
            .requiredPropertyWithUnaryMapping(
                EpRequirements.domainPropertyReq("http://www.opengis.net/ont/geosparql#Geometry"),
                Labels.withId(GEOMETRY_KEY), PropertyScope.MEASUREMENT_PROPERTY
            )
            .requiredPropertyWithUnaryMapping(
                EpRequirements.domainPropertyReq("http://data.ign.fr/def/ignf#CartesianCS"),
                Labels.withId(EPSG_KEY), PropertyScope.MEASUREMENT_PROPERTY
            )
            .build()
        )
        .requiredTextParameter(Labels.withId(GEOFENCE_KEY)
        )
        .supportedFormats(SupportedFormats.jsonFormat())
        .supportedProtocols(SupportedProtocols.kafka(), SupportedProtocols.jms())
        .build();
  }

  @Override
  public ConfiguredEventSink<StoreGeofenceParameters> onInvocation(DataSinkInvocation graph, DataSinkParameterExtractor extractor) {

    String geofenceName = extractor.singleValueParameter(GEOFENCE_KEY, String.class);
    String geom_wkt = extractor.mappingPropertyValue(GEOMETRY_KEY);
    String epsg = extractor.mappingPropertyValue(EPSG_KEY);

    StoreGeofenceParameters params = new StoreGeofenceParameters(graph, geofenceName, geom_wkt, epsg);

    return new ConfiguredEventSink<>(params, StoreGeofence::new);
  }


}
