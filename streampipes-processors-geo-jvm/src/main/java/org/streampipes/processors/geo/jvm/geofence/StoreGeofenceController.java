package org.streampipes.processors.geo.jvm.geofence;

import org.streampipes.model.DataSinkType;
import org.streampipes.model.graph.DataSinkDescription;
import org.streampipes.model.graph.DataSinkInvocation;
import org.streampipes.sdk.builder.DataSinkBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.DataSinkParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.wrapper.standalone.ConfiguredEventSink;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventSinkDeclarer;

public class StoreGeofenceController extends StandaloneEventSinkDeclarer<StoreGeofenceParameters>  {

  protected final static String WKT_TEXT = "wkt_text";
  protected final static String EPSG_CODE = "epsg_code";


  protected final static String GEOFENCE = "geofence";
  protected final static String UNIT = "units";

  protected final static String EPA_NAME = "Geofence for Polygons and MultiPolygons";


  @Override
  public DataSinkDescription declareModel() {
    return DataSinkBuilder.create("org/streampipes/processors/geo/jvm/geofence", EPA_NAME,
        "Stores Polygons or MultiPolygons as geofence together with calculated area in a database.")
            .category(DataSinkType.STORAGE)
            .withAssets(Assets.DOCUMENTATION, Assets.ICON)
            .requiredStream(StreamRequirementsBuilder.create()
                    .requiredProperty(EpRequirements.anyProperty())
                    .build()
            )
            .requiredTextParameter(Labels.from(
                    GEOFENCE,
                    "Name of the Geofence",
                    "Set the name of the geofence. Name has to be unique"))


            .supportedFormats(SupportedFormats.jsonFormat())
            .supportedProtocols(SupportedProtocols.kafka(), SupportedProtocols.jms())
            .build();
  }


  @Override
  public ConfiguredEventSink<StoreGeofenceParameters> onInvocation(DataSinkInvocation graph,
                                                                   DataSinkParameterExtractor extractor) {


    String geofence = extractor.singleValueParameter(GEOFENCE, String.class);







    StoreGeofenceParameters params = new StoreGeofenceParameters(graph, geofence);

    return new ConfiguredEventSink<>(params, StoreGeofence::new);
  }


}
