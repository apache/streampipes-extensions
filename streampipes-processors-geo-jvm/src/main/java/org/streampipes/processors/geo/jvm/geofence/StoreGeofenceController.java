package org.streampipes.processors.geo.jvm.geofence;

import org.streampipes.model.DataSinkType;
import org.streampipes.model.graph.DataSinkDescription;
import org.streampipes.model.graph.DataSinkInvocation;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.processors.geo.jvm.helpers.AreaSpOperator;
import org.streampipes.sdk.builder.DataSinkBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.DataSinkParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.sdk.utils.Assets;
import org.streampipes.wrapper.standalone.ConfiguredEventSink;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventSinkDeclarer;

public class StoreGeofenceController extends StandaloneEventSinkDeclarer<StoreGeofenceParameters>  {

  protected final static String WKT_TEXT = "wkt_text";
  protected final static String EPSG_CODE = "epsg_code";
  protected final static String M_VALUE = "m_value";


  protected final static String GEOFENCE = "geofence";
  protected final static String UNIT = "units";

  protected final static String EPA_NAME = "Geofence for Polygons and MultiPolygons";


  @Override
  public DataSinkDescription declareModel() {
    return DataSinkBuilder.create("org/streampipes/processors/geo/jvm/geofence", EPA_NAME,
        "Stores Polygons or MultiPolygons as geofence together with calculated area in a database.")
            .category(DataSinkType.STORAGE)
            .withAssets(Assets.DOCUMENTATION, Assets.ICON)
            .requiredStream(StreamRequirementsBuilder
                    .create()
                    .requiredPropertyWithUnaryMapping(
                            EpRequirements.stringReq(),
                            Labels.from(
                                    WKT_TEXT,
                                    "WKT_String",
                                    "Contains wkt String"),
                            PropertyScope.NONE
                    )
                    .requiredPropertyWithUnaryMapping(
                            EpRequirements.numberReq(),
                            Labels.from(
                                    EPSG_CODE,
                                    "EPSG Code",
                                    "EPSG Code for SRID"),
                            PropertyScope.NONE
                    )
                    .requiredPropertyWithUnaryMapping(
                            EpRequirements.numberReq(),
                            Labels.from(
                                    M_VALUE,
                                    "M Value",
                                    "extra value that will be stored in geofence table"),
                            PropertyScope.NONE
                    )
                    .build()
            )
            .requiredSingleValueSelection(
                    Labels.from(
                            UNIT,
                            "Goal Unit",
                            "Select the unit of your choice"),
                    Options.from(
                            AreaSpOperator.ValidAreaUnits.squareMeter.name(),
                            AreaSpOperator.ValidAreaUnits.squareKM.name(),
                            AreaSpOperator.ValidAreaUnits.hectar.name(),
                            AreaSpOperator.ValidAreaUnits.ar.name()
                    )
            )
            .requiredTextParameter(Labels.from(
                    GEOFENCE,
                    "Name of the Geofence",
                    "Set the name of the geofence. Name has to be unique")
            )


            .supportedFormats(SupportedFormats.jsonFormat())
            .supportedProtocols(SupportedProtocols.kafka(), SupportedProtocols.jms())
            .build();
  }


  @Override
  public ConfiguredEventSink<StoreGeofenceParameters> onInvocation(DataSinkInvocation graph,DataSinkParameterExtractor extractor) {


    String geofence = extractor.singleValueParameter(GEOFENCE, String.class);

    String wkt_String = extractor.mappingPropertyValue(WKT_TEXT);
    String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);
    String m_value = extractor.mappingPropertyValue(M_VALUE);


    String chosenUnit = extractor.selectedSingleValue(UNIT, String.class);
    int unit = 1;
    if (chosenUnit.equals(AreaSpOperator.ValidAreaUnits.squareKM.name())){
      unit = AreaSpOperator.ValidAreaUnits.squareKM.getNumber();
    } else if (chosenUnit.equals(AreaSpOperator.ValidAreaUnits.hectar.name())){
      unit = AreaSpOperator.ValidAreaUnits.hectar.getNumber();
    } else if (chosenUnit.equals(AreaSpOperator.ValidAreaUnits.ar.name())){
      unit = AreaSpOperator.ValidAreaUnits.ar.getNumber();
    }

    StoreGeofenceParameters params = new StoreGeofenceParameters(graph, geofence, wkt_String, epsg_code, unit, m_value);

    return new ConfiguredEventSink<>(params, StoreGeofence::new);
  }


}
