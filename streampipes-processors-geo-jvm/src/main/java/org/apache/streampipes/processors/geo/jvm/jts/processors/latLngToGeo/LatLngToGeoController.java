package org.apache.streampipes.processors.geo.jvm.jts.processors.latLngToGeo;

import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.sdk.utils.Assets;
import org.streampipes.vocabulary.SO;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

public class LatLngToGeoController extends  StandaloneEventProcessingDeclarer<LatLngToGeoParameter> {


    public final static String LAT_FIELD = "lat_field";
    public final static String LNG_FIELD = "lng_field";
    public final static String EPSG = "EPSG";
    public final static String WKT = "geom_wkt";
    public final static String EPA_NAME = "Create Point from Latitude and Longitude";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.apache.streampipes.processors.geo.jvm.jts.processors.latLngToGeo",
                        EPA_NAME,
                        "Creates a point geometry from Latitude and Longitude values")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(
                        StreamRequirementsBuilder
                                .create()
                                .requiredPropertyWithUnaryMapping(
                                        EpRequirements.numberReq(),
                                        Labels.from(LAT_FIELD,
                                                "Latitude field",
                                                "Latitude value"),
                                        PropertyScope.NONE
                                )
                                .requiredPropertyWithUnaryMapping(
                                        EpRequirements.numberReq(),
                                        Labels.from(LNG_FIELD,
                                                "Longitude field",
                                                "Longitude value"),
                                        PropertyScope.NONE
                                )
                                .requiredPropertyWithUnaryMapping(
                                        EpRequirements.numberReq(),
                                        Labels.from(EPSG, "EPSG Field", "EPSG Code for SRID"),
                                        PropertyScope.NONE
                                )
                                .build()
                )
                .outputStrategy(OutputStrategies.append(EpProperties.stringEp(
                        Labels.from(
                                "point_wkt",
                                "wkt",
                                "wkt point from long lat values"),
                    WKT,
                        SO.Text))
                )

                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<LatLngToGeoParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {


        String lat = extractor.mappingPropertyValue(LAT_FIELD);
        String lng = extractor.mappingPropertyValue(LNG_FIELD);
        String epsg_value = extractor.mappingPropertyValue(EPSG);

        LatLngToGeoParameter params = new LatLngToGeoParameter(graph, epsg_value, lat, lng);

        return new ConfiguredEventProcessor<>(params, LatLngToGeo::new);
    }
}
