package org.streampipes.processors.geo.jvm.processors.dataOperators.latLngToGeo;

import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.sdk.utils.Assets;
import org.streampipes.vocabulary.Geo;
import org.streampipes.vocabulary.SO;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

public class LatLngToGeoController extends  StandaloneEventProcessingDeclarer<LatLngToGeoParameter> {


    public final static String LAT_FIELD = "lat_field";
    public final static String LNG_FIELD = "lng_field";
    public final static String EPSG_CODE = "epsg_code";
    public final static String WKT_TEXT = "wkt_text";

    public final static String EPA_NAME = "Create Point from Latitude and Longitude";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.dataOperators.latLngToGeo",
                        EPA_NAME,
                        "Creates a point geometry from Latitude and Longitude values")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(
                        StreamRequirementsBuilder
                                .create()
                                .requiredPropertyWithUnaryMapping(
                                        EpRequirements.domainPropertyReq(Geo.lat),
                                        Labels.from(LAT_FIELD,
                                                "Latitude field",
                                                "Latitude value"),
                                        PropertyScope.NONE
                                )
                                .requiredPropertyWithUnaryMapping(
                                        EpRequirements.domainPropertyReq(Geo.lng),
                                        Labels.from(LNG_FIELD,
                                                "Longitude field",
                                                "Longitude value"),
                                        PropertyScope.NONE
                                )
                                .requiredPropertyWithUnaryMapping(
                                        EpRequirements.numberReq(),
                                        Labels.from(EPSG_CODE, "EPSG Field", "EPSG Code for SRID"),
                                        PropertyScope.NONE
                                )
                                .build()
                )
                .outputStrategy(OutputStrategies.append(EpProperties.stringEp(
                        Labels.from(
                                "point_string",
                                "wkt_string",
                                "wkt geometry string"),
                        WKT_TEXT,
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
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);

        LatLngToGeoParameter params = new LatLngToGeoParameter(graph, epsg_code, lat, lng);

        return new ConfiguredEventProcessor<>(params, LatLngToGeo::new);
    }
}
