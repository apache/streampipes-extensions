package org.streampipes.processors.geo.jvm.database.networkAnalyst.routing.oneManual;

import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.vocabulary.SO;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;


public class RoutingSingleInputController extends StandaloneEventProcessingDeclarer<RoutingSingleInputParameter> {


    private final static String LAT_COORD = "LAT_Coord";
    private final static String LNG_COORD = "LNG_Coord";
    private final static String CHOOSER = "chooser";

    public final static String WKT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";


    public final static String ROUTING_TEXT = "routing_text";
    public final static String EPA_NAME = "Routing with Single Input";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.database.networkAnalyst.routing.oneManual", EPA_NAME,
                        "Calculates a routing LineString from a point geometry and a manual input")
                .category(DataProcessorType.GEO)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
                                Labels.from(WKT,
                                        "WKT_String",
                                        "Contains wkt String"),
                                PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.from(EPSG_CODE,
                                        "EPSG Code",
                                        "EPSG Code"),
                                PropertyScope.NONE)
                        .build()
                )

                .requiredFloatParameter(
                        Labels.from(
                                LAT_COORD,
                                "Latitude",
                                ""),
                        48.865979f)

                .requiredFloatParameter(
                        Labels.from(
                                LNG_COORD,
                                "Longitudes",
                                ""),
                        8.190295f)


                .requiredSingleValueSelection(
                        Labels.from(
                                CHOOSER,
                                "Choose manual Routing position",
                                "choose coords as start or destination routing point "),
                        Options.from(
                                "Start",
                                "Destination"
                        )
                )


                .outputStrategy(
                        OutputStrategies.append(
                                EpProperties.stringEp(
                                        Labels.from(
                                                "routing_wkt",
                                                "routing_wkt",
                                                "wkt string from routing geometry result"),
                                        ROUTING_TEXT,
                                        SO.Text)
                        )
                )
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }



    @Override
    public ConfiguredEventProcessor<RoutingSingleInputParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {


        Double lat = extractor.singleValueParameter(LAT_COORD, Double.class);
        Double lng = extractor.singleValueParameter(LNG_COORD, Double.class);

        String chooser = extractor.selectedSingleValue(CHOOSER, String.class);

        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);
        String wkt = extractor.mappingPropertyValue(WKT);

        RoutingSingleInputParameter params = new RoutingSingleInputParameter(graph, lat,lng, chooser, wkt, epsg_code);

        return new ConfiguredEventProcessor<>(params, RoutingSingleInput::new);
    }
}
