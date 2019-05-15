package org.streampipes.processors.geo.jvm.database.networkAnalyst.routing.twoManual;


import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.vocabulary.SO;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;


public class Routing_input_inputController extends StandaloneEventProcessingDeclarer<Routing_input_inputParameter> {


    public final static String LAT_START = "LAT_Source";
    public final static String LNG_START = "LNG_Source";

    public final static String LAT_DESTINATION = "LAT_Dest";
    public final static String LNG_DESTINATION = "LNG_Dest";

    public final static String ROUTING_TEXT = "routing_text";
    public final static String EPA_NAME = "Routing_input_input";





    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.database.networkAnalyst.routing.twoManual", EPA_NAME,
                        "Calculates a routing LineString from two manual inputs")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredProperty(EpRequirements.anyProperty())
                        .build()
                )
                .requiredFloatParameter(
                        Labels.from(
                                LAT_START,
                                "Start Latitude value",
                                ""),
                        48.865979f)

                .requiredFloatParameter(
                        Labels.from(
                                LNG_START,
                                "Start Longitude value",
                                ""),
                        8.190295f)


                .requiredFloatParameter(
                        Labels.from(
                                LAT_DESTINATION,
                                "Destination Latitude value",
                                ""),
                        48.861264f)

                .requiredFloatParameter(
                        Labels.from(
                                LNG_DESTINATION,
                                "Destination Longitude value",
                                ""),
                        8.215521f)


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
    public ConfiguredEventProcessor<Routing_input_inputParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor)  {


        Double start_lat = extractor.singleValueParameter(LAT_START, Double.class);
        Double start_lng = extractor.singleValueParameter(LNG_START, Double.class);

        Double dest_lat = extractor.singleValueParameter(LAT_DESTINATION, Double.class);
        Double dest_lng = extractor.singleValueParameter(LNG_DESTINATION, Double.class);

        Routing_input_inputParameter params = new Routing_input_inputParameter(graph, start_lat,start_lng, dest_lat, dest_lng);

        return new ConfiguredEventProcessor<>(params, Routing_input_input::new);
    }
}
