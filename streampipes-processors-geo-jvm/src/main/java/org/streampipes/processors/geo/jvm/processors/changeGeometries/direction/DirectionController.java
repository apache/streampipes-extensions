package org.streampipes.processors.geo.jvm.processors.changeGeometries.direction;


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


public class DirectionController extends StandaloneEventProcessingDeclarer<DirectionParameter> {




    public final static String GEOMETRY_1 = "geometry1";
    public final static String GEOMETRY_2 = "geometry2";


    public final static String EPSG_GEOM_1 = "epsg_geom1";
    public final static String EPSG_GEOM_2 = "epsg_geom2";

    public final static String DIRECTION = "direction";
    public final static String UNIT = "unitOfDirection";
    public final static String ORIENTATION = "orientation";

    public final static String DECIMAL_CHOICE = "decimalNumber";
    public final static String WHO_with_WHOM = "who_with_whom";
    

    public final static String EPA_NAME = "Direction";
    

    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.changeGeometries.direction", EPA_NAME,
                        "Calculates the direction between two geometries in degree (azimuth) and direction e.g. SSW")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                .create()
                .requiredPropertyWithUnaryMapping(
                        EpRequirements.stringReq(),
                        Labels.from(
                                GEOMETRY_1,
                                "Geometry I",
                                "First Geometry"),
                        PropertyScope.NONE)

                .requiredPropertyWithUnaryMapping(
                        EpRequirements.numberReq(),
                        Labels.from(
                                EPSG_GEOM_1,
                                "EPSG Code I",
                                "EPSG code for geometry I"),
                        PropertyScope.NONE)


                .requiredPropertyWithUnaryMapping(
                        EpRequirements.stringReq(),
                        Labels.from(
                                GEOMETRY_2,
                                "Geometry II",
                                "Second Geometry"),
                        PropertyScope.NONE)

                .requiredPropertyWithUnaryMapping(
                        EpRequirements.numberReq(),
                        Labels.from(
                                EPSG_GEOM_2,
                                "EPSG Code II",
                                "EPSG code for geometry II"),
                        PropertyScope.NONE)
                .build()

                )
//                .requiredStream(StreamRequirementsBuilder
//                        .create()
//                        .requiredPropertyWithUnaryMapping(
//                                EpRequirements.stringReq(),
//                                Labels.from(
//                                        GEOMETRY_2,
//                                        "Geometry II",
//                                        "Second Geometry"),
//                                PropertyScope.NONE)
//
//                        .requiredPropertyWithUnaryMapping(
//                                EpRequirements.numberReq(),
//                                Labels.from(
//                                        EPSG_GEOM_2,
//                                        "EPSG Code II",
//                                        "EPSG code for geometry II"),
//                                PropertyScope.NONE)
//                        .build()
//                )

                .requiredSingleValueSelection(
                        Labels.from(
                                WHO_with_WHOM,
                                "Choose First and Second Geometry",
                                "Choose Geometry. First geometry will be checked against second"),
                        Options.from(
                                "Geometry 1 vs. Geometry 2",
                                "Geometry 2 vs. Geometry 1")
                )
                .requiredIntegerParameter(
                        Labels.from(
                                DECIMAL_CHOICE,
                                "decimal positions",
                                "Choose the amount of decimal Position."),
                        0, 10,1
                )
                .outputStrategy(OutputStrategies.append(
                        EpProperties.stringEp(
                                Labels.from(
                                        DIRECTION,
                                        "direction",
                                        "direction between A and B"),
                                DIRECTION, SO.Text),
                        EpProperties.stringEp(
                                Labels.from(
                                        UNIT,
                                        "unit of direction",
                                        "unit of direction"),
                                UNIT, SO.Text),
                        EpProperties.stringEp(
                                Labels.from(
                                        ORIENTATION,
                                        "orientation",
                                        "orientation between A and B"),
                                ORIENTATION, SO.Text)
                        )
                )
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<DirectionParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor)  {


        String epsg_geom1 = extractor.mappingPropertyValue(EPSG_GEOM_1);
        String geom1 = extractor.mappingPropertyValue(GEOMETRY_1);

        String epsg_geom2 = extractor.mappingPropertyValue(EPSG_GEOM_2);
        String geom2 = extractor.mappingPropertyValue(GEOMETRY_2);

        Integer decimalPosition = extractor.singleValueParameter(DECIMAL_CHOICE, Integer.class);



        String whoVsWhom = extractor.selectedSingleValue(WHO_with_WHOM, String.class);
        boolean vsChecker = false;

        if (whoVsWhom.equals("Geometry 1 vs. Geometry 2")){
            vsChecker = true;
        }


        DirectionParameter params = new DirectionParameter(graph, geom1, epsg_geom1, geom2, epsg_geom2, decimalPosition, vsChecker);

        return new ConfiguredEventProcessor<>(params, Direction::new);
    }
}
