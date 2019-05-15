package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.equals;


import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;


public class EqualsTopoController extends StandaloneEventProcessingDeclarer<EqualsTopoParameter> {


    public final static String GEOMETRY_1 = "geometry1";
    public final static String GEOMETRY_2 = "geometry2";


    public final static String EPSG_GEOM_1 = "epsg_geom1";
    public final static String EPSG_GEOM_2 = "epsg_geom2";

    public final static String TYPE = "topoRelationType";
    public final static String WHO_with_WHOM = "who_with_whom";


    public final static String EPA_NAME = "Equals Topology Filter";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.equals",
                        EPA_NAME,
                        "Filters Topology Equals results ")
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
                                        "EPSG Field I",
                                        "EPSG code for geometry I "),
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
                                        "EPSG Field II",
                                        "EPSG code for geometry II"),
                                PropertyScope.NONE)
                        .build()
                )
                .requiredSingleValueSelection(
                        Labels.from(
                                TYPE,
                                "Topology Relation Chooser",
                                "Choose a topology Operator. Geometry I will be tested with Geometry II"),
                        Options.from(
                                EqualsTypes.equals.name(),
                                EqualsTypes.equalsExact.name()
                        )
                )

                .requiredSingleValueSelection(
                        Labels.from(
                                WHO_with_WHOM,
                                "Choose First and Second Geometry",
                                "Choose Geometry. First geometry will be checked against second"),
                        Options.from(
                                "Geometry 1 vs. Geometry 2",
                                "Geometry 2 vs. Geometry 1")
                )
                .outputStrategy(OutputStrategies.keep())
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<EqualsTopoParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor){



        String epsg_geom1 = extractor.mappingPropertyValue(EPSG_GEOM_1);
        String geom1 = extractor.mappingPropertyValue(GEOMETRY_1);


        String epsg_geom2 = extractor.mappingPropertyValue(EPSG_GEOM_2);
        String geom2 = extractor.mappingPropertyValue(GEOMETRY_2);


        String whoVsWhom = extractor.selectedSingleValue(WHO_with_WHOM, String.class);


        boolean vsChecker = false;
        if (whoVsWhom.equals("Geometry 1 vs. Geometry 2")){
            vsChecker = true;
        }

        String read_type = extractor.selectedSingleValue(TYPE, String.class);



        int type = 1;
        if (read_type.equals(EqualsTypes.equalsExact.name())){
            type = EqualsTypes.equalsExact.getNumber();
        }


        EqualsTopoParameter params = new EqualsTopoParameter(graph, geom1, epsg_geom1, geom2, epsg_geom2, type, vsChecker);

        return new ConfiguredEventProcessor<>(params, EqualsTopo::new);
    }
}
