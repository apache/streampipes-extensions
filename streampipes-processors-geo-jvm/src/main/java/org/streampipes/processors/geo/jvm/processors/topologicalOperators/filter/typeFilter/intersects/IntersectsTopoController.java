package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.intersects;


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


public class IntersectsTopoController extends StandaloneEventProcessingDeclarer<IntersectsTopoParameter> {


    public final static String GEOMETRY_1 = "geometry1";
    public final static String GEOMETRY_2 = "geometry2";


    public final static String EPSG_GEOM_1 = "epsg_geom1";
    public final static String EPSG_GEOM_2 = "epsg_geom2";

    public final static String TYPE = "topoRelationType";
    public final static String WHO_with_WHOM = "who_with_whom";


    public final static String EPA_NAME = "Intersects Topology Filter";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.intersects",
                        EPA_NAME,
                        "Topology filter with different intersects types")
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
//                                        "EPSG Field II",
//                                        "EPSG code for geometry II"),
//                                PropertyScope.NONE)
//                        .build()
//                )
                .requiredSingleValueSelection(
                        Labels.from(
                                TYPE,
                                "Topology Relation Chooser",
                                "Choose a topology Operator. Geometry I will be tested with Geometry II"),
                        Options.from(
                                IntersectsTypes.normal.name(),
                                IntersectsTypes.bothInterior.name(),
                                IntersectsTypes.bothBoundary.name(),
                                IntersectsTypes.interiorVsBoundary.name(),
                                IntersectsTypes.interiorVsBoundary.name()
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
    public ConfiguredEventProcessor<IntersectsTopoParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor){


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
        if (read_type.equals(IntersectsTypes.bothInterior.name())){
            type = IntersectsTypes.bothInterior.getNumber();
        } else if (read_type.equals(IntersectsTypes.bothBoundary.name())){
            type = IntersectsTypes.bothBoundary.getNumber();
        } else if (read_type.equals(IntersectsTypes.interiorVsBoundary.name())) {
            type = IntersectsTypes.interiorVsBoundary.getNumber();
        } else if (read_type.equals(IntersectsTypes.boundaryVsInterior.name())) {
            type = IntersectsTypes.boundaryVsInterior.getNumber();
        }

        IntersectsTopoParameter params = new IntersectsTopoParameter(graph, geom1, epsg_geom1, geom2, epsg_geom2, type, vsChecker);

        return new ConfiguredEventProcessor<>(params, IntersectsTopo::new);
    }
}
