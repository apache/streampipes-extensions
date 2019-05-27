package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter;


import org.streampipes.processors.geo.jvm.processors.topologicalOperators.TopoRelationTypes;
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
import org.streampipes.sdk.utils.Assets;

public class TopoRelationController extends StandaloneEventProcessingDeclarer<TopoRelationParameter> {


    public final static String GEOMETRY_1 = "geometry1";
    public final static String GEOMETRY_2 = "geometry2";


    public final static String EPSG_GEOM_1 = "epsg_geom1";
    public final static String EPSG_GEOM_2 = "epsg_geom2";

    public final static String TYPE = "topoRelationType";
    public final static String WHO_with_WHOM = "who_with_whom";

    public final static String EPA_NAME = "Topology Filter";




    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter",
                        EPA_NAME,
                        "Filter on topology relations")
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
//



                .requiredSingleValueSelection(
                        Labels.from(
                                TYPE,
                                "Topology Relation Chooser",
                                "Choose the topology operator"),
                        Options.from(
                                TopoRelationTypes.Equals.name(),
                                TopoRelationTypes.Contains.name(),
                                TopoRelationTypes.Crosses.name(),
                                TopoRelationTypes.Intersects.name(),
                                TopoRelationTypes.Disjoint.name(),
                                TopoRelationTypes.Within.name(),
                                TopoRelationTypes.Touches.name(),
                                TopoRelationTypes.Overlaps.name(),
                                TopoRelationTypes.CoveredBy.name(),
                                TopoRelationTypes.Covers.name()
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
    public ConfiguredEventProcessor<TopoRelationParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {


        String epsg_geom1 = extractor.mappingPropertyValue(EPSG_GEOM_1);
        String geom1 = extractor.mappingPropertyValue(GEOMETRY_1);


        String epsg_geom2 = extractor.mappingPropertyValue(EPSG_GEOM_2);
        String geom2 = extractor.mappingPropertyValue(GEOMETRY_2);

        String topoRelation = extractor.selectedSingleValue(TYPE, String.class);

        String whoVsWhom = extractor.selectedSingleValue(WHO_with_WHOM, String.class);


        boolean vsChecker = false;
        if (whoVsWhom.equals("Geometry 1 vs. Geometry 2")){
            vsChecker = true;
        }


        TopoRelationParameter params = new TopoRelationParameter(graph, geom1, epsg_geom1, geom2, epsg_geom2, topoRelation, vsChecker);

        return new ConfiguredEventProcessor<>(params, TopoRelation::new);
    }
}
