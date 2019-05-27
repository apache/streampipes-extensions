package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.within;


import org.streampipes.container.api.ResolvesContainerProvidedOptions;
import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.runtime.RuntimeOptions;
import org.streampipes.model.schema.EventProperty;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.streampipes.sdk.utils.Assets;

public class WithinTopoController extends StandaloneEventProcessingDeclarer<WithinTopoParameter> implements ResolvesContainerProvidedOptions {


    public final static String GEOMETRY_1 = "geometry1";
    public final static String GEOMETRY_2 = "geometry2";


    public final static String EPSG_GEOM_1 = "epsg_geom1";
    public final static String EPSG_GEOM_2 = "epsg_geom2";

    public final static String TYPE = "topoRelationType";
    public final static String WHO_with_WHOM = "who_with_whom";

    public static final String EPA_NAME = "Within Topology Filter";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.within",
                        EPA_NAME,
                        "Topology Filter with different Within types ")
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
                .requiredSingleValueSelectionFromContainer(Labels.from("id", "Example Name", "Example " +
                        "Description"))

                .requiredSingleValueSelection(
                        Labels.from(
                                TYPE,
                                "Topology Relation Chooser",
                                "Choose a topology Operator. Geometry I will be tested with Geometry II"),
                        Options.from(
                                WithinTypes.within.toString(),
                                WithinTypes.withinComplete.name()
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
    public ConfiguredEventProcessor<WithinTopoParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {


        String epsg_geom1 = extractor.mappingPropertyValue(EPSG_GEOM_1);
        String geom1 = extractor.mappingPropertyValue(GEOMETRY_1);


        String epsg_geom2 = extractor.mappingPropertyValue(EPSG_GEOM_2);
        String geom2 = extractor.mappingPropertyValue(GEOMETRY_2);


        String whoVsWhom = extractor.selectedSingleValue(WHO_with_WHOM, String.class);


        boolean vsChecker = false;
        if (whoVsWhom.equals("Geometry 1 vs. Geometry 2")){
            vsChecker = true;
        }

        String read_withinType = extractor.selectedSingleValue(TYPE, String.class);


        String database = extractor.selectedSingleValue("id", String.class);

        int withinType = 1;
        if (read_withinType.equals(WithinTypes.withinComplete.name())){
            withinType = WithinTypes.withinComplete.getNumber();
        }


        WithinTopoParameter params = new WithinTopoParameter(graph, geom1, epsg_geom1, geom2, epsg_geom2, withinType, vsChecker);

        return new ConfiguredEventProcessor<>(params, WithinTopo::new);
    }

    @Override
    public List<RuntimeOptions> resolveOptions(String s, EventProperty eventProperty) {
        List<RuntimeOptions> results = new ArrayList<>();

        // TODO get names from db
        List<String> geofenceNames = Arrays.asList("bundeslaender", "strasse");

        for (String name : geofenceNames) {
            results.add(new RuntimeOptions(name, ""));
        }

        return results;
    }
}
