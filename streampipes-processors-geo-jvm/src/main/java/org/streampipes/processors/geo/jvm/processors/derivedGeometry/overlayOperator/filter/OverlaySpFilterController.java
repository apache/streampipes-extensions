package org.streampipes.processors.geo.jvm.processors.derivedGeometry.overlayOperator.filter;



import org.streampipes.processors.geo.jvm.processors.derivedGeometry.overlayOperator.OverlayTypes;
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


public class OverlaySpFilterController extends StandaloneEventProcessingDeclarer<OverlaySpFilterParameter> {


    public final static String GEOMETRY_1 = "geometry1";
    public final static String GEOMETRY_2 = "geometry2";


    public final static String EPSG_GEOM_1 = "epsg_geom1";
    public final static String EPSG_GEOM_2 = "epsg_geom2";

    public final static String TYPE = "topoRelationType";
    public final static String WHO_with_WHOM = "who_with_whom";


    public final static String OVERLAY = "overlay_wkt";

    public final static String EPA_NAME = "Overlay Operator";



    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.derivedGeometry.overlayOperator.filter",
                        EPA_NAME,
                        "Overlay Operator. Spatial boolean logic on geometries")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
                                Labels.from(GEOMETRY_1,
                                        "Geometry I",
                                        "First Geometry"),
                                PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.from(EPSG_GEOM_1,
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
                .requiredSingleValueSelection(
                        Labels.from(
                                TYPE,
                                "Overlay TypeChooser",
                                "Choose an overlay type."),
                        Options.from(
                                OverlayTypes.Intersection.name(),
                                OverlayTypes.Union.name(),
                                OverlayTypes.UnionLevel.name(),
                                OverlayTypes.Difference.name(),
                                OverlayTypes.SymDifference.name()
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

                .outputStrategy(
                        OutputStrategies.append(
                                EpProperties.stringEp(
                                        Labels.from(
                                                "overlay_wkt",
                                                "overlay_wkt",
                                                "overlay wkt"),
                                        OVERLAY,
                                        SO.Text)
                        )
                )
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<OverlaySpFilterParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {



        String epsg_geom1 = extractor.mappingPropertyValue(EPSG_GEOM_1);
        String geom1 = extractor.mappingPropertyValue(GEOMETRY_1);

        String epsg_geom2 = extractor.mappingPropertyValue(EPSG_GEOM_2);
        String geom2 = extractor.mappingPropertyValue(GEOMETRY_2);

        String whoVsWhom = extractor.selectedSingleValue(WHO_with_WHOM, String.class);


        boolean vsChecker = false;
        if (whoVsWhom.equals("Geometry 1 vs. Geometry 2")){
            vsChecker = true;
        }

        String read_overlayType = extractor.selectedSingleValue(TYPE, String.class);


        int overlayType = 1;
        if (read_overlayType.equals(OverlayTypes.Union.name())){
            overlayType = OverlayTypes.Union.getNumber();
        } else if (read_overlayType.equals(OverlayTypes.UnionLevel.name())){
            overlayType = OverlayTypes.UnionLevel.getNumber();
        } else if (read_overlayType.equals(OverlayTypes.Difference.name())){
            overlayType = OverlayTypes.Difference.getNumber();
        } else if (read_overlayType.equals(OverlayTypes.SymDifference.name())){
            overlayType = OverlayTypes.SymDifference.getNumber();
        }


        OverlaySpFilterParameter params = new OverlaySpFilterParameter(graph,geom1, epsg_geom1, geom2, epsg_geom2, overlayType, vsChecker);

        return new ConfiguredEventProcessor<>(params, OverlaySpFilter::new);
    }
}
