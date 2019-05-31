package org.streampipes.processors.geo.jvm.processors.derivedGeometry.convexHull;



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
import org.streampipes.sdk.utils.Assets;

public class ConvexHullController extends StandaloneEventProcessingDeclarer<ConvexHullParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String CONVEX_HULL_WKT = "convex-hull_wkt";
    public final static String EPSG_CODE_CONVEXHULL = "epsg_code_convex_hull";

    public final static String EPA_NAME = "Convex Hull";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.derivedGeometry.convexHull",
                        EPA_NAME,
                        "Creates a ConvexHull around the geometry")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
                                Labels.from(WKT_TEXT, "WKT_String", "Field with WKT String"), PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.from(EPSG_CODE, "EPSG Field", "EPSG Code for SRID"),
                                PropertyScope.NONE)
                        .build()
                )
                .outputStrategy(
                        OutputStrategies.append(
                                EpProperties.stringEp(
                                        Labels.from(
                                                "convex-hull_wkt",
                                                "convex-hull_wkt",
                                                "convex hull wkt"),
                                        CONVEX_HULL_WKT,
                                        SO.Text),
                                EpProperties.numberEp(
                                        Labels.from(
                                                "EPSG Code ConvexHull",
                                                "EPSG Code from ConvexHull",
                                                "EPSG Code for SRID from ConvexHull"),
                                        EPSG_CODE_CONVEXHULL, SO.Number)
                        )
                )
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<ConvexHullParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

        String wkt_string = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);

        ConvexHullParameter params = new ConvexHullParameter(graph, wkt_string, epsg_code);

        return new ConfiguredEventProcessor<>(params, ConvexHull::new);
    }
}
