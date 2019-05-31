package org.streampipes.processors.geo.jvm.processors.derivedGeometry.centroidPoint;

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

public class CentroidPointController extends StandaloneEventProcessingDeclarer<CentroidPointParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";

    public final static String CENTROID_POINT = "centroid_wkt";
    public final static String EPSG_CODE_CENTROID = "epsg_code_centroid";


    public final static String EPA_NAME = "Centroid Point";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.derivedGeometry.centroidPoint",
                        EPA_NAME,
                        "Creates a centroid point from the a Polygon or MultiPolygon geometry.")
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .category(DataProcessorType.GEO)
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
                                                "centroid_wkt",
                                                "centroid_wkt",
                                                "centroid WKT"),
                                        CENTROID_POINT,
                                        SO.Text),
                                EpProperties.numberEp(
                                        Labels.from(
                                                "EPSG Code Centroid",
                                                "EPSG Code from Centroid",
                                                "EPSG Code for SRID from Centroid"),
                                        EPSG_CODE_CENTROID, SO.Number)
                        )

                )


                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<CentroidPointParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {


        String wkt_string = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);

        CentroidPointParameter params = new CentroidPointParameter(graph, wkt_string, epsg_code);

        return new ConfiguredEventProcessor<>(params, CentroidPoint::new);
    }
}
