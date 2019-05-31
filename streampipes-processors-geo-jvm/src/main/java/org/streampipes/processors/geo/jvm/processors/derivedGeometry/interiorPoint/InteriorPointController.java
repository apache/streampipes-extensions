package org.streampipes.processors.geo.jvm.processors.derivedGeometry.interiorPoint;

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

public class InteriorPointController extends StandaloneEventProcessingDeclarer<InteriorPointParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String INTERIOR_POINT = "interior_wkt";
    public final static String EPSG_CODE_INTERIOR = "epsg_code_interior";

    public final static String EPA_NAME = "Interior Point";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.derivedGeometry.interiorPoint",
                        EPA_NAME,
                        "Creates an InteriorPoint of a LineString")
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
                                                "interior_wkt",
                                                "interior_wkt",
                                                "interior_wkt"),
                                        INTERIOR_POINT,
                                        SO.Text),
                                EpProperties.numberEp(
                                        Labels.from(
                                                "EPSG Code Interior",
                                                "EPSG Code from Interior",
                                                "EPSG Code for SRID from Interior"),
                                        EPSG_CODE_INTERIOR, SO.Number)
                        )
                )


                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<InteriorPointParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {


        String wkt_string = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);

        InteriorPointParameter params = new InteriorPointParameter(graph, wkt_string, epsg_code);

        return new ConfiguredEventProcessor<>(params, InteriorPoint::new);
    }
}
