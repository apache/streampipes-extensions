package org.streampipes.processors.geo.jvm.processors.dataOperators.projTransformation;

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

public class ProjTransformationController extends StandaloneEventProcessingDeclarer<ProjTransformationParameter> {


    public final static String WKT_STRING = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String Target_EPSG = "target_epsg";

    public final static String EPA_NAME = "Geometry Reprojection";




    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.dataOperators.projTransformation",
                        EPA_NAME,
                        "Reprojection of the geometries to target EPSG code")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.stringReq(),
                                Labels.from(
                                        WKT_STRING,
                                        "WKT_String",
                                        "Field with WKT String"),
                                PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.numberReq(),
                                Labels.from(
                                        EPSG_CODE,
                                        "EPSG Field",
                                        "EPSG Code for SRID"),
                                PropertyScope.NONE)
                        .build()
                )
                .requiredIntegerParameter(
                        Labels.from(
                                Target_EPSG,
                                "Set EPSG Code for target projection",
                                "Define a valid EPSG Code for your target projection. e.g. 32632 for UTM 32 N Germany"),
                        32632)
                .outputStrategy(OutputStrategies.keep())
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<ProjTransformationParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {



        String wkt_String = extractor.mappingPropertyValue(WKT_STRING);
        String source_epsg = extractor.mappingPropertyValue(EPSG_CODE);
        Integer target_epsg = extractor.singleValueParameter(Target_EPSG, Integer.class);



        ProjTransformationParameter params = new ProjTransformationParameter(graph, wkt_String, source_epsg, target_epsg);

        return new ConfiguredEventProcessor<>(params, ProjTransformation::new);
    }
}
