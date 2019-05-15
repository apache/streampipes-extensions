package org.streampipes.processors.geo.jvm.processors.changeGeometries.refineLine;


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


public class RefineLineController extends StandaloneEventProcessingDeclarer<RefineLineParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String DISTANCE = "distance";

    public final static String EPA_NAME = "Refine Line";

    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.changeGeometries.refineLine",
                        EPA_NAME,
                        "refines a LineString")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
                                Labels.from(WKT_TEXT, "WKT_String", "Field with WKT"), PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.from(EPSG_CODE, "EPSG Field", "EPSG Code for SRID"),
                                PropertyScope.NONE)
                        .build()
                )
                .requiredFloatParameter(
                        Labels.from(
                                DISTANCE,
                                "refine distance",
                                "set the distance in meter in which subpoints of the LineString will be created")
                )

                .outputStrategy(OutputStrategies.keep())
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<RefineLineParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor){

        String wkt_string = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);
        Double distance = extractor.singleValueParameter(DISTANCE, Double.class);


        RefineLineParameter params = new RefineLineParameter(graph, wkt_string, epsg_code, distance);
        return new ConfiguredEventProcessor<>(params, RefineLine::new);
    }
}
