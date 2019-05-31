package org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.bufferPoint;



import org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.CapStyle;

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

public class BufferPointController extends StandaloneEventProcessingDeclarer<BufferPointParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String OUTPUT_WKT = "buffer_wkt";
    public final static String EPSG_CODE_Buffer = "epsg_code_buffer";



    public final static String CAP= "cap_style";
    public final static String QUADRANT_SEGMENTS = "quadrant_segments";
    public final static String SIMPLIFY_FACTOR = "simplify_factor";
    public final static String DISTANCE = "distance";


    public final static String EPA_NAME = "Buffer from Point";



    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.bufferPoint",
                        EPA_NAME,
                        "Creates a buffer polygon around a point geometry")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.stringReq(),
                                Labels.from(
                                        WKT_TEXT,
                                        "WKT_String",
                                        "Field with WKT String"),
                                PropertyScope.NONE
                        )
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.numberReq(),
                                Labels.from(
                                        EPSG_CODE,
                                        "EPSG Field",
                                        "EPSG Code for SRID"),
                                PropertyScope.NONE
                        )
                        .build()
                )
                //sets capStyle Parameter
                .requiredSingleValueSelection(
                        Labels.from(
                                CAP,
                                "cap style",
                                "defines the cap style"),
                        Options.from(
                                CapStyle.Square.name(),
                                CapStyle.Round.name())
                )

                .requiredIntegerParameter(
                        Labels.from(
                                QUADRANT_SEGMENTS,
                                "quadrant_segments",
                                "set the quadrant segment"),
                        8
                )

                .requiredFloatParameter(
                        Labels.from(
                                SIMPLIFY_FACTOR,
                                "simplify factor",
                                "set the default simplify Factor"),
                        0.01f
                )

                .requiredFloatParameter(
                        Labels.from(
                                DISTANCE,
                                "BufferPoint Distance",
                                "distance in meter for buffer creation")
                )

                .outputStrategy(
                        OutputStrategies.append(
                                EpProperties.stringEp(
                                        Labels.from(
                                                "buffer_text",
                                                "buffer_wkt",
                                                "buffer_wkt"),
                                        OUTPUT_WKT,
                                        SO.Text),
                                EpProperties.numberEp(
                                        Labels.from(
                                                "EPSG Code Buffer",
                                                "EPSG Code from Buffer Polygon",
                                                "EPSG Code for SRID from Buffer"),
                                        EPSG_CODE_Buffer, SO.Number)
                        )
                )

                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<BufferPointParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {


        String wkt_string   = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code    = extractor.mappingPropertyValue(EPSG_CODE);
        String read_capStyle  = extractor.selectedSingleValue(CAP, String.class);


        //round will be assumed to be default
        int capStyle = 1;
        if (read_capStyle.equals(CapStyle.Square.name())) {
            capStyle = CapStyle.Square.getNumber();
        }


        Integer segments = extractor.singleValueParameter(QUADRANT_SEGMENTS, Integer.class);
        Double simplifyFactor = extractor.singleValueParameter(SIMPLIFY_FACTOR, Double.class);
        Double distance = extractor.singleValueParameter(DISTANCE, Double.class);



        BufferPointParameter params = new BufferPointParameter(graph, wkt_string, epsg_code, distance, capStyle, segments, simplifyFactor);

        return new ConfiguredEventProcessor<>(params, BufferPoint::new);
    }
}
