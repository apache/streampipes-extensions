package org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.bufferGeometry;

import org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.BufferSide;
import org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.CapStyle;
import org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.JoinStyle;


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

public class BufferGeometryController extends StandaloneEventProcessingDeclarer<BufferGeometryParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String EPSG_CODE_Buffer = "epsg_code_buffer";


    public final static String CAP= "cap_style";
    public final static String JOIN = "join_style";
    public final static String MITRE_LIMIT = "mitre_limit";
    public final static String QUADRANT_SEGMENTS = "quadrant_segments";
    public final static String SIMPLIFY_FACTOR = "simplify_factor";
    public final static String DISTANCE = "distance";
    public final static String SIDE = "side";
    public final static String OUTPUT_WKT = "buffer_wkt";


    public final static String EPA_NAME = "Buffer Geometry";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.bufferGeometry",
                        EPA_NAME,
                        "Creates buffer polygon geometry around the input geometry within a manual distance")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.stringReq(),
                                Labels.from(
                                        WKT_TEXT,
                                        "WKT_String",
                                        "Contains wkt String"),
                                PropertyScope.NONE
                        )
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.numberReq(),
                                Labels.from(
                                        EPSG_CODE,
                                        "EPSG Code",
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
                                CapStyle.Flat.name(),
                                CapStyle.Round.name())
                )

                .requiredSingleValueSelection(
                        Labels.from(
                                JOIN,
                                "join style",
                                "defines the join style"),
                        Options.from(
                                JoinStyle.Bevel.name(),
                                JoinStyle.Mitre.name(),
                                JoinStyle.Round.name())
                )

                .requiredIntegerParameter(
                        Labels.from(
                                MITRE_LIMIT,
                                "mitre limit",
                                "set the mitre limit"),
                        5)


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
                                "set the default simplify factor "),
                        0.01f
                )

                .requiredFloatParameter(
                        Labels.from(
                                DISTANCE,
                                "distance in meter for buffer creation",
                                "set the distance")
                )

                .requiredSingleValueSelection(
                        Labels.from(
                                SIDE,
                                "side selection",
                                "Chooses the side of BufferGeometry. Standard is both side" ),
                        Options.from(
                                BufferSide.Both.name(),
                                BufferSide.Left.name(),
                                BufferSide.Right.name())
                )



                .outputStrategy(
                        OutputStrategies.append(
                                EpProperties.stringEp(
                                        Labels.from(
                                                "buffer_wkt",
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
    public ConfiguredEventProcessor<BufferGeometryParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor){


        String wkt_string = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);


        String read_joinStyle = extractor.selectedSingleValue(JOIN, String.class);
        String read_capStyle  = extractor.selectedSingleValue(CAP, String.class);



        //sets round and default for capStyle and joinStyle
        int capStyle = 1;

        // verwendet den final parameter der Klasse BufferParameters und wird zum double
        if (read_capStyle.equals(CapStyle.Square.name())){
            capStyle = CapStyle.Square.getNumber();
        } else if (read_capStyle.equals(CapStyle.Flat.name())){
            capStyle = CapStyle.Flat.getNumber();
        }


        int joinStyle = 1;
        // verwendet den final parameter der Klasse BufferParameters und wird zum double
        if (read_joinStyle.equals(JoinStyle.Bevel.name())){
            joinStyle = JoinStyle.Bevel.getNumber();
        } else if (read_joinStyle.equals(JoinStyle.Mitre.name())){
            joinStyle = JoinStyle.Mitre.getNumber();
        }


        //reads out the side options. will be used for line and poly
        String read_side = extractor.selectedSingleValue(SIDE, String.class);

        int side = 0;
        boolean  singleSided = false;

        if (read_joinStyle.equals(BufferSide.Right.name())){
            side = BufferSide.Right.getNumber();
            singleSided = true;
        } else if (read_joinStyle.equals(BufferSide.Left.name())){
            joinStyle = BufferSide.Left.getNumber();
            singleSided = true;
        }

        Double mitreLimit = extractor.singleValueParameter(MITRE_LIMIT, Double.class);
        Double distance = extractor.singleValueParameter(DISTANCE, Double.class);
        Integer segments = extractor.singleValueParameter(QUADRANT_SEGMENTS, Integer.class);
        Double simplifyFactor = extractor.singleValueParameter(SIMPLIFY_FACTOR, Double.class);

        BufferGeometryParameter params = new BufferGeometryParameter(graph, wkt_string, epsg_code, distance, capStyle, joinStyle, mitreLimit, segments, simplifyFactor, singleSided, side);

        return new ConfiguredEventProcessor<>(params, BufferGeometry::new);
    }
}
