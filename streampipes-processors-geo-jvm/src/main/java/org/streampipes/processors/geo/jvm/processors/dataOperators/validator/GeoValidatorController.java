package org.streampipes.processors.geo.jvm.processors.dataOperators.validator;


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

import java.util.List;

public class GeoValidatorController extends StandaloneEventProcessingDeclarer<GeoValidatorParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String OUTPUT_OPTION = "output_option";
    public final static String FILTER_OPTION = "filter_option";


    public  final static String EPA_NAME = "Geo Validator";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.dataOperators.validator",
                        EPA_NAME,
                        "Validate Geometry and checks for isSimple, isEmpty and isValid")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
                                Labels.from(WKT_TEXT, "WKT String", "Field with WKT String"), PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.from(EPSG_CODE, "EPSG Field", "EPSG Code for SRID"),
                                PropertyScope.NONE)
                        .build()
                )
                .requiredSingleValueSelection(
                        Labels.from(
                                OUTPUT_OPTION,
                                "Output Option",
                                "Choose the output stream result, that will be forwarded "),
                        Options.from(
                                ValidaterEnums.ValidationTypes.VALID.name(),
                                ValidaterEnums.ValidationTypes.INVALID.name()))

                .requiredMultiValueSelection(
                        Labels.from(
                                FILTER_OPTION,
                                "Filer Option",
                                "Choose Filter result"),
                        Options.from(
                                ValidaterEnums.FilterType.IsEmpty.name(),
                                ValidaterEnums.FilterType.IsSimple.name(),
                                ValidaterEnums.FilterType.IsValid.name())
                )
                .outputStrategy(OutputStrategies.keep())
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<GeoValidatorParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor){


        String wkt_String = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);


        List<String> filter_type = extractor.selectedMultiValues(FILTER_OPTION, String.class);
        String outputChooser = extractor.selectedSingleValue(OUTPUT_OPTION, String.class);


        GeoValidatorParameter params = new GeoValidatorParameter(graph, wkt_String, epsg_code, outputChooser, filter_type);

        return new ConfiguredEventProcessor<>(params, GeoValidator::new);
    }
}
