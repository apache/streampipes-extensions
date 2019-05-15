package org.streampipes.processors.geo.jvm.processors.thematicQueries.arithmeticOperators;



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


public class NumAttributeCalcController extends StandaloneEventProcessingDeclarer<NumAttributeCalcParameter> {


    public final static String FIRST_VALUE = "first";
    public final static String SECOND_VALUE = "second";
    public final static String FIElD_NAME = "field_name";
    public final static String CALC_TYPE = "calculationType";


    public final static String EPA_NAME = "Attribute Calculator";


    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.thematicQueries.arithmeticOperators",
                        EPA_NAME,
                        "Attribute Calculator")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.numberReq(),
                                Labels.from(
                                        FIRST_VALUE,
                                        "First parameter",
                                        "First parameter"),
                                PropertyScope.MEASUREMENT_PROPERTY
                        )
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.numberReq(),
                                Labels.from(
                                        SECOND_VALUE,
                                        "Second parameter",
                                        "Second parameter"),
                                PropertyScope.MEASUREMENT_PROPERTY
                        )
                        .build()
                )

                //todo dynamik field name for output strategie possible?
                .requiredTextParameter(
                        Labels.from(
                                FIElD_NAME,
                                "field name",
                                "Field name for created")
                )

                .requiredSingleValueSelection(Labels.from(CALC_TYPE, "Operator", "Choose the operator"),
                        Options.from(
                                ArithmeticOperator.ADDITION.name(),
                                ArithmeticOperator.SUBTRACTION.name(),
                                ArithmeticOperator.MULTIPLICATION.name(),
                                ArithmeticOperator.DIVISION.name(),
                                ArithmeticOperator.MODULO.name()
                        )
                )

                .outputStrategy(OutputStrategies.append(
                        EpProperties.numberEp(
                                Labels.from(
                                        "CalcResult",
                                        "calc_result",
                                        "wkt geometry string"),
                                // muss hier dynamisch geändert werden?
                                "calc_Result",
                                SO.Number)))


                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())

                .build();
    }


    @Override
    public ConfiguredEventProcessor<NumAttributeCalcParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {


        String value1 = extractor.mappingPropertyValue(FIRST_VALUE);
        String value2 = extractor.mappingPropertyValue(SECOND_VALUE);

        String type = extractor.selectedSingleValue(CALC_TYPE, String.class);
        String dynamic_fieldName = extractor.singleValueParameter(FIElD_NAME, String.class);


        NumAttributeCalcParameter params = new NumAttributeCalcParameter(graph, value1, value2, dynamic_fieldName, type);


        return new ConfiguredEventProcessor<>(params, NumAttributeCalc::new);

    }
}
