package org.streampipes.processors.geo.jvm.processors.thematicQueries.comparisonOperators;

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


public class NumericalFilterController extends StandaloneEventProcessingDeclarer<NumericalFilterParameters> {

  public static final String VALUE = "value";
  public static final String THRESHOLD = "threshold";
  public static final String FILTER_TYPE = "type";
  public static final String EPA_NAME = "Numerical Filter";



  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder
            .create("org.streampipes.processors.geo.jvm.processors.thematicQueries.comparisonOperators",
                    EPA_NAME,
                    "Filters numerical values on a user defined threshold")
            .category(DataProcessorType.FILTER)
            .withAssets(Assets.DOCUMENTATION, Assets.ICON)
            .requiredStream(StreamRequirementsBuilder
                    .create()
                    .requiredPropertyWithUnaryMapping(
                            EpRequirements.numberReq(),
                            Labels.from(
                                    VALUE,
                                    "Value field",
                                    "Specifies the field name where the typeFilter operation should be applied on."),
                            PropertyScope.NONE)
                    .build()
            )

            .requiredSingleValueSelection(
                    Labels.from(
                            FILTER_TYPE,
                            "Filter type",
                            "Specifies the typeFilter operation that should be applied on the field"),
                    Options.from(
                            "<",
                            "<=",
                            ">",
                            ">=",
                            "==",
                            "!=")
            )

            .requiredFloatParameter(
                    Labels.from(
                            THRESHOLD,
                            "Threshold value",
                            "Specifies a threshold value.")
            )

            .outputStrategy(OutputStrategies.keep())

            .supportedProtocols(
                    SupportedProtocols.kafka(),
                    SupportedProtocols.jms())

            .supportedFormats(SupportedFormats.jsonFormat())

            .build();
  }

  @Override
  public ConfiguredEventProcessor<NumericalFilterParameters> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

    Double threshold = extractor.singleValueParameter(VALUE, Double.class);
    String stringOperation = extractor.selectedSingleValue(FILTER_TYPE, String.class);
    String streamValue = extractor.mappingPropertyValue(VALUE);



    String operation = "GT";

    if (stringOperation.equals("<=")) {
      operation = "LT";
    } else if (stringOperation.equals("<")) {
      operation = "LE";
    } else if (stringOperation.equals(">=")) {
      operation = "GE";
    } else if (stringOperation.equals("==")) {
      operation = "EQ";
    } else if (stringOperation.equals("!=")){
      operation = "NE";
    }




    NumericalFilterParameters params = new NumericalFilterParameters(graph, threshold, NumericalOperator.valueOf(operation), streamValue);

    return new ConfiguredEventProcessor<>(params, NumericalFilter::new);
  }
}
