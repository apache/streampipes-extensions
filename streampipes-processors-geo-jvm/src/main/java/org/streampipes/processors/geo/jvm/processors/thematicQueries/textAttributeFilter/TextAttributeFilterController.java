package org.streampipes.processors.geo.jvm.processors.thematicQueries.textAttributeFilter;

import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.EpRequirements;
import org.streampipes.sdk.helpers.Labels;
import org.streampipes.sdk.helpers.Options;
import org.streampipes.sdk.helpers.OutputStrategies;
import org.streampipes.sdk.helpers.SupportedFormats;
import org.streampipes.sdk.helpers.SupportedProtocols;
import org.streampipes.sdk.utils.Assets;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

public class TextAttributeFilterController extends StandaloneEventProcessingDeclarer<TextAttributeFilterParameter> {

  private static final String KEYWORD_ID = "keyword";
  private static final String OPERATION_ID = "operation";
  private static final String ATTRIBUTE_COLUMN = "attribute column";
  private static final String CASE_SENSITIV = "case sensitiv";
  protected static final String EPA_NAME = "Text Attribute Filter";

  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder
            .create("org.streampipes.processors.geo.jvm.processors.thematicQueries.textAttributeFilter",
                    EPA_NAME,
                    "Filters single text attribute parameters")
            .category(DataProcessorType.FILTER)
            .withAssets(Assets.DOCUMENTATION, Assets.ICON)
            .requiredStream(StreamRequirementsBuilder
                    .create()
                    .requiredPropertyWithUnaryMapping(EpRequirements
                    .stringReq(), Labels.from(ATTRIBUTE_COLUMN,
                            "Attribute Column",
                            "Select the desired attribute column"),
                            PropertyScope.NONE)
                    .build())

            .requiredSingleValueSelection(
                    Labels.from(
                            OPERATION_ID,
                            "Select Operation",
                            "is means equals. Like means contains"),
                    Options.from(SearchOption.IS.name(),
                            SearchOption.IS_NOT.name(),
                            SearchOption.LIKE.name(),
                            SearchOption.NOT_LIKE.name()))

            .requiredTextParameter(Labels.from(KEYWORD_ID, "keyword", "Select keyword you are searching for" +
                    "Special words are null for no entry"))
            .requiredSingleValueSelection(
                    Labels.from(
                            CASE_SENSITIV,
                            "case sensitive",
                            "should search be case sensitive?" ),
                    Options.from(
                            "True",
                            "False"
                    )
            )

            .outputStrategy(OutputStrategies.keep())
            .supportedFormats(SupportedFormats.jsonFormat())
            .supportedProtocols(SupportedProtocols.kafka(), SupportedProtocols.jms())
            .build();
  }

  @Override
  public ConfiguredEventProcessor<TextAttributeFilterParameter> onInvocation
          (DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

    String keyword = extractor.singleValueParameter(KEYWORD_ID, String.class);
    String operation = extractor.selectedSingleValue(OPERATION_ID, String.class);
    String filterProperty = extractor.mappingPropertyValue(ATTRIBUTE_COLUMN);
    Boolean caseSensitiv = extractor.selectedSingleValue(CASE_SENSITIV, Boolean.class);


    TextAttributeFilterParameter params = new TextAttributeFilterParameter(graph, keyword, operation, filterProperty, caseSensitiv);



    return new ConfiguredEventProcessor<>(params, TextAttributeFilter::new);
  }
}
