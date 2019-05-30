package org.streampipes.processors.geo.jvm.processors.thematicQueries.multiTextAttributeFilter;

import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.processors.geo.jvm.processors.thematicQueries.textAttributeFilter.SearchOption;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.sdk.utils.Assets;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

public class MultiTextAttributeFilterController extends StandaloneEventProcessingDeclarer<MultiTextAttributeFilterParameter> {

  private static final String KEYWORD_1 = "keyword_1";
  private static final String KEYWORD_2 = "keyword_2";
  private static final String OPERATION_1 = "operation_1";
  private static final String OPERATION_2 = "operation_2";
  private static final String ATTRIBUTE_COLUMN = "attribute column";
  private static final String CASE_SENSITIVE = "case sensitive";
  protected static final String EPA_NAME = "Multi Text Attribute Filter";

  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder
            .create("org.streampipes.processors.geo.jvm.processors.thematicQueries.multiTextAttributeFilter",
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
                            OPERATION_1,
                            "Select Operation for First Keyword",
                            "is means equals. Like means contains"),
                    Options.from(SearchOption.IS.name(),
                            SearchOption.IS_NOT.name(),
                            SearchOption.LIKE.name(),
                            SearchOption.NOT_LIKE.name()))

            .requiredTextParameter(Labels.from(KEYWORD_1, "first keyword", "Select the first keyword you are searching for" +
                    "Special words are null for no entry"))
            .requiredTextParameter(Labels.from(KEYWORD_2, "second keyword", "Select the second keyword you are searching for" +
                    "Special words are null for no entry"))

            .requiredSingleValueSelection(
                    Labels.from(
                            OPERATION_2,
                            "Select Operation for Second Keyword",
                            "is means equals. Like means contains"),
                    Options.from(SearchOption.IS.name(),
                            SearchOption.IS_NOT.name(),
                            SearchOption.LIKE.name(),
                            SearchOption.NOT_LIKE.name()))

            .requiredSingleValueSelection(
                    Labels.from(
                            CASE_SENSITIVE,
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
  public ConfiguredEventProcessor<MultiTextAttributeFilterParameter> onInvocation
          (DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

    String keyword_1 = extractor.singleValueParameter(KEYWORD_1, String.class);
    String operation_1 = extractor.selectedSingleValue(OPERATION_1, String.class);

    String keyword_2 = extractor.singleValueParameter(KEYWORD_2, String.class);
    String operation_2 = extractor.selectedSingleValue(OPERATION_2, String.class);


    String filterProperty = extractor.mappingPropertyValue(ATTRIBUTE_COLUMN);
    Boolean caseSensitive = extractor.selectedSingleValue(CASE_SENSITIVE, Boolean.class);


    MultiTextAttributeFilterParameter params = new MultiTextAttributeFilterParameter(graph, keyword_1, operation_1, keyword_2, operation_2, filterProperty, caseSensitive);



    return new ConfiguredEventProcessor<>(params, MultiTextAttributeFilter::new);
  }
}
