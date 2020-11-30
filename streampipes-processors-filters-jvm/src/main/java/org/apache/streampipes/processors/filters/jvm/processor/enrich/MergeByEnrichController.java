/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package org.apache.streampipes.processors.filters.jvm.processor.enrich;

import org.apache.streampipes.container.api.ResolvesContainerProvidedOptions;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.model.runtime.RuntimeOptions;
import org.apache.streampipes.model.staticproperty.Option;
import org.apache.streampipes.processors.filters.jvm.config.FiltersJvmConfig;
import org.apache.streampipes.sdk.StaticProperties;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.apache.streampipes.sdk.extractor.StaticPropertyExtractor;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.apache.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class MergeByEnrichController extends StandaloneEventProcessingDeclarer<MergeByEnrichParameters> implements ResolvesContainerProvidedOptions {

  private static final String SELECT_STREAM = "select-stream";
  private static final String GROUP_STREAM = "group-stream";
  private static final String DISABLE_GROUPING = "disable-grouping";

  private static final String ENABLE_GROUPING = "enable-grouping";
  private static final String GROUP_CONFIG = "group-config";
  private static final String GROUP_ID_STREAM_1 = "group_id_stream_1";
  private static final String GROUP_ID_STREAM_2 = "group_id_stream_2";

  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("org.apache.streampipes.processors.filters.jvm.enrich")
            .category(DataProcessorType.TRANSFORM)
            .withAssets(Assets.DOCUMENTATION, Assets.ICON)
            .withLocales(Locales.EN)
            .requiredStream(StreamRequirementsBuilder
                    .create()
                    .requiredProperty(EpRequirements.anyProperty())
                    .build())
            .requiredStream(StreamRequirementsBuilder
                    .create()
                    .requiredProperty(EpRequirements.anyProperty())
                    .build())
            .requiredSingleValueSelection(Labels.withId(SELECT_STREAM),
                    Options.from("Stream 1", "Stream 2"))

            .requiredAlternatives(
                    Labels.withId(GROUP_STREAM),
                    Alternatives.from(Labels.withId(DISABLE_GROUPING)),

                    Alternatives.from(Labels.withId(ENABLE_GROUPING),
                            StaticProperties.group(Labels.withId(GROUP_CONFIG),
                                    StaticProperties.singleValueSelectionFromContainer(Labels.withId(GROUP_ID_STREAM_1)),
                                    StaticProperties.singleValueSelectionFromContainer(Labels.withId(GROUP_ID_STREAM_2)))))

            .outputStrategy(OutputStrategies.custom(true))
            .build();
  }

  @Override
  public ConfiguredEventProcessor<MergeByEnrichParameters>
  onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

    List<String> outputKeySelectors = extractor.outputKeySelectors();

    String selectedStream = extractor.selectedSingleValue(SELECT_STREAM, String.class);

    MergeByEnrichParameters staticParam = new MergeByEnrichParameters(
            graph, outputKeySelectors, selectedStream);

    return new ConfiguredEventProcessor<>(staticParam, MergeByEnrich::new);
  }

  @Override
  public List<Option> resolveOptions(String requestId, StaticPropertyExtractor parameterExtractor) {

    System.out.println(requestId);
      return new ArrayList<>();
  }
}
