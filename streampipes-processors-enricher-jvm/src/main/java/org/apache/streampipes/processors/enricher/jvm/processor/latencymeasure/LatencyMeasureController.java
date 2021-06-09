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
package org.apache.streampipes.processors.enricher.jvm.processor.latencymeasure;

import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.apache.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

public class LatencyMeasureController extends StandaloneEventProcessingDeclarer<LatencyMeasureParameters> {

    final static String EVENT_LATENCY = "eventLatency";
    private static final String TIMESTAMP = "timestamp";

    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder.create("org.apache.streampipes.processors.enricher.jvm.latencymeasure")
                .category(DataProcessorType.ENRICH)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .withLocales(Locales.EN)
                .requiredStream(StreamRequirementsBuilder.create().requiredPropertyWithUnaryMapping(
                        EpRequirements.timestampReq(),
                        Labels.withId(TIMESTAMP),
                        PropertyScope.NONE).build())
                .outputStrategy(OutputStrategies.append(EpProperties.doubleEp(
                        Labels.withId(EVENT_LATENCY),
                        EVENT_LATENCY,
                        "https://schema.org/processingTime")))
                .build();
    }

    @Override
    public ConfiguredEventProcessor<LatencyMeasureParameters>
    onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {
        String timestampField = extractor.mappingPropertyValue(TIMESTAMP);
        LatencyMeasureParameters staticParam = new LatencyMeasureParameters(graph, timestampField);

        return new ConfiguredEventProcessor<>(staticParam, LatencyMeasure::new);
    }
}
