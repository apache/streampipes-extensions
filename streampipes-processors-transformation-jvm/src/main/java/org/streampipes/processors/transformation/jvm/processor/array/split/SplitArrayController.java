/*
 * Copyright 2018 FZI Forschungszentrum Informatik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.streampipes.processors.transformation.jvm.processor.array.split;

import org.streampipes.container.api.ResolvesContainerProvidedOutputStrategy;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.schema.EventProperty;
import org.streampipes.model.schema.EventPropertyList;
import org.streampipes.model.schema.EventPropertyNested;
import org.streampipes.model.schema.EventSchema;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.processors.transformation.jvm.config.TransformationJvmConfig;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.EpRequirements;
import org.streampipes.sdk.helpers.Labels;
import org.streampipes.sdk.helpers.OutputStrategies;
import org.streampipes.sdk.helpers.SupportedFormats;
import org.streampipes.sdk.helpers.SupportedProtocols;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

import java.util.ArrayList;
import java.util.List;

public class SplitArrayController extends StandaloneEventProcessingDeclarer<SplitArrayParameters> implements ResolvesContainerProvidedOutputStrategy<DataProcessorInvocation> {

    public static final String KEEP_PROPERTIES_ID = "keep";
    public static final String ARRAY_FIELD_ID = "array_field";

    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder.create("org.streampipes.processors" +
                ".transformation.jvm.split-array", "Split Array", "This processor takes " +
                "an array of event properties and creates an event for each of them. Further property of the events can be added to each element")
                .iconUrl(TransformationJvmConfig.iconBaseUrl + "splitarray.png")

                .requiredStream(StreamRequirementsBuilder.create()
                        .requiredPropertyWithNaryMapping(EpRequirements.anyProperty(),
                                Labels.from(KEEP_PROPERTIES_ID, "Keep Proeprties", "The properties that should be added to the events of array"),
                                PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.listRequirement(),
                                Labels.from(ARRAY_FIELD_ID, "Array of Events", "Contains an array with events"),
                                PropertyScope.NONE)
                        .build())

                .outputStrategy(OutputStrategies.customTransformation())
                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }

    @Override
    public ConfiguredEventProcessor<SplitArrayParameters> onInvocation(DataProcessorInvocation graph) {
        ProcessingElementParameterExtractor extractor = getExtractor(graph);

//        List<String> keepProperties = SepaUtils.getMultipleMappingPropertyNames(graph,
//                KEEP_PROPERTIES_ID, true);


        String arrayField = extractor.mappingPropertyValue(ARRAY_FIELD_ID);
        List<String> keepProperties = extractor.mappingPropertyValues(KEEP_PROPERTIES_ID);

        SplitArrayParameters params = new SplitArrayParameters(graph, arrayField, keepProperties);
        return new ConfiguredEventProcessor<>(params, () -> new SplitArray(params));
    }

    @Override
    public EventSchema resolveOutputStrategy(DataProcessorInvocation processingElement) {
        ProcessingElementParameterExtractor extractor = getExtractor(processingElement);
        String arrayField = extractor.mappingPropertyValue(ARRAY_FIELD_ID);
        List<String> keepProperties = extractor.mappingPropertyValues(KEEP_PROPERTIES_ID);

        List<EventProperty> outProperties = new ArrayList<>();
        for(EventProperty prop : processingElement.getInputStreams().get(0).getEventSchema().getEventProperties()) {
            if (prop.getRuntimeName().equals(arrayField)) {
                EventPropertyNested epn = (EventPropertyNested) ((EventPropertyList) prop).getEventProperties().get(0);
                outProperties.addAll(epn.getEventProperties());
            }

            for (String name : keepProperties) {
                if (prop.getRuntimeName() == name) {
                    outProperties.add(prop);
                }
            }
        }



        return new EventSchema(outProperties);
    }
}