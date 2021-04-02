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
package org.apache.streampipes.processors.python.greeter;

import com.google.gson.JsonObject;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.vocabulary.SO;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesExternalDataProcessor;

import java.util.HashMap;
import java.util.Map;

public class PythonGreeterProcessor extends StreamPipesExternalDataProcessor {

    private static final String GREETING_KEY = "greeting-key";
    public static final String PROCESSOR_ID = "org.apache.streampipes.processors.python.greeter";

    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder.create(PROCESSOR_ID)
                .withLocales(Locales.EN)
                .requiredStream(StreamRequirementsBuilder.any())
                // create a simple text parameter
                .requiredTextParameter(Labels.withId(GREETING_KEY), "greeting")
                // append greeting to event stream
                .outputStrategy(OutputStrategies.append(
                        EpProperties.stringEp(Labels.empty(),"greeting", SO.Text)))
                // NOTE: currently one Kafka transport protocol is supported
                .supportedProtocols(SupportedProtocols.kafka())
                .supportedFormats(SupportedFormats.jsonFormat())
                .build();
    }

    @Override
    public void onInvocation(ProcessorParams parameters, EventProcessorRuntimeContext runtimeContext) throws SpRuntimeException {

        // extract static properties and add to map to build minimal invocation graph
        Map<String, String> staticPropertyMap = new HashMap<>();
        staticPropertyMap.put("greeting", parameters.extractor().singleValueParameter(GREETING_KEY, String.class));

        JsonObject minimalInvocationGraph = createMinimalInvocationGraph(staticPropertyMap);

        // send invocation request to python
        invoke(minimalInvocationGraph);
    }

    @Override
    public void onDetach() throws SpRuntimeException {
        // send detach request to python to stop processor with invocationId
        detach();
    }
}
