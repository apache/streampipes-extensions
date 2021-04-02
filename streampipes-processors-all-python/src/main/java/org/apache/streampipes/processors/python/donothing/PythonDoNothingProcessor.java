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
package org.apache.streampipes.processors.python.donothing;

import com.google.gson.JsonObject;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.Locales;
import org.apache.streampipes.sdk.helpers.OutputStrategies;
import org.apache.streampipes.sdk.helpers.SupportedFormats;
import org.apache.streampipes.sdk.helpers.SupportedProtocols;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesExternalDataProcessor;

import java.util.HashMap;
import java.util.Map;

public class PythonDoNothingProcessor extends StreamPipesExternalDataProcessor {

    public static final String PROCESSOR_ID = "org.apache.streampipes.processors.python.donothing";

    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder.create(PROCESSOR_ID)
                .withLocales(Locales.EN)
                .requiredStream(StreamRequirementsBuilder.any())
                // Append greeting to event stream
                .outputStrategy(OutputStrategies.keep())
                // NOTE: currently one Kafka transport protocol is supported
                .supportedProtocols(SupportedProtocols.kafka())
                .supportedFormats(SupportedFormats.jsonFormat())
                .build();
    }

    @Override
    public void onInvocation(ProcessorParams parameters, EventProcessorRuntimeContext runtimeContext) throws SpRuntimeException {
        Map<String, String> staticPropertyMap = new HashMap<>();
        JsonObject minimalInvocationGraph = createMinimalInvocationGraph(staticPropertyMap);
        // send invocation request to python
        invoke(minimalInvocationGraph);
    }

    @Override
    public void onDetach() throws SpRuntimeException {
        detach();
    }
}
