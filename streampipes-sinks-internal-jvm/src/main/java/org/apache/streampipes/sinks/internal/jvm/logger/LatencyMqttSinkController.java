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

package org.apache.streampipes.sinks.internal.jvm.logger;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.logging.evaluation.EvaluationLogger;
import org.apache.streampipes.model.DataSinkType;
import org.apache.streampipes.model.graph.DataSinkDescription;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.DataSinkBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.EpRequirements;
import org.apache.streampipes.sdk.helpers.Labels;
import org.apache.streampipes.sdk.helpers.Locales;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.wrapper.context.EventSinkRuntimeContext;
import org.apache.streampipes.wrapper.standalone.SinkParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesDataSink;

public class LatencyMqttSinkController extends StreamPipesDataSink {

    private EvaluationLogger logger;
    private String topic;
    private String timestampField;

    private static final String LOGGER_TOPIC = "logger-topic-name";
    private static final String TIMESTAMP_MAPPING_KEY = "timestamp_mapping";

    @Override
    public DataSinkDescription declareModel() {
        return DataSinkBuilder.create("org.apache.streampipes.sinks.internal.jvm.logger")
                .withLocales(Locales.EN)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .category(DataSinkType.FORWARD)
                .requiredStream(StreamRequirementsBuilder.create().requiredPropertyWithUnaryMapping(
                        EpRequirements.timestampReq(),
                        Labels.withId(TIMESTAMP_MAPPING_KEY),
                        PropertyScope.NONE).build())
                .requiredTextParameter(Labels.withId(LOGGER_TOPIC))
                .build();
    }

    @Override
    public void onInvocation(SinkParams parameters, EventSinkRuntimeContext runtimeContext) throws SpRuntimeException {
        logger = EvaluationLogger.getInstance();
        topic = parameters.extractor().singleValueParameter(LOGGER_TOPIC, String.class);
        timestampField = parameters.extractor().mappingPropertyValue(TIMESTAMP_MAPPING_KEY);
    }

    @Override
    public void onEvent(Event event) throws SpRuntimeException {
        long timestamp = event.getFieldBySelector(timestampField).getAsPrimitive().getAsLong();
        long latency = System.currentTimeMillis() - timestamp;
        Object[] obs = {"event received", "", latency};
        logger.logMQTT(topic, obs);
    }

    @Override
    public void onDetach() throws SpRuntimeException {

    }
}
