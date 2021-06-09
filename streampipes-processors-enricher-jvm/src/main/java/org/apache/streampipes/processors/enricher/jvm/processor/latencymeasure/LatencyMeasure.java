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

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.runtime.EventProcessor;

public class LatencyMeasure implements EventProcessor<LatencyMeasureParameters> {

    private String timestampField;

    @Override
    public void onInvocation(LatencyMeasureParameters parameters, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) throws SpRuntimeException {
        this.timestampField = parameters.getTimestampField();
    }

    @Override
    public void onEvent(Event event, SpOutputCollector collector) throws SpRuntimeException {
        long timestamp = event.getFieldBySelector(timestampField).getAsPrimitive().getAsLong();
        long latency = System.currentTimeMillis() - timestamp;
        event.addField(LatencyMeasureController.EVENT_LATENCY, latency);
        collector.collect(event);
    }

    @Override
    public void onDetach() throws SpRuntimeException {

    }
}
