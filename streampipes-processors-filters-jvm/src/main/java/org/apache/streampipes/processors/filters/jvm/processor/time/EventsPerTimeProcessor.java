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

package org.apache.streampipes.processors.filters.jvm.processor.time;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesDataProcessor;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class EventsPerTimeProcessor extends StreamPipesDataProcessor {

  private static final String TIMESTAMP_MAPPING = "timestamp-mapping";
  private static final String NUMBER_OF_EVENTS = "number-of-events";
  private static final String TIME_WINDOW = "time-window";
  private static final String TIME_OPTION = "time-option";
  private static final String FREEZE_TIME = "freeze-time";
  private static final String FIRE_OPTION = "fire-option";

  public static final String HOUR = "Hour";
  public static final String MINUTE = "Minute";
  public static final String SECOND = "Second";
  public static final String ONCE = "Once";
  public static final String EACH = "Each event";

  private String timestampKey;
  private int numberOfEvents;
  private int timeWindow;
  private String timeOption;
  private int freezeTime;
  private String fireOption;

  private Long lastFire;

  List<Long> timestamps;

  public EventsPerTimeProcessor() {
   super();
  }

  public EventsPerTimeProcessor(List<Long> timestamps,
                                int numberOfEvents,
                                int timeWindow,
                                String timeOption,
                                int freezeTime,
                                String fireOption) {
    this.timestamps = timestamps;
    this.numberOfEvents = numberOfEvents;
    this.timeWindow = timeWindow;
    this.timeOption = timeOption;
    this.freezeTime = freezeTime;
    this.fireOption = fireOption;
  }

  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("org.apache.streampipes.processors.filters.jvm.processor.time")
            .category(DataProcessorType.FILTER)
            .withAssets(Assets.DOCUMENTATION, Assets.ICON)
            .withLocales(Locales.EN)
            .requiredStream(StreamRequirementsBuilder.create()
                    .requiredPropertyWithUnaryMapping(
                            EpRequirements.timestampReq(),
                            Labels.withId(TIMESTAMP_MAPPING),
                            PropertyScope.HEADER_PROPERTY).build())

            .requiredIntegerParameter(Labels.withId(NUMBER_OF_EVENTS))

            .requiredSingleValueSelection(Labels.withId(TIME_OPTION), Options.from(HOUR, MINUTE, SECOND))
            .requiredIntegerParameter(Labels.withId(TIME_WINDOW))
            .requiredIntegerParameter(Labels.withId(FREEZE_TIME))
            .requiredSingleValueSelection(Labels.withId(FIRE_OPTION), Options.from(ONCE, EACH))

            .outputStrategy(OutputStrategies.keep())

            .build();

  }


  @Override
  public void onInvocation(ProcessorParams processorParams, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext eventProcessorRuntimeContext) throws SpRuntimeException {
    this.timestampKey = processorParams.extractor().mappingPropertyValue(TIMESTAMP_MAPPING);
    this.numberOfEvents = processorParams.extractor().singleValueParameter(NUMBER_OF_EVENTS, Integer.class);
    this.timeWindow = processorParams.extractor().singleValueParameter(TIME_WINDOW, Integer.class);
    this.timeOption = processorParams.extractor().selectedSingleValue(TIME_OPTION, String.class);
    this.freezeTime = processorParams.extractor().singleValueParameter(FREEZE_TIME, Integer.class);
    this.fireOption = processorParams.extractor().selectedSingleValue(FIRE_OPTION, String.class);

    this.timestamps = new ArrayList<>();
  }

  @Override
  public void onEvent(Event event, SpOutputCollector spOutputCollector) throws SpRuntimeException {
    long timestamp = event.getFieldBySelector(this.timestampKey).getAsPrimitive().getAsLong();

    this.timestamps.add(timestamp);

    if (this.applyRule()) {
      spOutputCollector.collect(event);
    }
  }

  @Override
  public void onDetach() throws SpRuntimeException {

  }

  public boolean applyRule() {
    boolean result;

    // remove old events (TODO change unit)
    long leftWindowTimestamp = this.timestamps.get(this.timestamps.size() - 1) - this.timeWindow * getMilliseconds();
    timestamps = timestamps
            .stream()
            .filter(t -> t > leftWindowTimestamp)
            .collect(Collectors.toList());

    // validate if enough events  are in time window
    result = timestamps.size() > this.numberOfEvents;

//    if  (result  && )

    // check if it should  only fire once
    if (ONCE.equals(fireOption)) {

    }



    return result;
  }

  public List<Long> getTimestamps() {
    return timestamps;
  }

  private int getMilliseconds() {
    switch (this.timeOption) {
      case SECOND:
        return 1000;
      case MINUTE:
        return 60000;
      case HOUR:
        return 86400;
      default:
        return 1000;
    }
  }
}
