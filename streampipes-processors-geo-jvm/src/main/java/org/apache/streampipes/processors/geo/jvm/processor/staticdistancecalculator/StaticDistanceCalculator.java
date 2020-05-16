/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.streampipes.processors.geo.jvm.processor.staticdistancecalculator;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.processors.geo.jvm.processor.util.SpLengthCalculator;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.runtime.EventProcessor;

public class StaticDistanceCalculator implements EventProcessor<StaticDistanceCalculatorParameters> {

  private String latitudeFieldName;
  private String longitudeFieldName;

  private Double selectedLocationLatitude;
  private Double selectedLocationLongitude;

  private Integer unit;

  SpLengthCalculator staticLength;

  @Override
  public void onInvocation(StaticDistanceCalculatorParameters parameters, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) throws SpRuntimeException {
    this.latitudeFieldName = parameters.getLatitudeFieldName();
    this.longitudeFieldName = parameters.getLongitudeFieldName();

    this.selectedLocationLatitude = parameters.getSelectedLatitude();
    this.selectedLocationLongitude = parameters.getSelectedLongitude();

    this.unit = parameters.getUnit();

    staticLength = new SpLengthCalculator(parameters.getDecimalPosition());
  }

  @Override
  public void onEvent(Event event, SpOutputCollector collector) throws SpRuntimeException {
    Double latitude = event.getFieldBySelector(latitudeFieldName).getAsPrimitive().getAsDouble();
    Double longitude = event.getFieldBySelector(longitudeFieldName).getAsPrimitive().getAsDouble();

    staticLength.calcGeodesicDistance(latitude, longitude, selectedLocationLatitude, selectedLocationLongitude);

    if (unit != 1) {
      staticLength.convertUnit(unit);
    }

    event.addField(StaticDistanceCalculatorController.LENGTH_RUNTIME, staticLength.getLengthAsString());
    event.addField(StaticDistanceCalculatorController.UNIT_RUNTIME, staticLength.getLengthUnit());

    collector.collect(event);
  }

  @Override
  public void onDetach() throws SpRuntimeException {

  }
}
