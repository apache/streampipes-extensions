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

package org.apache.streampipes.processors.geo.jvm.processor.distancecalculator;

import org.apache.streampipes.logging.api.Logger;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.processors.geo.jvm.processor.util.SpLengthCalculator;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.runtime.EventProcessor;

public class DistanceCalculator implements EventProcessor<DistanceCalculatorParameters> {

  private static Logger LOG;
  private SpLengthCalculator length;

  private String latitute1;
  private String longitude1;
  private String latitude2;
  private String longitude2;
  private Integer unit;



  @Override
  public void onInvocation(DistanceCalculatorParameters params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext
          runtimeContext) {

    LOG = params.getGraph().getLogger(DistanceCalculatorParameters.class);
    this.latitute1 = params.getLat1PropertyName();
    this.longitude1 = params.getLong1PropertyName();
    this.latitude2 = params.getLat2PropertyName();
    this.longitude2 = params.getLong2PropertyName();
    this.unit = params.getUnit();

    // init class with constructor
    length = new SpLengthCalculator(params.getDecimalPosition());

  }

  @Override
  public void onEvent(Event event, SpOutputCollector out) {

    double lat1 = event.getFieldBySelector(latitute1).getAsPrimitive().getAsDouble();
    double lng1 = event.getFieldBySelector(longitude1).getAsPrimitive().getAsDouble();
    double lat2 = event.getFieldBySelector(latitude2).getAsPrimitive().getAsDouble();
    double lng2 = event.getFieldBySelector(longitude2).getAsPrimitive().getAsDouble();

    length.calcGeodesicDistance(lat1, lng1, lat2, lng2);

    if (unit != 1) {
      length.convertUnit(unit);
    }

    event.addField(DistanceCalculatorController.LENGTH_RUNTIME, length.getLengthAsString());
    event.addField(DistanceCalculatorController.UNIT_RUNTIME, length.getLengthUnit());

    out.collect(event);
  }

  @Override
  public void onDetach() {

  }

}
