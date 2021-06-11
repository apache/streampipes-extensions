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

package org.apache.streampipes.processors.filters.jvm.processor.dummy;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.logging.evaluation.EvaluationLogger;
import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.resource.NodeResourceRequirement;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.ResourceRequirementsBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.vocabulary.SO;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.standalone.ProcessorParams;
import org.apache.streampipes.wrapper.standalone.StreamPipesReconfigurableProcessor;

public class DummyController extends StreamPipesReconfigurableProcessor {

  private static double reconfigurableValue;

  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("org.apache.streampipes.processors.filters.jvm.dummy")
            .category(DataProcessorType.FILTER)
            .withAssets(Assets.DOCUMENTATION)
            .withLocales(Locales.EN)
            .requiredStream(StreamRequirementsBuilder.any())
            //.requiredNodeResources(ResourceRequirementsBuilder.any())
            .requiredNodeResources(ResourceRequirementsBuilder.create()
                    .requiredGpu(false)
                    .requiredCores(1)
                    .requiredMemory("128MB")
                    .requiredStorage("50MB")
                    .build())
            .requiredFloatParameter(Labels.withId("static-float"))
            .requiredReconfigurableFloatParameter(Labels.withId("i-am-reconfigurable"))
//            .requiredReconfigurableIntegerParameter(Labels.withId("i-am-also-reconfigurable"))
            .outputStrategy(OutputStrategies.append(EpProperties.numberEp(Labels.empty(), "appended-reconfigurable",
                    SO.Number)))
            .build();
  }

  @Override
  public void onInvocation(ProcessorParams parameters, SpOutputCollector spOutputCollector,
                           EventProcessorRuntimeContext runtimeContext) throws SpRuntimeException {
    // do nothing
    reconfigurableValue = parameters.extractor().singleValueParameter("i-am-reconfigurable", Double.class);
  }

  @Override
  public void onEvent(Event event, SpOutputCollector collector) throws SpRuntimeException {
    // transform event
    event.addField("appended-reconfigurable", reconfigurableValue);
    collector.collect(event);
  }

  @Override
  public void onDetach() throws SpRuntimeException {
    // do nothing
  }

  @Override
  public void onReconfigurationEvent(Event event) throws SpRuntimeException {
    Object[] obs = {System.currentTimeMillis(), String.format("Dummy processor reconfigured with value %s", event.getFieldByRuntimeName("i-am-reconfigurable").getAsPrimitive().getAsDouble())};
    EvaluationLogger.getInstance().addLine(obs);
    reconfigurableValue = event.getFieldByRuntimeName("i-am-reconfigurable").getAsPrimitive().getAsDouble();
    EvaluationLogger.getInstance().writeOut();
  }
}
