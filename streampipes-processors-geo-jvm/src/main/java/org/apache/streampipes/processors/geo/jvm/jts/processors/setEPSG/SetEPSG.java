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

package org.apache.streampipes.processors.geo.jvm.jts.processors.setEPSG;

import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;


public class SetEPSG implements EventProcessor<SetEpsgParameter> {

    public static Logger LOG;
    public SetEpsgParameter params;
    public Integer epsg_value;



    @Override
    public void onInvocation(SetEpsgParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(SetEPSG.class);
        this.params = params;
        this.epsg_value = params.getEpsg_value();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {
        in.addField(SetEpsgController.EPSG, epsg_value);
        out.collect(in);
        System.out.println(in.getRaw());
    }

    @Override
    public void onDetach() {

    }
}
