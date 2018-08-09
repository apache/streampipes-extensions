/*
 * Copyright 2017 FZI Forschungszentrum Informatik
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
 */

package org.streampipes.sinks.internal.jvm.dashboard;

import org.streampipes.model.graph.DataSinkInvocation;
import org.streampipes.model.schema.EventSchema;
import org.streampipes.sinks.internal.jvm.config.SinksInternalJvmConfig;
import org.streampipes.wrapper.params.binding.EventSinkBindingParams;

public class DashboardParameters extends EventSinkBindingParams {
    private String pipelineId;
    private EventSchema schema;
    private String broker;

    public DashboardParameters(DataSinkInvocation invocationGraph) {
        super(invocationGraph);
        this.schema = invocationGraph.getInputStreams().get(0).getEventSchema();
        this.pipelineId = invocationGraph.getCorrespondingPipeline();
        this.broker = "ws://" + SinksInternalJvmConfig.INSTANCE.getNginxHost() +":" +SinksInternalJvmConfig.INSTANCE
                .getNginxPort()
                +"/streampipes/ws";
    }

    private String removeProtocol(String url) {
       return url.replaceFirst("^(tcp://|ws://)","");
    }
    public String getPipelineId() {
        return pipelineId;
    }

    public void setPipelineId(String pipelineId) {
        this.pipelineId = pipelineId;
    }

    public EventSchema getSchema() {
        return schema;
    }

    public void setSchema(EventSchema schema) {
        this.schema = schema;
    }

    public String getBroker() {
        return broker;
    }

    public void setBroker(String broker) {
        this.broker = broker;
    }
}