
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
    }

    @Override
    public void onDetach() {

    }
}
