package org.streampipes.processors.geo.jvm.processors.dataOperators.setEpsgCode;

import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;


public class SetEPSG implements EventProcessor<SetEpsgParameter> {

    public static Logger LOG;
    public SetEpsgParameter params;
    public Integer epsgCode;



    @Override
    public void onInvocation(SetEpsgParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(SetEPSG.class);
        this.params = params;
        this.epsgCode = params.getEpsg_code();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {

        // adds key and value to each stream event
        in.addField(SetEpsgController.EPSG_CODE, epsgCode);
        out.collect(in);
    }

    @Override
    public void onDetach() {

    }
}
