package org.streampipes.processors.geo.jvm.processors.derivedGeometry.envelope;

import org.locationtech.jts.geom.Geometry;
import org.streampipes.logging.LoggerFactory;
import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class Envelope implements EventProcessor<EnvelopeParameter> {

    private static Logger LOG;
    //private static Logger LOG = LoggerFactory.getPeLogger(Envelope.class);
    private EnvelopeParameter params;

//    public Envelope(EnvelopeParameter params) {
//        super(params);
//    }

    @Override
    public void onInvocation(EnvelopeParameter envelope_parameter, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext)  {

        LOG = envelope_parameter.getGraph().getLogger(Envelope.class);
        this.params = envelope_parameter;
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out) {


        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();

        Geometry geometry = createSPGeom(wkt, epsgCode);


        Geometry envelope = createSPGeom(geometry.getEnvelope(), epsgCode);


        in.addField(EnvelopeController.ENVELOPE, envelope.toText());

        out.collect(in);


    }

    @Override
    public void onDetach() {

    }
}
