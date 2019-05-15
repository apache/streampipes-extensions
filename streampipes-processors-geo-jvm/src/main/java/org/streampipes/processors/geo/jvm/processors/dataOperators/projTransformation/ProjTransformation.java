package org.streampipes.processors.geo.jvm.processors.dataOperators.projTransformation;



import org.locationtech.jts.geom.Geometry;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;




public class ProjTransformation implements EventProcessor<ProjTransformationParameter> {


    private static Logger LOG;
    private ProjTransformationParameter params;
    private Integer targetEPSG;



    @Override
    public void onInvocation(ProjTransformationParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(ProjTransformation.class);
        this.params = params;
        targetEPSG = params.getTarget_epsg();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {

        //LOG.info(in.toString());

        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsgCode()).getAsPrimitive().getAsInt();
        Geometry geometry = createSPGeom(wkt, epsgCode);


        Geometry transformed = transformSpGeom(geometry, targetEPSG);

        if (!transformed.isEmpty()){
            in.updateFieldBySelector(ProjTransformationController.WKT_STRING, transformed.toText());
            in.updateFieldBySelector(ProjTransformationController.EPSG_CODE, params.getTarget_epsg());
            out.collect(in);
        } else {
            LOG.warn("An empty point geometry is created in " + ProjTransformationController.EPA_NAME + " " +
                    "due invalid input values. Check used epsg Code:" + epsgCode);

        }



    }

    @Override
    public void onDetach() {

    }
}
