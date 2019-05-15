package org.streampipes.processors.geo.jvm.processors.changeGeometries.refineLine;




import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.streampipes.logging.api.Logger;

import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;

import static org.streampipes.processors.geo.jvm.helpers.Helper.*;
import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class RefineLine implements EventProcessor<RefineLineParameter> {

    private static Logger LOG;
    private RefineLineParameter params;
    private Double distance;



    @Override
    public void onInvocation(RefineLineParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(RefineLineParameter.class);
        this.params = params;
        this.distance = params.getDistance();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {

        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();
        Geometry geometry = createSPGeom(wkt, epsgCode);

        if (geometry instanceof LineString){
            LineString refined = refineLine((LineString) geometry, distance);


            in.updateFieldBySelector(RefineLineController.WKT_TEXT, refined.toText());
            out.collect(in);

            //LOG.info(in.toString());
        } else {
            LOG.warn("Only LinesStrings are supported in the " + RefineLineController.EPA_NAME + "but input type is " + geometry.getGeometryType());


        }

    }

    @Override
    public void onDetach() {

    }
}
