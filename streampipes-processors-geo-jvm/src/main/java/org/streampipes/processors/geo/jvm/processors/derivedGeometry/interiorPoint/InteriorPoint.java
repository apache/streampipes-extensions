package org.streampipes.processors.geo.jvm.processors.derivedGeometry.interiorPoint;




import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class InteriorPoint implements EventProcessor<InteriorPointParameter> {

    private static Logger LOG;
    private InteriorPointParameter params;


    @Override
    public void onInvocation(InteriorPointParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(InteriorPoint.class);
        this.params = params;
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {

        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();
        Geometry geometry = createSPGeom(wkt, epsgCode);


        if (geometry instanceof LineString){
            Point interior = getInteriorSp((LineString) geometry);
            in.addField(InteriorPointController.INTERIOR_POINT, interior.toText());
            out.collect(in);
        } else {
            LOG.warn("Only LinesStrings are supported in the " + InteriorPointController.EPA_NAME + "but input type is " + geometry.getGeometryType());
        }



    }

    @Override
    public void onDetach() {

    }
}
