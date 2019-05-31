package org.streampipes.processors.geo.jvm.processors.derivedGeometry.convexHull;


import org.locationtech.jts.geom.Geometry;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class ConvexHull implements EventProcessor<ConvexHullParameter> {

    private static Logger LOG;
    private ConvexHullParameter params;


    @Override
    public void onInvocation(ConvexHullParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {


        LOG = params.getGraph().getLogger(ConvexHull.class);
        this.params = params;
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {

        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();
        Geometry geometry = createSPGeom(wkt, epsgCode);


        Geometry convexHull = createSPGeom(geometry.convexHull(), epsgCode);

        in.addField(ConvexHullController.CONVEX_HULL_WKT, convexHull.toText());
        in.addField(ConvexHullController.EPSG_CODE_CONVEXHULL, convexHull.getSRID());


        out.collect(in);


    }

    @Override
    public void onDetach() {

    }
}
