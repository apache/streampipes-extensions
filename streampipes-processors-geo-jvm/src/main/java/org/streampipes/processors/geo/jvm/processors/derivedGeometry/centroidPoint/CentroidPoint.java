package org.streampipes.processors.geo.jvm.processors.derivedGeometry.centroidPoint;




import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;
import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public class CentroidPoint implements EventProcessor<CentroidPointParameter> {

    private static Logger LOG;
    private CentroidPointParameter params;
    Point centroidPoint;




    @Override
    public void onInvocation(CentroidPointParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(CentroidPoint.class);
        this.params = params;


    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {



        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();
        Geometry geometry = createSPGeom(wkt, epsgCode);




        if (geometry instanceof Polygon){
            centroidPoint = (Point) createSPGeom(geometry.getCentroid(), geometry.getSRID());
            in.addField(CentroidPointController.CENTROID_POINT, centroidPoint.toText());

            out.collect(in);

        } else if (geometry instanceof MultiPolygon){
            centroidPoint = (Point) createSPGeom(geometry.getCentroid(), geometry.getSRID());
            in.addField(CentroidPointController.CENTROID_POINT, centroidPoint.toText());

            out.collect(in);
        } else {
            LOG.warn("Only Polygons and MultiPolygons are supported in the " + CentroidPointController.EPA_NAME + "but input type is " + geometry.getGeometryType());

        }



    }

    @Override
    public void onDetach() {

    }
}
