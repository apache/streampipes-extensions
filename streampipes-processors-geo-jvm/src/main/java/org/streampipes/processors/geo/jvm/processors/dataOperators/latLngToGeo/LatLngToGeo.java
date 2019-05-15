package org.streampipes.processors.geo.jvm.processors.dataOperators.latLngToGeo;


import org.locationtech.jts.geom.Geometry;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;


import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public class LatLngToGeo implements EventProcessor<LatLngToGeoParameter> {


    private static Logger LOG;
    private LatLngToGeoParameter params;




    @Override
    public void onInvocation(LatLngToGeoParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(LatLngToGeoParameter.class);
        this.params = params;

    }

    @Override
    public void onEvent(Event in, SpOutputCollector out) {


        Double lat = in.getFieldBySelector(params.getLat()).getAsPrimitive().getAsDouble();
        Double lng = in.getFieldBySelector(params.getLng()).getAsPrimitive().getAsDouble();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();
        Geometry geometry =  createSPGeom(lat, lng, epsgCode);

        if (!geometry.isEmpty()){
            in.addField(LatLngToGeoController.WKT_TEXT, geometry.toString());
            out.collect(in);
        } else {
            LOG.warn("An empty point geometry is created in " + LatLngToGeoController.EPA_NAME + " " +
                    "due invalid input values. check: Latitude:" + lat + "Longitude: " + lng + "or epsg Code:" + epsgCode);


        }




    }

    @Override
    public void onDetach() {

    }
}
