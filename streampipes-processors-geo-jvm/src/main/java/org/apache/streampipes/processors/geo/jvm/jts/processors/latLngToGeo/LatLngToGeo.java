package org.apache.streampipes.processors.geo.jvm.jts.processors.latLngToGeo;



import org.apache.streampipes.processors.geo.jvm.jts.helpers.SpGeometryBuilder;
import org.locationtech.jts.geom.Point;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;


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
        Integer epsg_value = in.getFieldBySelector(params.getEpsg_value()).getAsPrimitive().getAsInt();
        Point geom =  SpGeometryBuilder.createSPGeom(lng, lat, epsg_value);

        //LOG.info(geometry.toString());

        if (!geom.isEmpty()){
            in.addField(LatLngToGeoController.WKT, geom.toString());
            out.collect(in);
        } else {
            LOG.warn("An empty point geometry in " + LatLngToGeoController.EPA_NAME + " is created due" +
                "invalid input field. Latitude: " + lat + "Longitude: " + lng);
        }
    }

    @Override
    public void onDetach() {

    }
}
