package org.streampipes.processors.geo.jvm.processors.measureOperator.distanceCalc.geodesic;

import org.locationtech.jts.geom.Geometry;

import org.streampipes.processors.geo.jvm.helpers.LengthSpOperator;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;



import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public class DistanceGeodesic implements EventProcessor<DistanceGeodesicParameter> {

    private static Logger LOG;
    private DistanceGeodesicParameter params;
    private Integer unit;
    private Integer decimalPositions;





    @Override
    public void onInvocation(DistanceGeodesicParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(DistanceGeodesic.class);
        this.params = params;
        this.unit = params.getUnit();
        this.decimalPositions = params.getDecimalPosition();
    }


    @Override
    public void onEvent(Event in, SpOutputCollector out){

//        //LOG.info(in.toString());
//
        String geom1_wkt = in.getFieldBySelector(params.getGeom_1()).getAsPrimitive().getAsString();
        Integer epsgCode_geom1 = in.getFieldBySelector(params.getEpsg_geom_1()).getAsPrimitive().getAsInt();


        String geom2_wkt = in.getFieldBySelector(params.getGeom_2()).getAsPrimitive().getAsString();
        Integer epsgCode_geom2 = in.getFieldBySelector(params.getEpsg_geom_2()).getAsPrimitive().getAsInt();


        Geometry source = createSPGeom(geom1_wkt, epsgCode_geom1);
        Geometry target =  createSPGeom(geom2_wkt, epsgCode_geom2);


        // here is the main algorithm
        LengthSpOperator geodeticLength = new LengthSpOperator(decimalPositions);

        geodeticLength.calGeodeticDistance(source, target);

        in.addField(DistanceGeodesicController.GEODESIC_DISTANCE, geodeticLength.getLengthAsString());
        in.addField(DistanceGeodesicController.UNIT, geodeticLength.getLengthUnit());

        out.collect(in);

    }

    @Override
    public void onDetach() {

    }
}
