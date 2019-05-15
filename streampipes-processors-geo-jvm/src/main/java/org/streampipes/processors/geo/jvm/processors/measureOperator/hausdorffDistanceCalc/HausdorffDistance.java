package org.streampipes.processors.geo.jvm.processors.measureOperator.hausdorffDistanceCalc;


import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.LengthSpOperator;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;




public class HausdorffDistance implements EventProcessor<HausdorffDistanceParameter> {

    private static Logger LOG;
    private HausdorffDistanceParameter params;
    private Integer unit;
    private Integer decimalPositions;
    private boolean checker;


    private Geometry mainGeom;
    private Geometry secondGeom;



    @Override
    public void onInvocation(HausdorffDistanceParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(HausdorffDistance.class);
        this.params = params;
        this.unit = params.getUnit();
        this.decimalPositions = params.getDecimalPosition();
        this.checker = params.getVsChecker();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out){

        //LOG.info(in.toString());


        String geom1_wkt = in.getFieldBySelector(params.getGeom_1()).getAsPrimitive().getAsString();
        Integer epsgCode_geom1 = in.getFieldBySelector(params.getEpsg_geom_1()).getAsPrimitive().getAsInt();


        String geom2_wkt = in.getFieldBySelector(params.getGeom_2()).getAsPrimitive().getAsString();
        Integer epsgCode_geom2 = in.getFieldBySelector(params.getEpsg_geom_2()).getAsPrimitive().getAsInt();




        if (checker){
            //true means geom 1 vs geom 2
            mainGeom = createSPGeom(geom1_wkt, epsgCode_geom1);
            secondGeom= createSPGeom(geom2_wkt, epsgCode_geom2);
        } else {
            // if false means geom2 vs geom 1 and position will be switched
            mainGeom = createSPGeom(geom2_wkt, epsgCode_geom2);
            secondGeom= createSPGeom(geom1_wkt, epsgCode_geom1);
        }


        secondGeom = unifyEPSG(mainGeom, secondGeom, true);

        LengthSpOperator hausdorff = new LengthSpOperator(decimalPositions);

        hausdorff.calcHausdorffDistance(mainGeom, secondGeom);

        // if it is another geometry, constructor will set automatically value -9999 and unit m
        if (unit != 1 | hausdorff.getLengthValue() != -9999) {
            // standard unit is already meter
            hausdorff.convertUnit(unit);
        }

        // output some warning that something went wrong
        if (hausdorff.getLengthValue() == -9999){
            LOG.warn("Calculation of hausdorff distance went wrong and is represented with value -9999 in the stream");
        }

        in.addField(HausdorffDistanceController.HAUSDORFF, hausdorff.getLengthAsString());
        in.addField(HausdorffDistanceController.UNIT, hausdorff.getLengthUnit());
        out.collect(in);

        //LOG.info(in.toString());

    }

    @Override
    public void onDetach() {

    }
}
