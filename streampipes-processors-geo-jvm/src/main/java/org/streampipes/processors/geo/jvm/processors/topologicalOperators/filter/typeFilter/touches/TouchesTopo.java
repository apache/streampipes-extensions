package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.touches;



import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.topologyGroup.TouchTopology;
import org.streampipes.logging.api.Logger;

import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;
import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;



public class TouchesTopo implements EventProcessor<TouchesTopoParameter> {

    private static Logger LOG;
    private TouchesTopoParameter params;
    private Integer type;
    private boolean checker;

    Geometry first;
    Geometry against;




    @Override
    public void onInvocation(TouchesTopoParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(TouchesTopo.class);
        this.params = params;
        this.type = params.getType();
        this.checker = params.getVsChecker();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {

        //creates variables
        String geom1_wkt = in.getFieldBySelector(params.getGeom_1()).getAsPrimitive().getAsString();
        Integer epsgCode_geom1 = in.getFieldBySelector(params.getEpsg_geom_1()).getAsPrimitive().getAsInt();


        String geom2_wkt = in.getFieldBySelector(params.getGeom_2()).getAsPrimitive().getAsString();
        Integer epsgCode_geom2 = in.getFieldBySelector(params.getEpsg_geom_2()).getAsPrimitive().getAsInt();



        if (checker){
            //true means geom 1 vs geom 2
            first = createSPGeom(geom1_wkt, epsgCode_geom1);
            against= createSPGeom(geom2_wkt, epsgCode_geom2);
        } else {
            // if false means geom2 vs geom 1 and position will be switched
            first = createSPGeom(geom2_wkt, epsgCode_geom2);
            against= createSPGeom(geom1_wkt, epsgCode_geom1);
        }

        TouchTopology touches = new TouchTopology(first, against);


        boolean satisfier = false;


        if (type == 1){
            satisfier = touches.touchesSP();
        } else if (type ==2){
            satisfier = touches.touchesInteriorSp();
        } else if (type == 3){
            satisfier = touches.touchesBoundarySp();
        }


        if (satisfier){
            out.collect(in);
        }
    }

    @Override
    public void onDetach() {

    }
}
