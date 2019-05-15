package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter.typeFilter.intersects;



import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.topologyGroup.IntersectTopology;

import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;
import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;







public class IntersectsTopo implements EventProcessor<IntersectsTopoParameter> {

    private static Logger LOG;
    private IntersectsTopoParameter params;
    private Integer type;
    private boolean checker;

    private Geometry first;
    private Geometry against;




    @Override
    public void onInvocation(IntersectsTopoParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(IntersectsTopo.class);
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
            first = createSPGeom(geom1_wkt, epsgCode_geom1);
            against= createSPGeom(geom2_wkt, epsgCode_geom2);
        } else {
            first = createSPGeom(geom2_wkt, epsgCode_geom2);
            against= createSPGeom(geom1_wkt, epsgCode_geom1);
        }


        IntersectTopology intersects = new IntersectTopology(first, against);


        boolean satisfiers = false;

        if (type == 1){
            satisfiers = intersects.intersectSp();
        } else if (type ==2){
            satisfiers = intersects.intersectSpOnInteriors();
        }else if (type ==3){
            satisfiers = intersects.intersectSpOnBoundary();
        } else if (type ==4){
            satisfiers = intersects.intersectSpOnInteriorsVSBoundary();
        } else if (type ==5){
            satisfiers = intersects.intersectSpOnBoundaryVSInterior();
        }


        if (satisfiers){
            out.collect(in);
        }
    }

    @Override
    public void onDetach() {

    }
}
