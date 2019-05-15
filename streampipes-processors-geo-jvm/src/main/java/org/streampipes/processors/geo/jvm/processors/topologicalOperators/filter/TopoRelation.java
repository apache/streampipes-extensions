package org.streampipes.processors.geo.jvm.processors.topologicalOperators.filter;


import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.processors.topologicalOperators.TopoRelationTypes;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;


import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public class TopoRelation implements EventProcessor<TopoRelationParameter> {

    private static Logger LOG;
    private TopoRelationParameter params;
    private String type;
    private boolean checker;
    Geometry first;
    Geometry against;




    @Override
    public void onInvocation(TopoRelationParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {


        LOG = params.getGraph().getLogger(TopoRelation.class);
        this.params = params;
        this.type = params.getType();
        this.checker = params.getVsChecker();


    }

    @Override
    public void onEvent(Event in, SpOutputCollector out){

        //LOG.info(in.toString());

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


        against = unifyEPSG(first, against, true);

        boolean satisfier = false;

        if (!(first.isEmpty()) || !(against.isEmpty())){

            if (type.equals(TopoRelationTypes.Equals.name())) {
                satisfier = first.equals(against);
            } else if (type.equals(TopoRelationTypes.Disjoint.name())) {
                satisfier = first.disjoint(against);
            } else if (type.equals(TopoRelationTypes.Intersects.name())) {
                satisfier = first.intersects(against);
            } else if (type.equals(TopoRelationTypes.Touches.name())) {
                satisfier = first.touches(against);
            } else if (type.equals(TopoRelationTypes.Crosses.name())) {
                satisfier = first.crosses(against);
            } else if (type.equals(TopoRelationTypes.Within.name())) {
                satisfier = first.within(against);
            } else if (type.equals(TopoRelationTypes.Contains.name())) {
                satisfier = first.contains(against);
            } else if (type.equals(TopoRelationTypes.Overlaps.name())) {
                satisfier = first.overlaps(against);
            } else if (type.equals(TopoRelationTypes.CoveredBy.name())) {
                satisfier = first.coveredBy(against);
            } else if (type.equals(TopoRelationTypes.Covers.name())) {
                satisfier = first.covers(against);
            }

            if (satisfier){
                out.collect(in);

            }

        } else {
            LOG.error("Geometries are empty. Check the field selection in the  " + TopoRelationController.EPA_NAME);
        }



    }

    @Override
    public void onDetach() {

    }
}
