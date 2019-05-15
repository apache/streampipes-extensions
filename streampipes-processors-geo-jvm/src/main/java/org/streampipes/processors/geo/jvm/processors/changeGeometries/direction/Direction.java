package org.streampipes.processors.geo.jvm.processors.changeGeometries.direction;


import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.DirectionSpOperator;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class Direction implements EventProcessor<DirectionParameter> {

    private static Logger LOG;
    private DirectionParameter params;
    private boolean checker;
    private Integer decimalPosition;

    private Geometry first_geom;
    private Geometry second_geom;



    @Override
    public void onInvocation(DirectionParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(DirectionParameter.class);
        this.params = params;
        this.checker = params.getChecker();
        this.decimalPosition = params.getDecimalPosition();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {




        String geom1_wkt = in.getFieldBySelector(params.getGeom_1()).getAsPrimitive().getAsString();
        Integer epsgCode_geom1 = in.getFieldBySelector(params.getEpsg_geom_1()).getAsPrimitive().getAsInt();


        String geom2_wkt = in.getFieldBySelector(params.getGeom_2()).getAsPrimitive().getAsString();
        Integer epsgCode_geom2 = in.getFieldBySelector(params.getEpsg_geom_2()).getAsPrimitive().getAsInt();



        if (checker){
            //true means geom 1 vs geom 2
            first_geom = createSPGeom(geom1_wkt, epsgCode_geom1);
            second_geom= createSPGeom(geom2_wkt, epsgCode_geom2);
        } else {
            // if false means geom2 vs geom 1 and position will be switched
            first_geom = createSPGeom(geom2_wkt, epsgCode_geom2);
            second_geom= createSPGeom(geom1_wkt, epsgCode_geom1);
        }


        DirectionSpOperator direction = new DirectionSpOperator(first_geom, second_geom, decimalPosition);

        in.addField(DirectionController.DIRECTION, direction.getStringDirection());
        in.addField(DirectionController.UNIT, direction.getDirectionUnit());
        in.addField(DirectionController.ORIENTATION, direction.getOrientation());

        out.collect(in);

    }

    @Override
    public void onDetach() {

    }
}
