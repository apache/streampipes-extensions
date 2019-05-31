package org.streampipes.processors.geo.jvm.processors.derivedGeometry.overlayOperator.filter;




import org.locationtech.jts.geom.Geometry;
import org.streampipes.processors.geo.jvm.helpers.overlayGroup.OverlayOperator;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;

public class OverlaySpFilter implements EventProcessor<OverlaySpFilterParameter> {

    private static Logger LOG;
    private OverlaySpFilterParameter params;
    private Integer type;
    private boolean checker;

    Geometry first_geom;
    Geometry second_geom;



    @Override
    public void onInvocation(OverlaySpFilterParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {


        LOG = params.getGraph().getLogger(OverlaySpFilter.class);
        this.params = params;
        this.type = params.getType();
        this.checker = params.getVsChecker();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out) {


        String geom1_wkt = in.getFieldBySelector(params.getGeom_1()).getAsPrimitive().getAsString();
        Integer epsgCode_geom1 = in.getFieldBySelector(params.getEpsg_geom_1()).getAsPrimitive().getAsInt();


        String geom2_wkt = in.getFieldBySelector(params.getGeom_2()).getAsPrimitive().getAsString();
        Integer epsgCode_geom2 = in.getFieldBySelector(params.getEpsg_geom_2()).getAsPrimitive().getAsInt();


        Geometry overlayResult = null;

        if (checker){
            first_geom = createSPGeom(geom1_wkt, epsgCode_geom1);
            second_geom= createSPGeom(geom2_wkt, epsgCode_geom2);
        } else {
            // if false means geom2 vs geom 1 and position will be switched
            first_geom = createSPGeom(geom2_wkt, epsgCode_geom2);
            second_geom= createSPGeom(geom1_wkt, epsgCode_geom1);
        }

        OverlayOperator overlay = new OverlayOperator(first_geom,second_geom);
        boolean satisfier = false;

        if (type==1){
            overlayResult = overlay.geomIntersectionSP();
            satisfier = true;
        } else if (type ==2){
            overlayResult = overlay.geomUnionSP();
            satisfier = true;

        } else if (type ==3){
            overlayResult = overlay.geomUnionLevel();
            satisfier = true;
        } else if (type == 4){
            overlayResult = overlay.geomDifferenceSP();
            satisfier = true;
        } else if (type == 5){
            overlayResult = overlay.geomSymDifferenceSP();
            satisfier = true;
        }


        if (satisfier){
            in.addField(OverlaySpFilterController.OVERLAY, overlayResult.toText());
            in.addField(OverlaySpFilterController.EPSG_CODE_OVERLAY, overlayResult.getSRID());
            out.collect(in);
        }


    }

    @Override
    public void onDetach() {

    }
}
