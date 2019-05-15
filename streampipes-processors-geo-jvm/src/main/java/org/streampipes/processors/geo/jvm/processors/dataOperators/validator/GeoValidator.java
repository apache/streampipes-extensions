package org.streampipes.processors.geo.jvm.processors.dataOperators.validator;



import org.locationtech.jts.geom.Geometry;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;


import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;





public class GeoValidator implements EventProcessor<GeoValidatorParameter> {


    private static Logger LOG;
    private GeoValidatorParameter params;




    @Override
    public void onInvocation(GeoValidatorParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(GeoValidator.class);
        this.params = params;
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {

        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();
        Geometry geometry = createSPGeom(wkt, epsgCode);


        Boolean satisfiers = false;

        // todo get from list all chosen options
        if (params.getOutput_choosing().equals( ValidaterEnums.ValidationTypes.VALID.name())){

           satisfiers = (geometry.isValid() && geometry.isSimple() && !(geometry.isEmpty()));

        } else if (params.getOutput_choosing().equals( ValidaterEnums.ValidationTypes.INVALID.name())){
            satisfiers = (!(geometry.isValid()) || !(geometry.isSimple()) || geometry.isEmpty());
        }



        if (satisfiers){
            out.collect(in);
        }


    }

    @Override
    public void onDetach() {

    }
}