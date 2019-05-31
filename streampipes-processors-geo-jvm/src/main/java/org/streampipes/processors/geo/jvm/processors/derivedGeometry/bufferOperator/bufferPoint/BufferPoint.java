package org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.bufferPoint;


import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.Point;
import org.streampipes.logging.api.Logger;
import org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.bufferGeometry.BufferGeometryController;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;


import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;



public class BufferPoint implements EventProcessor<BufferPointParameter> {

    private static Logger LOG;
    private BufferPointParameter params;
    private Integer capStyle;
    private Integer segments;
    private Double simplifyFactor;
    private Double distance;



    @Override
    public void onInvocation(BufferPointParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(BufferPoint.class);
        this.params = params;

        this.capStyle = params.getCapStyle();
        this.segments = params.getSegments();
        this.simplifyFactor = params.getSimplifyFactor();
        this.distance = params.getDistance();

    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {


        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();
        Geometry geometry = createSPGeom(wkt, epsgCode);



        if (geometry instanceof Point){

            // main algorithm
            Geometry buffer = createSpBuffer((Point) geometry, distance, capStyle, segments, simplifyFactor);

            if (!buffer.isEmpty()){
                in.addField(BufferPointController.OUTPUT_WKT, buffer.toText());
                in.addField(BufferGeometryController.EPSG_CODE_Buffer, buffer.getSRID());
                out.collect(in);
            } else {
                LOG.warn("An empty polygon geometry is created in " + BufferGeometryController.EPA_NAME + " and is not parsed into the stream");
            }

            //LOG.info(in.toString());
        } else {
            LOG.warn("Only points are supported in the " + BufferPointController.EPA_NAME + "but input type is " + geometry.getGeometryType());



        }


    }

    @Override
    public void onDetach() {

    }
}

