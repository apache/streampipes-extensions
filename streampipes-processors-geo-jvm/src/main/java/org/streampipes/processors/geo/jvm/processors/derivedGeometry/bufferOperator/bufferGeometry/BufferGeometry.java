package org.streampipes.processors.geo.jvm.processors.derivedGeometry.bufferOperator.bufferGeometry;


import org.locationtech.jts.geom.Geometry;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;


public class BufferGeometry implements EventProcessor<BufferGeometryParameter> {

    private static Logger LOG;
    private BufferGeometryParameter params;
    private Integer capStyle;
    private Integer joinStyle;
    private Double mitreLimit;
    private Integer segments;
    private Double simplifyFactor;
    private Double distance;
    private Integer side;
    private Boolean singleSided;



    @Override
    public void onInvocation(BufferGeometryParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(BufferGeometry.class);
        this.params = params;
        this.capStyle = params.getCapStyle();
        this.joinStyle = params.getJoinStyle();
        this.mitreLimit = params.getMitreLimit();
        this.segments = params.getSegments();
        this.simplifyFactor = params.getSimplifyFactor();
        this.distance = params.getDistance();
        this.side = params.getSide();
        this.singleSided = params.getSingleSided();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out)  {


        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();
        Geometry geometry = createSPGeom(wkt, epsgCode);


        Geometry bufferGeom = createSpBuffer(geometry ,distance, capStyle, joinStyle, mitreLimit, segments, simplifyFactor, singleSided, side  );

        if (!bufferGeom.isEmpty()){
            in.addField(BufferGeometryController.OUTPUT_WKT, bufferGeom.toText());
            in.addField(BufferGeometryController.EPSG_CODE_Buffer, bufferGeom.getSRID());
            out.collect(in);
        } else {
            LOG.warn("An empty polygon geometry is created in " + BufferGeometryController.EPA_NAME + " and is not parsed into the stream");
        }



    }

    @Override
    public void onDetach() {

    }
}

