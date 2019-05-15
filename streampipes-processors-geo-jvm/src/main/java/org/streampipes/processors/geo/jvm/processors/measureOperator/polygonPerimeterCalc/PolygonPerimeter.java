package org.streampipes.processors.geo.jvm.processors.measureOperator.polygonPerimeterCalc;


import org.locationtech.jts.geom.*;
import org.streampipes.processors.geo.jvm.helpers.LengthSpOperator;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;




public class PolygonPerimeter implements EventProcessor<PolygonPerimeterParameter> {

    private static Logger LOG;
    private PolygonPerimeterParameter params;
    private Integer unit;
    private Integer decimalPositions;


    @Override
    public void onInvocation(PolygonPerimeterParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(PolygonPerimeter.class);
        this.params = params;
        this.unit = params.getUnit();
        this.decimalPositions = params.getDecimalPos();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out){

        //LOG.info(in.toString());

        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();
        Geometry geometry = createSPGeom(wkt, epsgCode);


        //class LengthOperator will be created



        if (!(geometry.getGeometryType()== "GeometryCollection")) {
            if (geometry.getDimension() == 2) {

                LengthSpOperator perimeter = new LengthSpOperator(decimalPositions);


                if (geometry instanceof Polygon) {
                    // calculates perimeter of Polygon
                    perimeter.calcPerimeter((Polygon) geometry);
                } else {
                    // calculates perimeter of MultiPolygon
                    perimeter.calcPerimeter((MultiPolygon) geometry);
                }

                if (unit != 1 | perimeter.getLengthValue() != -9999) {
                    // standard unit is already meter
                    perimeter.convertUnit(unit);
                }
                if (perimeter.getLengthValue() == -9999) {
                    LOG.warn("Calculation of perimeter went wrong and is represented with value -9999 in the stream");
                }


                in.addField(PolygonPerimeterController.PERIMETER, perimeter.getLengthAsString());
                in.addField(PolygonPerimeterController.UNIT, perimeter.getLengthUnit());
                out.collect(in);
            } else {

                LOG.warn("Only Polygons and MultiPolygons are supported in the " + PolygonPerimeterController.EPA_NAME + "but input type is " + geometry.getGeometryType());
            }
        } else {
            LOG.warn("GeometryCollection is not supported in the  " + PolygonPerimeterController.EPA_NAME + "operator.");
        }

    }

    @Override
    public void onDetach() {

    }
}
