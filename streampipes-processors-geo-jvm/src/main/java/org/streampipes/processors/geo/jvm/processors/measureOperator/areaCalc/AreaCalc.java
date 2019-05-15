package org.streampipes.processors.geo.jvm.processors.measureOperator.areaCalc;



import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Polygon;
import org.streampipes.processors.geo.jvm.helpers.AreaSpOperator;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;


import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;



public class AreaCalc implements EventProcessor<AreaCalcParameter> {


    public static Logger LOG;
    public AreaCalcParameter params;


    // parameters only needed ones
    public Integer decimalPosition;
    public Integer unit;




    @Override
    public void onInvocation(AreaCalcParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(AreaCalc.class);
        this.params = params;
        this.decimalPosition = params.getDecimalPos();
        this.unit = params.getUnit();
    }

    @Override
    public void onEvent(Event in, SpOutputCollector out) {

    //logger.info(in.toString());


        String wkt = in.getFieldBySelector(params.getWkt_string()).getAsPrimitive().getAsString();
        Integer epsgCode = in.getFieldBySelector(params.getEpsg_code()).getAsPrimitive().getAsInt();

        Geometry geometry =  createSPGeom(wkt, epsgCode);


        AreaSpOperator area = new AreaSpOperator(decimalPosition);



        if (!(geometry.getGeometryType()== "GeometryCollection")) {
            if (geometry.getDimension() == 2) {


                if (geometry instanceof Polygon) {
                    area.calcArea((Polygon) geometry);

                } else {
                    //MultiPolygon
                    area.calcArea((MultiPolygon) geometry);
                }


                //if unit is not meter (user chooses another output) or value in not -9999
                if (unit != 1 | area.getAreaValue() != -9999) {
                    area.convertUnit(unit);
                }

                if (area.getAreaValue() == -9999) {
                    LOG.warn("Calculation of area in the " + AreaCalcController.EPA_NAME + " went wrong (valid: " + geometry.isValid() + ") and is represented with value -9999 in the stream");
                }

                in.addField(AreaCalcController.AREA, area.getAreaAsString());
                in.addField(AreaCalcController.UNIT, area.getAreaUnit());
                out.collect(in);


            } else {
                LOG.warn("Only Polygons or MultiPolygons are supported in the '" + AreaCalcController.EPA_NAME + "' but input type is " + geometry.getGeometryType());
            }
        } else {

            LOG.warn("GeometryCollection is not supported in the  " + AreaCalcController.EPA_NAME + " operator.");

        }

    }

    @Override
    public void onDetach() {

    }


}

