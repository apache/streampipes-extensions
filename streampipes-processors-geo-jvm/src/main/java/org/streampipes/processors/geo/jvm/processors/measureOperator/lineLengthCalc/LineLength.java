package org.streampipes.processors.geo.jvm.processors.measureOperator.lineLengthCalc;


import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.streampipes.processors.geo.jvm.helpers.LengthSpOperator;
import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;

import static org.streampipes.processors.geo.jvm.helpers.GeometryCreation.*;




public class LineLength implements EventProcessor<LineLengthParameter> {

    private static Logger LOG;
    private LineLengthParameter params;
    private Integer unit;
    private Integer decimalPositions;



    @Override
    public void onInvocation(LineLengthParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(LineLength.class);
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



        if (!(geometry.getGeometryType()== "GeometryCollection")) {
            if (geometry.getDimension() == 1) {


                //class LengthOperator will be created
                LengthSpOperator length = new LengthSpOperator(decimalPositions);


                if (geometry instanceof LineString) {
                    // calculates length for LineString
                    length.calcLength((LineString) geometry);
                } else if (geometry instanceof MultiLineString) {
                    // calculates length for MultiLineString
                    length.calcLength((MultiLineString) geometry);
                }

                if (unit != 1 | length.getLengthValue() != -9999) {
                    // standard unit is already meter
                    length.convertUnit(unit);
                }

                // output some warning that something went wrong
                if (length.getLengthValue() == -9999) {
                    LOG.warn("Calculation of length went wrong and is represented with value -9999 in the stream");
                }

                in.addField(LineLengthController.LENGTH, length.getLengthAsString());
                in.addField(LineLengthController.UNIT, length.getLengthUnit());
                out.collect(in);

                //LOG.info(in.toString());
            } else {
                LOG.warn("Only LinesStrings and MultiLineStrings are supported in the " + LineLengthController.EPA_NAME + "but input type is " + geometry.getGeometryType());

            }
        } else {
            LOG.warn("GeometryCollection is not supported in the  " + LineLengthController.EPA_NAME + "operator.");

        }

    }

    @Override
    public void onDetach() {

    }
}
