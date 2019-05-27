package org.streampipes.processors.geo.jvm.processors.thematicQueries.arithmeticOperators;


import org.streampipes.logging.api.Logger;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;
import org.streampipes.sdk.utils.Assets;


public class NumAttributeCalc implements EventProcessor<NumAttributeCalcParameter> {

    private static Logger LOG;
    private NumAttributeCalcParameter params;
    String calc_operator;
    String runtimeName;






    @Override
    public void onInvocation(NumAttributeCalcParameter params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {

        LOG = params.getGraph().getLogger(NumAttributeCalc.class);
        this.params = params;
        this.calc_operator = params.getCalcType();
        this.runtimeName = params.getField_name();

    }

    @Override
    public void onEvent(Event in, SpOutputCollector out){


        Double value1 =  in.getFieldBySelector(params.getParam1()).getAsPrimitive().getAsDouble();
        Double value2 =  in.getFieldBySelector(params.getParam2()).getAsPrimitive().getAsDouble();


        Double result = null;
        boolean satisfier = false;


        if(calc_operator.equals(ArithmeticOperator.ADDITION.name())){
            result = value1 + value2;
            satisfier = true;
        } else if (calc_operator.equals(ArithmeticOperator.SUBTRACTION.name())){
            result = value1 - value2;
            satisfier  = true;
        } else if (calc_operator.equals(ArithmeticOperator.MULTIPLICATION.name())){
            result = value1 * value2;
            satisfier  = true;
        } else if (calc_operator.equals(ArithmeticOperator.DIVISION.name())) {
            // prevent division 0 exception
            if (value2 == 0) {
                result = null;
                satisfier = true;
            } else {
                result = value1 / value2;
                satisfier = true;
            }
        } else if (calc_operator.equals(ArithmeticOperator.MODULO.name())) {
            //if second value is greater than first value. modulo makes no sense and value is returned
            //todo makes that sense?
            if (value2 > value1){
                result = value1;
                satisfier = true;
                LOG.warn("AttributeCalculator second value is higher than first and modulo operator would return a weird result. In " +
                        "This case first value is returned");
            } else {
                result = value1 % value2;
                satisfier = true;
            }

        }

        if (satisfier) {
            in.addField(runtimeName, result);
            out.collect(in);
        }

    }

    @Override
    public void onDetach() {

    }
}
