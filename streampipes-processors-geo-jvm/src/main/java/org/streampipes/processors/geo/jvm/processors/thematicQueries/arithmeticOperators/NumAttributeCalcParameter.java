package org.streampipes.processors.geo.jvm.processors.thematicQueries.arithmeticOperators;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;


public class NumAttributeCalcParameter extends EventProcessorBindingParams {

    private String param1;
    private String param2;
    public String field_name;
    private String calcType;


    public NumAttributeCalcParameter(DataProcessorInvocation graph, String param1, String param2, String field_name, String calcType) {
        super(graph);
        this.param1 = param1;
        this.param2 = param2;
        this.field_name = field_name;
        this.calcType =  calcType;
    }

    public String getParam1() {
        return param1;
    }

    public String getParam2() {
        return param2;
    }

    public String getField_name() {
        return field_name;
    }

    public String getCalcType() {
        return calcType;
    }
}
