
package org.streampipes.processors.geo.jvm.processors.thematicQueries.comparisonOperators;



import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.model.runtime.Event;
import org.streampipes.logging.api.Logger;




public class NumericalFilter implements EventProcessor<NumericalFilterParameters> {

  private NumericalFilterParameters params;
  private Double threshold;
  private static Logger LOG;


  @Override
  public void onInvocation(NumericalFilterParameters params, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {


    this.params = params;
    this.threshold = params.getThreshold();
  }

  @Override
  public void onEvent(Event in, SpOutputCollector out){


    Boolean satisfiesFilter = false;


    Double value = in.getFieldBySelector(params.getFilterProperty()).getAsPrimitive().getAsDouble();


    if (params.getNumericalOperator() == NumericalOperator.EQ) {
      satisfiesFilter = (value == threshold);
    } else if (params.getNumericalOperator() == NumericalOperator.GE) {
      satisfiesFilter = (value >= threshold);
    } else if (params.getNumericalOperator() == NumericalOperator.GT) {
      satisfiesFilter = value > threshold;
    } else if (params.getNumericalOperator() == NumericalOperator.LE) {
      satisfiesFilter = (value <= threshold);
    } else if (params.getNumericalOperator() == NumericalOperator.LT) {
      satisfiesFilter = (value < threshold);
    } else if (params.getNumericalOperator() == NumericalOperator.NE) {
      satisfiesFilter = (value != threshold);
    }

    if (satisfiesFilter) {
      out.collect(in);
    }
  }

  @Override
  public void onDetach() {

  }
}
