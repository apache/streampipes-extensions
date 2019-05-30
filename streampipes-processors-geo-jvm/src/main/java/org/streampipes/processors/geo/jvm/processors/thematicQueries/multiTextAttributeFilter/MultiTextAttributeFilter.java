package org.streampipes.processors.geo.jvm.processors.thematicQueries.multiTextAttributeFilter;

import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.processors.geo.jvm.processors.thematicQueries.textAttributeFilter.SearchOption;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;


public class MultiTextAttributeFilter implements EventProcessor<MultiTextAttributeFilterParameter> {

  private MultiTextAttributeFilterParameter params;
  private boolean caseSensitiv;
  private String firstKeyword;
  private String firstOption;
  private String secondKeyword;
  private String secondOption;
  public static Logger LOG;

  @Override
  public void onInvocation(MultiTextAttributeFilterParameter multiTextAttributeFilterParameter, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {
    this.params = multiTextAttributeFilterParameter;
    this.caseSensitiv = params.getCaseSensitiv();
    this.firstKeyword = params.getKeyword_1();
    this.firstOption = params.getSearchOption_1();
    this.secondKeyword = params.getKeyword_2();
    this.secondOption = params.getSearchOption_2();


    LOG = params.getGraph().getLogger(MultiTextAttributeFilter.class);
  }

  @Override
  public void onEvent(Event event, SpOutputCollector out) {
    Boolean satisfiesFirst = false;
    Boolean satisfiesSecond = false;


    String filterAttribute = event.getFieldBySelector(params.getFilterAttribute()).getAsPrimitive().getAsString();

    if (!caseSensitiv && !(firstKeyword == null)) {
      filterAttribute = filterAttribute.toLowerCase();
      firstKeyword = firstKeyword.toLowerCase();
      secondKeyword = secondKeyword.toLowerCase();
    }



    // checks for first Attribute
    if (firstOption.equals(SearchOption.IS.name()) ) {
      satisfiesFirst = (filterAttribute.equals(firstKeyword));
    } else if (firstOption.equals(SearchOption.LIKE.name())) {
      satisfiesFirst = (filterAttribute.contains(firstKeyword));
    } else if (firstOption.equals(SearchOption.IS_NOT.name())) {
      satisfiesFirst = (!filterAttribute.equals(firstKeyword));
    }  else if (firstOption.equals(SearchOption.IS_NOT.name())){
      satisfiesFirst = (!filterAttribute.contains(firstKeyword));
    }


    // checks for first Attribute
    if (secondOption.equals(SearchOption.IS.name()) ) {
      satisfiesSecond = (filterAttribute.equals(secondKeyword));
    } else if (secondOption.equals(SearchOption.LIKE.name())) {
      satisfiesSecond = (filterAttribute.contains(secondKeyword));
    } else if (secondOption.equals(SearchOption.IS_NOT.name())) {
      satisfiesSecond = (!filterAttribute.equals(secondKeyword));
    }  else if (secondOption.equals(SearchOption.IS_NOT.name())){
      satisfiesSecond = (!filterAttribute.contains(secondKeyword));
    }



    if (satisfiesFirst && satisfiesSecond) {
      out.collect(event);
    }
  }

  @Override
  public void onDetach() {

  }
}
