package org.streampipes.processors.geo.jvm.processors.thematicQueries.textAttributeFilter;

import org.streampipes.logging.api.Logger;
import org.streampipes.model.runtime.Event;
import org.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.streampipes.wrapper.routing.SpOutputCollector;
import org.streampipes.wrapper.runtime.EventProcessor;




public class TextAttributeFilter implements EventProcessor<TextAttributeFilterParameter> {

  private TextAttributeFilterParameter params;
  private boolean caseSensitiv;
  private String searchedKeyword;
  private String option;
  public static Logger LOG;

  @Override
  public void onInvocation(TextAttributeFilterParameter textAttributeFilterParameter, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {
    this.params = textAttributeFilterParameter;
    this.caseSensitiv = params.getCaseSensitiv();
    this.searchedKeyword = params.getKeyword();
    this.option = params.getSearchOption();
    LOG = params.getGraph().getLogger(TextAttributeFilter.class);
  }

  @Override
  public void onEvent(Event event, SpOutputCollector out) {
    Boolean satisfiesFilter = false;


    String attributeValue = event.getFieldBySelector(params.getFilterProperty()).getAsPrimitive().getAsString();


    if (!caseSensitiv && !(searchedKeyword == null)) {
      attributeValue = attributeValue.toLowerCase();
      searchedKeyword = searchedKeyword.toLowerCase();
    }

    if (option.equals(SearchOption.IS.name())) {
      satisfiesFilter = (attributeValue.equals(searchedKeyword));
    } else if (option.equals(SearchOption.LIKE.name())) {
      satisfiesFilter = (attributeValue.contains(searchedKeyword));
    } else if (option.equals(SearchOption.IS_NOT.name())) {
      satisfiesFilter = (!attributeValue.equals(searchedKeyword));
    }  else if (option.equals(SearchOption.IS_NOT.name())){
      satisfiesFilter = (!attributeValue.contains(searchedKeyword));
    }


    if (satisfiesFilter) {
      out.collect(event);
    }
  }

  @Override
  public void onDetach() {

  }
}
