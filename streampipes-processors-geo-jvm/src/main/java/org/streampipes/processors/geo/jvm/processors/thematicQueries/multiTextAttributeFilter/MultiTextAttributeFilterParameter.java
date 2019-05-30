/*
 * Copyright 2017 FZI Forschungszentrum Informatik
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.streampipes.processors.geo.jvm.processors.thematicQueries.multiTextAttributeFilter;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class MultiTextAttributeFilterParameter extends EventProcessorBindingParams {

  private String keyword_1;
  private String searchOption_1;
  private String keyword_2;
  private String searchOption_2;
  private String filterAttribute;
  private Boolean caseSensitiv;

  public MultiTextAttributeFilterParameter(DataProcessorInvocation graph, String keyword_1, String searchOption_1, String keyword_2, String searchOption_2, String filterAttribute, boolean caseSensitiv) {
    super(graph);
    this.keyword_1 = keyword_1;
    this.searchOption_1 = searchOption_1;
    this.keyword_2 = keyword_2;
    this.searchOption_2 = searchOption_2;
    this.filterAttribute = filterAttribute;
    this.caseSensitiv = caseSensitiv;
  }

  public String getKeyword_1() {
    return keyword_1;
  }

  public String getSearchOption_1() {
    return searchOption_1;
  }

  public String getFilterAttribute() {
    return filterAttribute;
  }

  public Boolean getCaseSensitiv() {
    return caseSensitiv;
  }


  public String getKeyword_2() {
    return keyword_2;
  }

  public String getSearchOption_2() {
    return searchOption_2;
  }
}
