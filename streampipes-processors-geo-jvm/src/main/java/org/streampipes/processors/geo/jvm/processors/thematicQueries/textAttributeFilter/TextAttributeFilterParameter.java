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

package org.streampipes.processors.geo.jvm.processors.thematicQueries.textAttributeFilter;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class TextAttributeFilterParameter extends EventProcessorBindingParams {

  private String keyword;
  private String searchOption;
  private String filterProperty;
  private Boolean caseSensitiv;

  public TextAttributeFilterParameter(DataProcessorInvocation graph, String keyword, String searchOption, String filterProperty, boolean caseSensitiv) {
    super(graph);
    this.keyword = keyword;
    this.searchOption = searchOption;
    this.filterProperty = filterProperty;
    this.caseSensitiv = caseSensitiv;
  }

  public String getKeyword() {
    return keyword;
  }

  public String getSearchOption() {
    return searchOption;
  }

  public String getFilterProperty() {
    return filterProperty;
  }

  public Boolean getCaseSensitiv() {
    return caseSensitiv;
  }
}
