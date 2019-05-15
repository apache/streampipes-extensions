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
package org.streampipes.processors.geo.jvm.processors.thematicQueries.comparisonOperators;

import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class NumericalFilterParameters extends EventProcessorBindingParams {

  private double threshold;
  private NumericalOperator numericalOperator;
  private String filterProperty;

  public NumericalFilterParameters(DataProcessorInvocation graph, Double threshold, NumericalOperator
          numericalOperator, String filterProperty) {
    super(graph);
    this.threshold = threshold;
    this.numericalOperator = numericalOperator;
    this.filterProperty = filterProperty;
  }

  public double getThreshold() {
    return threshold;
  }

  public NumericalOperator getNumericalOperator() {
    return numericalOperator;
  }

  public String getFilterProperty() {
    return filterProperty;
  }
}
