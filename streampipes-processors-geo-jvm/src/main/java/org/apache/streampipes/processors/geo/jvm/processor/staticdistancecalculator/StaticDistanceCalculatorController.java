/*
 *   Licensed to the Apache Software Foundation (ASF) under one or more
 *   contributor license agreements.  See the NOTICE file distributed with
 *   this work for additional information regarding copyright ownership.
 *   The ASF licenses this file to You under the Apache License, Version 2.0
 *   (the "License"); you may not use this file except in compliance with
 *   the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
 */
package org.apache.streampipes.processors.geo.jvm.processor.staticdistancecalculator;

import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.model.schema.PropertyScope;
import org.apache.streampipes.processors.geo.jvm.processor.util.SpLengthCalculator;
import org.apache.streampipes.sdk.builder.PrimitivePropertyBuilder;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.sdk.utils.Datatypes;
import org.apache.streampipes.vocabulary.Geo;
import org.apache.streampipes.vocabulary.SO;
import org.apache.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.apache.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

import java.net.URI;

public class StaticDistanceCalculatorController extends StandaloneEventProcessingDeclarer<StaticDistanceCalculatorParameters> {

  private static final String LATITUDE_KEY = "latitude-key";
  private static final String LONGITUDE_KEY = "longitude-key";
  private static final String SELECTED_LATITUDE_KEY = "selected-latitude-key";
  private static final String SELECTED_LONGITUDE_KEY = "selected-longitude-key";

  private static final String DECIMAL_POSITION_KEY = "decimalPosition";
  private static final String UNIT_KEY = "unit";

  protected final static String LENGTH_RUNTIME = "geodesicStaticDistance";
  protected final static String UNIT_RUNTIME = "geodesicStaticDistanceUnit";

  @Override
  public DataProcessorDescription declareModel() {
    return ProcessingElementBuilder.create("org.apache.streampipes.processors.geo.jvm.processor" +
        ".staticdistancecalculator")
        .category(DataProcessorType.FILTER)
        .withAssets(Assets.DOCUMENTATION)
        .withLocales(Locales.EN)
        .requiredStream(StreamRequirementsBuilder
            .create()
            .requiredPropertyWithUnaryMapping(EpRequirements.domainPropertyReq(Geo.lat)
                , Labels.withId(LATITUDE_KEY), PropertyScope.MEASUREMENT_PROPERTY)
            .requiredPropertyWithUnaryMapping(EpRequirements.domainPropertyReq(Geo.lng)
                , Labels.withId(LONGITUDE_KEY), PropertyScope.MEASUREMENT_PROPERTY)
            .build())
        .requiredFloatParameter(Labels.withId(SELECTED_LATITUDE_KEY), (float) 49.0068901)
        .requiredFloatParameter(Labels.withId(SELECTED_LONGITUDE_KEY), (float) 8.4036527)
        .requiredIntegerParameter(
            Labels.withId(DECIMAL_POSITION_KEY), 0, 10, 1)
        .requiredSingleValueSelection(
            Labels.withId(UNIT_KEY),
            Options.from(
                SpLengthCalculator.ValidLengthUnits.METER.name(),
                SpLengthCalculator.ValidLengthUnits.KM.name(),
                SpLengthCalculator.ValidLengthUnits.MILE.name(),
                SpLengthCalculator.ValidLengthUnits.FOOT.name()
            )
        )

        .outputStrategy(OutputStrategies.append(
            PrimitivePropertyBuilder
                .create(Datatypes.Double,LENGTH_RUNTIME)
                .domainProperty(SO.Number)
                //todo dynamic
                //.measurementUnit(URI.create("http://qudt.org/vocab/unit#Meter"))
                .build(),
            PrimitivePropertyBuilder
                .create(Datatypes.Double,UNIT_RUNTIME)
                .domainProperty(SO.Text)
                // todo unit type?
                .measurementUnit(URI.create("http://qudt.org/vocab/quantitykind/Length"))
                .build())
        )
        .build();

  }

  @Override
  public ConfiguredEventProcessor<StaticDistanceCalculatorParameters> onInvocation(DataProcessorInvocation graph,
                                                                                   ProcessingElementParameterExtractor extractor) {
    String latitudeFieldName = extractor.mappingPropertyValue(LATITUDE_KEY);
    String longitudeFieldName = extractor.mappingPropertyValue(LONGITUDE_KEY);
    Double selectedLatitude = extractor.singleValueParameter(SELECTED_LATITUDE_KEY, Double.class);
    Double selectedLongitude = extractor.singleValueParameter(SELECTED_LONGITUDE_KEY, Double.class);


    Integer decimalPosition = extractor.singleValueParameter(DECIMAL_POSITION_KEY, Integer.class);

    String chosenUnit = extractor.selectedSingleValue(UNIT_KEY, String.class);

    // convert enum to integer values default meter
    int unit = 1;
    if (chosenUnit.equals(SpLengthCalculator.ValidLengthUnits.KM.name())){
      unit = SpLengthCalculator.ValidLengthUnits.KM.getNumber();
    } else if (chosenUnit.equals(SpLengthCalculator.ValidLengthUnits.MILE.name())){
      unit = SpLengthCalculator.ValidLengthUnits.MILE.getNumber();
    } else if (chosenUnit.equals(SpLengthCalculator.ValidLengthUnits.FOOT.name())){
      unit = SpLengthCalculator.ValidLengthUnits.FOOT.getNumber();
    }

    StaticDistanceCalculatorParameters staticParam = new StaticDistanceCalculatorParameters(graph,
        latitudeFieldName, longitudeFieldName, selectedLatitude, selectedLongitude, decimalPosition, unit);

    return new ConfiguredEventProcessor<>(staticParam, StaticDistanceCalculator::new);
  }
}
