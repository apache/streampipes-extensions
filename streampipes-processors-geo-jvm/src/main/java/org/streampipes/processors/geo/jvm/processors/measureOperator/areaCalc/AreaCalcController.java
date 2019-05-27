package org.streampipes.processors.geo.jvm.processors.measureOperator.areaCalc;

import org.streampipes.processors.geo.jvm.helpers.AreaSpOperator;
import org.streampipes.model.DataProcessorType;
import org.streampipes.model.graph.DataProcessorDescription;
import org.streampipes.model.graph.DataProcessorInvocation;
import org.streampipes.model.schema.PropertyScope;
import org.streampipes.sdk.builder.ProcessingElementBuilder;
import org.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.streampipes.sdk.helpers.*;
import org.streampipes.vocabulary.SO;
import org.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;
import org.streampipes.sdk.utils.Assets;



public class AreaCalcController extends  StandaloneEventProcessingDeclarer<AreaCalcParameter> {


    protected final static String WKT_TEXT = "wkt_text";
    protected final static String EPSG_CODE = "epsg_code";


    protected final static String AREA = "area";
    protected final static String DECIMAL_CHOICE = "decimalNumber";
    protected final static String UNIT = "units";

    public final static String EPA_NAME = "Area Calculation";


    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.measureOperator.areaCalc",
                        EPA_NAME,
                        "Calculates Area of Polygons. If Input is a MultiPolygon, the sum " +
                        "of all polygon areas will be calculated"  )
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(EpRequirements.stringReq(),
                                Labels.from(
                                        WKT_TEXT,
                                        "WKT_String",
                                        "Field with WKT String"),
                                PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(EpRequirements.numberReq(),
                                Labels.from(
                                        EPSG_CODE,
                                        "EPSG Field",
                                        "EPSG Code for SRID"),
                                PropertyScope.NONE)
                        .build()
                )
                .requiredIntegerParameter(
                        Labels.from(
                                DECIMAL_CHOICE,
                                "decimal positions",
                                "Choose the amount of decimal Position."),
                        0, 10,1
                )



                .requiredSingleValueSelection(
                        Labels.from(
                                UNIT,
                                "Goal Unit",
                                "Select the unit of your choice"),
                        Options.from(
                                AreaSpOperator.ValidAreaUnits.squareMeter.name(),
                                AreaSpOperator.ValidAreaUnits.squareKM.name(),
                                AreaSpOperator.ValidAreaUnits.hectar.name(),
                                AreaSpOperator.ValidAreaUnits.ar.name()
                ))


                .outputStrategy(OutputStrategies.append(
                        EpProperties.numberEp(
                                Labels.from(AREA,
                                        "Area Calculation value",
                                        "Result of the area calculation"),
                                AREA, SO.Number),

                        EpProperties.stringEp(
                                Labels.from(UNIT,
                                        "Area Calculation unit",
                                        "used units of the area calculation"),
                                UNIT, SO.Text))
                )

                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<AreaCalcParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {


        String wkt_String = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);

        Integer decimalPos = extractor.singleValueParameter(DECIMAL_CHOICE, Integer.class);
        if (decimalPos <0){
            decimalPos = 0;
        }


        String chosenUnit = extractor.selectedSingleValue(UNIT, String.class);


        // convert enum to integer values, Meter is default
        int unit = 1;
        if (chosenUnit.equals(AreaSpOperator.ValidAreaUnits.squareKM.name())){
            unit = AreaSpOperator.ValidAreaUnits.squareKM.getNumber();
        } else if (chosenUnit.equals(AreaSpOperator.ValidAreaUnits.hectar.name())){
            unit = AreaSpOperator.ValidAreaUnits.hectar.getNumber();
        } else if (chosenUnit.equals(AreaSpOperator.ValidAreaUnits.ar.name())){
            unit = AreaSpOperator.ValidAreaUnits.ar.getNumber();
        }


        AreaCalcParameter params = new AreaCalcParameter(graph, wkt_String, epsg_code, decimalPos, unit);

        return new ConfiguredEventProcessor<>(params, AreaCalc::new);
    }
}

