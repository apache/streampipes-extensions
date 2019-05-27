package org.streampipes.processors.geo.jvm.processors.measureOperator.polygonPerimeterCalc;

import org.streampipes.processors.geo.jvm.helpers.LengthSpOperator;
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

public class PolygonPerimeterController extends StandaloneEventProcessingDeclarer<PolygonPerimeterParameter> {


    public final static String WKT_TEXT = "wkt_text";
    public final static String EPSG_CODE = "epsg_code";
    public final static String PERIMETER = "perimeter";
    public final static String UNIT = "unitOfPerimeter";
    public final static String DECIMAL_CHOICE = "decimalNumber";

    public final static String EPA_NAME = "Polygon Perimeter Calculation";



    @Override
    public DataProcessorDescription declareModel() {
        return ProcessingElementBuilder
                .create("org.streampipes.processors.geo.jvm.processors.measureOperator.polygonPerimeterCalc",
                        EPA_NAME,
                        "Calculates the Perimeter of a Polygon. If Input is a MultiPolygon, the sum " +
                                "of all polygon perimeters will be calculated")
                .category(DataProcessorType.GEO)
                .withAssets(Assets.DOCUMENTATION, Assets.ICON)
                .requiredStream(StreamRequirementsBuilder
                        .create()
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.stringReq(),
                                Labels.from(
                                        WKT_TEXT,
                                        "WKT_String",
                                        "Field with WKT String"),
                                PropertyScope.NONE)
                        .requiredPropertyWithUnaryMapping(
                                EpRequirements.numberReq(),
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
                                "Choose the amount of decimal position"),
                        0, 10, 1)


                .requiredSingleValueSelection(
                        Labels.from(
                                UNIT,
                                "Goal Unit",
                                "Select the unit of your choice"),
                        Options.from(
                                LengthSpOperator.ValidLengthUnits.meter.name(),
                                LengthSpOperator.ValidLengthUnits.km.name(),
                                LengthSpOperator.ValidLengthUnits.mile.name(),
                                LengthSpOperator.ValidLengthUnits.foot.name()
                        )
                )


                .outputStrategy(OutputStrategies.append(
                        EpProperties.numberEp(
                                Labels.from(
                                        PERIMETER,
                                        "perimeter",
                                        "perimeter value"),
                                PERIMETER, SO.Number),
                        EpProperties.stringEp(
                                Labels.from(
                                        UNIT,
                                        "unitPerimeter",
                                        "unit of the perimeter"),
                                UNIT, SO.Text)
                        )
                )


                .supportedFormats(SupportedFormats.jsonFormat())
                .supportedProtocols(SupportedProtocols.kafka())
                .build();
    }


    @Override
    public ConfiguredEventProcessor<PolygonPerimeterParameter> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {



        Integer decimalPos = extractor.singleValueParameter(DECIMAL_CHOICE, Integer.class);

        String chosenUnit = extractor.selectedSingleValue(UNIT, String.class);

        // convert enum to integer values
        int unit = 1;
        if (chosenUnit.equals(LengthSpOperator.ValidLengthUnits.km.name())){
            unit = LengthSpOperator.ValidLengthUnits.km.getNumber();
        } else if (chosenUnit.equals(LengthSpOperator.ValidLengthUnits.mile.name())){
            unit = LengthSpOperator.ValidLengthUnits.mile.getNumber();
        } else if (chosenUnit.equals(LengthSpOperator.ValidLengthUnits.foot.name())){
            unit = LengthSpOperator.ValidLengthUnits.foot.getNumber();
        }


        String wkt_string = extractor.mappingPropertyValue(WKT_TEXT);
        String epsg_code = extractor.mappingPropertyValue(EPSG_CODE);


        PolygonPerimeterParameter params = new PolygonPerimeterParameter(graph, wkt_string, epsg_code, unit, decimalPos);

        return new ConfiguredEventProcessor<>(params, PolygonPerimeter::new);
    }
}
